import os
import time
import sqlite3
import requests
import concurrent.futures
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)
RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
DEXSCREENER_BASE = "https://api.dexscreener.com"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
DB_FILE = "token_history.db"

# ✅ API Caching System (1 Minute)
API_CACHE = {}
CACHE_TTL = 60 

INITIAL_MCAPS = {}
INITIAL_LIQUIDITY = {}
INITIAL_TOP10 = {}
INITIAL_LARGEST = {}
INITIAL_TOTAL_HOLDERS = {}
INITIAL_TIME = {}

# ✅ New Dictionaries for Pool Composition
INITIAL_POOLED_BASE = {}
INITIAL_POOLED_QUOTE = {}

# ─────────────────────────── Helper Functions ───────────────────────────
def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def detect_narrative(name, symbol):
    text = f"{name} {symbol}".lower()
    tags = []
    if any(k in text for k in ["ai", "gpt", "neural", "bot", "agent", "artificial"]): tags.append("AI")
    if any(k in text for k in ["dog", "doge", "shib", "cat", "pepe", "frog", "bear", "bull", "monkey"]): tags.append("Animal")
    if any(k in text for k in ["trump", "maga", "biden", "elon", "musk", "political", "president"]): tags.append("Political")
    if any(k in text for k in ["moon", "rocket", "mars", "space", "star"]): tags.append("Space")
    if any(k in text for k in ["game", "play", "p2e", "rpg"]): tags.append("GameFi")
    if any(k in text for k in ["sol", "solana"]): tags.append("Solana")
    return list(set(tags))

# ─────────────────────────── Database Setup ───────────────────────────
def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  mint TEXT, name TEXT, symbol TEXT,
                  score TEXT, verdict TEXT,
                  mcap TEXT, age TEXT, dex_url TEXT,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

init_db()

# ─────────────────────────── API Fetchers (With 60s Cache) ───────────────────────────
def fetch_goplus(mint):
    cache_key = f"goplus_{mint}"
    if cache_key in API_CACHE:
        data, timestamp = API_CACHE[cache_key]
        if time.time() - timestamp < CACHE_TTL:
            return data

    try:
        r = requests.get(f"{GOPLUS_BASE}/solana/token_security?contract_addresses={mint}", timeout=2)
        if r.ok:
            data = r.json().get("result", {}).get(mint, {})
            API_CACHE[cache_key] = (data, time.time())
            return data
        return {}
    except Exception: return {}

def fetch_rugcheck(mint):
    cache_key = f"rugcheck_{mint}"
    if cache_key in API_CACHE:
        data, timestamp = API_CACHE[cache_key]
        if time.time() - timestamp < CACHE_TTL:
            return data

    try:
        r = requests.get(f"{RUGCHECK_BASE}/tokens/{mint}/report", timeout=2.5)
        if r.ok:
            data = r.json()
            API_CACHE[cache_key] = (data, time.time())
            return data
        return {}
    except Exception: return {}

def fetch_dexscreener(mint):
    try:
        r = requests.get(f"{DEXSCREENER_BASE}/latest/dex/tokens/{mint}", timeout=2)
        if not r.ok: return {}
        pairs = r.json().get("pairs") or []
        if not pairs: return {}
        pairs.sort(key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0, reverse=True)
        return pairs[0]
    except Exception: return {}

# ─────────────────────────── নতুন টোকেন ফেচার ───────────────────────────
def fetch_new_tokens(limit=5, mode="new"): 
    candidates, seen = [], set()
    try:
        r = requests.get(f"{DEXSCREENER_BASE}/token-profiles/latest/v1", timeout=3)
        if r.ok:
            for item in r.json():
                if item.get("chainId") == "solana" and item.get("tokenAddress") not in seen:
                    candidates.append(item["tokenAddress"]); seen.add(item["tokenAddress"])
    except: pass

    if mode == "trending" or len(candidates) < limit:
        try:
            endpoint = "token-boosts/top/v1" if mode == "trending" else "token-boosts/latest/v1"
            r = requests.get(f"{DEXSCREENER_BASE}/{endpoint}", timeout=3)
            if r.ok:
                for item in r.json():
                    if item.get("chainId") == "solana" and item.get("tokenAddress") not in seen:
                        candidates.append(item["tokenAddress"]); seen.add(item["tokenAddress"])
        except: pass

    valid_tokens = []
    ALLOWED_KEYWORDS = ["raydium", "pumpswap"] 

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_dexscreener, candidates[:30]))

    for dex in results:
        if not dex: continue
        dex_id_lower = dex.get("dexId", "").lower()
        if not any(keyword in dex_id_lower for keyword in ALLOWED_KEYWORDS): continue
        
        created = dex.get("pairCreatedAt")
        if not created: continue
        age_mins = (time.time() * 1000 - created) / 60000
        
        if mode == "new":
            if age_mins <= 300:
                valid_tokens.append(dex)
        elif mode == "trending":
            h1_pc = (dex.get("priceChange") or {}).get("h1", 0) or 0
            h1_vol = (dex.get("volume") or {}).get("h1", 0) or 0
            if age_mins <= 1440 and h1_pc > 5 and h1_vol > 10000:
                valid_tokens.append(dex)

    if mode == "new":
        valid_tokens.sort(key=lambda x: x.get("pairCreatedAt", 0), reverse=True)
    else:
        valid_tokens.sort(key=lambda x: (x.get("volume") or {}).get("h1", 0) or 0, reverse=True)

    return valid_tokens[:limit]

# ─────────────────────────── Analysis (24 Checks) ───────────────────────────
def analyze(mint, dex_data=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_rug = executor.submit(fetch_rugcheck, mint)
        future_goplus = executor.submit(fetch_goplus, mint)
        rug = future_rug.result()
        goplus = future_goplus.result()

    dex = dex_data if dex_data else fetch_dexscreener(mint)
    checks = []

    def safe(idx, name, fn):
        try:
            passed, detail = fn()
            checks.append({"idx": idx, "name": name, "status": "pass" if passed else "fail", "detail": detail})
        except Exception:
            checks.append({"idx": idx, "name": name, "status": "fail", "detail": "Error"})

    safe(1, "Mint Authority", lambda: (not rug.get("mintAuthority"), "REVOKED" if not rug.get("mintAuthority") else "ACTIVE"))
    safe(2, "Freeze Authority", lambda: (not rug.get("freezeAuthority"), "REVOKED" if not rug.get("freezeAuthority") else "ACTIVE"))
    safe(3, "GoPlus Mintable", lambda: (goplus.get("mintable", {}).get("status") == "0", "FALSE" if goplus.get("mintable", {}).get("status") == "0" else "TRUE"))
    safe(4, "GoPlus Freezable", lambda: (goplus.get("freezable", {}).get("status") == "0", "FALSE" if goplus.get("freezable", {}).get("status") == "0" else "TRUE"))

    def c5():
        liq = (dex.get("liquidity") or {}).get("usd", 0) or 0
        return liq >= 20000, f"${liq:,.0f}"
    safe(5, "Liquidity", c5)

    def c6():
        markets = rug.get("markets") or []
        pct = (markets[0].get("lp") or {}).get("lpLockedPct", 0) or 0 if markets else 0
        return pct >= 80, f"{pct:.0f}% LOCKED"
    safe(6, "LP Locked", c6)
    safe(7, "GoPlus Honeypot", lambda: (goplus.get("transfer_fee", {}).get("fee_rate", 0) == 0, "CLEAR" if goplus.get("transfer_fee", {}).get("fee_rate", 0) == 0 else "DANGER"))

    def c8():
        risks = rug.get("risks") or []
        bad = any("honeypot" in (r.get("name") or "").lower() for r in risks)
        return not bad, "SAFE" if not bad else "DETECTED"
    safe(8, "RugCheck Honeypot", c8)

    holders = rug.get("topHolders") or []
    top10_val = sum(safe_float(h.get("pct")) for h in holders[:10])
    largest_val = safe_float(holders[0].get("pct")) if holders else 0.0

    total_holders_val = int(rug.get("totalHolders") or 0)
    if total_holders_val == 0 and holders:
        total_holders_val = len(holders)

    def c9():
        return top10_val <= 70, f"{top10_val:.1f}%"
    safe(9, "Top 10 Holders", c9)

    def c10():
        return largest_val < 15, f"{largest_val:.2f}%"
    safe(10, "Largest Holder", c10)

    def c11():
        bal, supply = rug.get("creatorBalance", 0) or 0, (rug.get("token") or {}).get("supply", 0) or 0
        pct = (bal / supply * 100) if supply else 0
        return pct < 5, f"{pct:.2f}%"
    safe(11, "Dev Wallet Holding", c11)

    def c12():
        events = rug.get("events") or []
        sells = sum(1 for e in events if "sell" in (e.get("description") or "").lower())
        return sells == 0, f"{sells} SELLS"
    safe(12, "Dev Sell History", c12)

    def c13():
        ins = rug.get("graphInsidersDetected", 0) or 0
        return ins == 0, f"{ins} FOUND"
    safe(13, "Bundled Wallets", c13)

    def c14():
        txns = dex.get("txns") or {}
        t = txns.get("h24") or txns.get("h6") or txns.get("h1") or txns.get("m5") or {}
        b, s = t.get("buys", 0) or 0, t.get("sells", 0) or 0
        if s == 0: return False, "BUY ONLY"
        return (b / s) <= 10, f"B:{b} S:{s}"
    safe(14, "Buy/Sell Balance", c14)

    def c15():
        prog = rug.get("tokenProgram") or ""
        return "2022" not in prog, "STANDARD" if "2022" not in prog else "2022"
    safe(15, "Token-2022", c15)

    safe(16, "GoPlus Closable", lambda: (goplus.get("closable", {}).get("status") == "0", "FALSE" if goplus.get("closable", {}).get("status") == "0" else "TRUE"))
    safe(17, "GoPlus Balance Mutable", lambda: (goplus.get("balance_mutable_authority", {}).get("status") == "0", "FALSE" if goplus.get("balance_mutable_authority", {}).get("status") == "0" else "TRUE"))

    def c18():
        created = dex.get("pairCreatedAt")
        if not created: return False, "UNKNOWN"
        days = (time.time() * 1000 - created) / (1000 * 60 * 60 * 24)
        return days > 3, f"{days:.1f} DAYS"
    safe(18, "Token Age", c18)

    def c19():
        info = dex.get("info") or {}
        return bool(info.get("websites") or info.get("socials")), "YES" if bool(info.get("websites") or info.get("socials")) else "NO"
    safe(19, "Social / Website", c19)

    def c20():
        return not (rug.get("tokenMeta") or {}).get("mutable", True), "IMMUTABLE" if not (rug.get("tokenMeta") or {}).get("mutable", True) else "MUTABLE"
    safe(20, "Metadata", c20)

    def c21():
        pc = dex.get("priceChange") or {}
        return abs(pc.get("h24", 0) or 0) <= 500, f"{pc.get('h24',0):.0f}%"
    safe(21, "Chart Pattern", c21)

    def c22():
        return (rug.get("score_normalised", 100) or 100) <= 30, f"{(rug.get('score_normalised', 100) or 100):.0f}"
    safe(22, "Overall Risk", c22)

    def c23():
        ins = rug.get("graphInsidersDetected", 0) or 0
        return ins <= 3, f"{ins} FOUND"
    safe(23, "Fresh Wallet Cluster", c23)

    def c24():
        risks = rug.get("risks") or []
        return not any("bundle" in (r.get("name") or "").lower() for r in risks), "NO BUNDLE"
    safe(24, "Bundle / Sniper", c24)

    passed = [c["name"] for c in checks if c["status"] == "pass"]
    failed = [c["name"] for c in checks if c["status"] != "pass"]

    soft_checks = [18]
    valid_checks = [c for c in checks if c["idx"] not in soft_checks]
    valid_passed = [c for c in valid_checks if c["status"] == "pass"]
    valid_failed = [c for c in valid_checks if c["status"] == "fail"]

    tier1_fails = [c["name"] for c in valid_failed if c["idx"] in [1,2,3,4,5,6,7,8]]

    if tier1_fails:
        verdict, verdict_color = "AVOID", "red"
    elif len(valid_failed) == 0:
        verdict, verdict_color = "GOOD", "green"
    elif len(valid_passed) >= 20:
        verdict, verdict_color = "GOOD", "green"
    elif len(valid_passed) >= 15:
        verdict, verdict_color = "CAUTION", "yellow"
    else:
        verdict, verdict_color = "RISKY", "red"

    token_name = (dex.get("baseToken") or {}).get("name", "Unknown Token")
    token_symbol = (dex.get("baseToken") or {}).get("symbol", "???")
    dex_url = dex.get("url") or f"https://dexscreener.com/solana/{mint}"
    dex_id = dex.get("dexId", "Unknown")

    info = dex.get("info") or {}
    image_url = info.get("imageUrl", "")
    narratives = detect_narrative(token_name, token_symbol)

    websites = info.get("websites") or []
    socials = info.get("socials") or []
    tw = next((s["url"] for s in socials if s.get("type", "").lower() in ["twitter", "x"]), "")
    tg = next((s["url"] for s in socials if s.get("type", "").lower() == "telegram"), "")
    web = websites[0]["url"] if websites else ""

    creator_tokens = rug.get("creatorTokens") or []
    prev_tokens = len(creator_tokens) if isinstance(creator_tokens, list) else 0
    rugged_tokens = sum(1 for t in creator_tokens if isinstance(t, dict) and t.get("rugged")) if isinstance(creator_tokens, list) else 0

    mcap_val = dex.get("marketCap") or dex.get("fdv") or 0
    liq_val = (dex.get("liquidity") or {}).get("usd", 0) or 0

    if (mint not in INITIAL_MCAPS or INITIAL_MCAPS[mint] == 0) and mcap_val > 0: INITIAL_MCAPS[mint] = mcap_val
    if (mint not in INITIAL_LIQUIDITY or INITIAL_LIQUIDITY[mint] == 0) and liq_val > 0: INITIAL_LIQUIDITY[mint] = liq_val
    if (mint not in INITIAL_TOP10 or INITIAL_TOP10[mint] == 0) and top10_val > 0: INITIAL_TOP10[mint] = top10_val
    if (mint not in INITIAL_LARGEST or INITIAL_LARGEST[mint] == 0) and largest_val > 0: INITIAL_LARGEST[mint] = largest_val
    if (mint not in INITIAL_TOTAL_HOLDERS or INITIAL_TOTAL_HOLDERS[mint] == 0) and total_holders_val > 0: INITIAL_TOTAL_HOLDERS[mint] = total_holders_val

    initial_mcap_val = INITIAL_MCAPS.get(mint, mcap_val)
    initial_liq_val = INITIAL_LIQUIDITY.get(mint, liq_val)
    init_top10 = INITIAL_TOP10.get(mint, top10_val)
    init_largest = INITIAL_LARGEST.get(mint, largest_val)
    init_total = INITIAL_TOTAL_HOLDERS.get(mint, total_holders_val)

    mcap_gained = mcap_val >= initial_mcap_val

    def fmt_mcap(val):
        if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
        elif val >= 1_000: return f"${val/1_000:.1f}K"
        else: return f"${val:,.0f}"

    mcap_str = f"{fmt_mcap(initial_mcap_val)} ➔ {fmt_mcap(mcap_val)}"

    def fmt_liq(val):
        if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
        elif val >= 1_000: return f"${val/1_000:.1f}K"
        else: return f"${val:,.0f}"

    liq_str = f"{fmt_liq(initial_liq_val)} ➔ {fmt_liq(liq_val)}"

    top10_display = f"{init_top10:.1f}% ➔ {top10_val:.1f}%"
    largest_display = f"{init_largest:.2f}% ➔ {largest_val:.2f}%"
    total_holders_display = f"{init_total:,} ➔ {total_holders_val:,}"

    current_time = time.time()
    if mint not in INITIAL_TIME: INITIAL_TIME[mint] = current_time
    elapsed_mins = max(1.0, (current_time - INITIAL_TIME[mint]) / 60.0)
    holder_diff = total_holders_val - init_total
    velocity = holder_diff / elapsed_mins
    velocity_str = f"{velocity:+.1f} / min"

    txns = dex.get("txns") or {}
    t = txns.get("h24") or txns.get("h6") or txns.get("h1") or txns.get("m5") or {}
    buys, sells = t.get("buys", 0) or 0, t.get("sells", 0) or 0
    total_txns = buys + sells
    vol = (dex.get("volume") or {}).get("h24", 0) or 0

    vol_mcap_ratio = (vol / mcap_val) if mcap_val > 0 else 0.0
    vol_mcap_str = f"{vol_mcap_ratio:.2f}x"

    if total_txns > 0:
        buy_pct, sell_pct = int((buys / total_txns) * 100), 100 - int((buys / total_txns) * 100)
        buy_vol, sell_vol = vol * (buy_pct / 100), vol * (sell_pct / 100)
    else:
        buy_pct, sell_pct, buy_vol, sell_vol = 0, 0, 0, 0

    def fmt_vol(v): 
        if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
        elif v >= 1_000: return f"${v/1_000:.1f}K"
        else: return f"${v:,.0f}"

    price_change = dex.get("priceChange") or {}
    m5_pump = price_change.get("m5", 0) or 0
    pump_str = f"{m5_pump:+.2f}%" if m5_pump != 0 else "0.00%"

    h24_pump = price_change.get("h24", 0) or 0
    h24_pump_str = f"{h24_pump:+.2f}%" if h24_pump != 0 else "0.00%"

    hype_score = 0
    if velocity >= 20: hype_score += 25
    elif velocity >= 10: hype_score += 15
    elif velocity >= 5: hype_score += 5
    if vol_mcap_ratio >= 2.0: hype_score += 25
    elif vol_mcap_ratio >= 1.0: hype_score += 15
    if m5_pump >= 10: hype_score += 25
    elif m5_pump >= 5: hype_score += 15

    hype_label, hype_color = "", ""
    if hype_score >= 55: hype_label, hype_color = "🔥 HIGH HYPE", "var(--accent-red)"
    elif hype_score >= 30: hype_label, hype_color = "⚡ MEDIUM HYPE", "var(--accent-yellow)"
    else: hype_label, hype_color = "❄️ LOW HYPE", "var(--text-muted)"

    age_str, age_seconds = "Unknown", 99999
    if dex.get("pairCreatedAt"):
        age_seconds = int((time.time() * 1000 - dex["pairCreatedAt"]) / 1000)
        diff_mins, diff_secs = age_seconds // 60, age_seconds % 60
        age_str = f"{diff_mins}m {diff_secs}s" if diff_mins < 60 else (f"{diff_mins//60}h {diff_mins%60}m" if diff_mins < 1440 else f"{diff_mins//1440}d {(diff_mins%1440)//60}h")

    # ✅ NEW: Pool Composition Logic (100% DexScreener Match)
    base_symbol = (dex.get("baseToken") or {}).get("symbol", "???")
    quote_symbol = (dex.get("quoteToken") or {}).get("symbol", "???")
    
    liq_data = dex.get("liquidity") or {}
    pooled_base = safe_float(liq_data.get("base", 0))
    pooled_quote = safe_float(liq_data.get("quote", 0))
    total_liq_usd = safe_float(liq_data.get("usd", 0))
    price_usd = safe_float(dex.get("priceUsd", 0))
    
    base_usd_val = pooled_base * price_usd
    quote_usd_val = max(0, total_liq_usd - base_usd_val)
    
    if (mint not in INITIAL_POOLED_BASE or INITIAL_POOLED_BASE[mint] == 0) and pooled_base > 0: 
        INITIAL_POOLED_BASE[mint] = pooled_base
    if (mint not in INITIAL_POOLED_QUOTE or INITIAL_POOLED_QUOTE[mint] == 0) and pooled_quote > 0: 
        INITIAL_POOLED_QUOTE[mint] = pooled_quote
        
    init_base = INITIAL_POOLED_BASE.get(mint, pooled_base)
    init_quote = INITIAL_POOLED_QUOTE.get(mint, pooled_quote)
    
    def fmt_amount(val):
        if val >= 1_000_000_000: return f"{val/1_000_000_000:.1f}B"
        elif val >= 1_000_000: return f"{val/1_000_000:.1f}M"
        elif val >= 1_000: return f"{val/1_000:.1f}K"
        else: return f"{val:,.2f}".rstrip('0').rstrip('.')
        
    def fmt_usd(val):
        if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
        elif val >= 1_000: return f"${val/1_000:.1f}K"
        else: return f"${val:,.0f}"

    return {
        "mint": mint, "name": token_name, "symbol": token_symbol,
        "dex_id": dex_id, "age": age_str, "age_seconds": age_seconds,
        "mcap": mcap_str, "liq": liq_str, "score": f"{len(passed)}/{len(checks)}",
        "verdict": verdict, "color": verdict_color, "results": checks,
        "passed": passed, "failed": failed, "dex_url": dex_url,
        "pump_str": pump_str, "m5_pump": m5_pump,
        "h24_pump_str": h24_pump_str, "h24_pump": h24_pump,
        "buys": buys, "sells": sells,
        "buy_pct": buy_pct, "sell_pct": sell_pct, "buy_vol": fmt_vol(buy_vol),
        "sell_vol": fmt_vol(sell_vol), "total_txns": total_txns,
        "mcap_gained": mcap_gained,
        "top10_display": top10_display,
        "largest_display": largest_display,
        "top10_now": round(top10_val, 1),
        "largest_now": round(largest_val, 2),
        "total_holders_display": total_holders_display,
        "total_holders_now": total_holders_val,
        "tw": tw, "tg": tg, "web": web, "image_url": image_url,
        "dev_address": rug.get("creator", ""), "prev_tokens": prev_tokens, "rugged_tokens": rugged_tokens,
        "narratives": narratives,
        "velocity": velocity, "velocity_str": velocity_str,
        "vol_mcap_ratio": vol_mcap_ratio, "vol_mcap_str": vol_mcap_str,
        "hype_label": hype_label, "hype_color": hype_color,
        
        # ✅ New Pool Variables
        "base_symbol": base_symbol,
        "quote_symbol": quote_symbol,
        "base_display": f"{fmt_amount(init_base)} ➔ {fmt_amount(pooled_base)}",
        "quote_display": f"{fmt_amount(init_quote)} ➔ {fmt_amount(pooled_quote)}",
        "base_usd_val": fmt_usd(base_usd_val),
        "quote_usd_val": fmt_usd(quote_usd_val)
    }

def save_to_history(token_data):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        c = conn.cursor()
        c.execute("INSERT INTO history (mint, name, symbol, score, verdict, mcap, age, dex_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (token_data["mint"], token_data["name"], token_data["symbol"], token_data["score"], token_data["verdict"], token_data["mcap"], token_data["age"], token_data["dex_url"]))
        conn.commit()
        conn.close()
    except: pass

# ─────────────────────────── HTML UI (V6 + Cyberpunk SHIELD) ───────────────────────────
PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Alpha Vanguard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg-dark: #0a0e17; --bg-card: #1a2235; --bg-item: #111827; --text-main: #e2e8f0;
    --text-muted: #94a3b8; --accent-blue: #3b82f6; --accent-green: #10b981;
    --accent-yellow: #f59e0b; --accent-red: #ef4444; --border-color: #2d3748;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
  
  body { 
    background-color: var(--bg-dark); color: var(--text-main); padding: 10px; padding-bottom: 50px;
    background-image: linear-gradient(rgba(45, 55, 72, 0.3) 1px, transparent 1px), linear-gradient(90deg, rgba(45, 55, 72, 0.3) 1px, transparent 1px);
    background-size: 20px 20px;
  }

  .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; padding: 12px; background: var(--bg-card); border-radius: 10px; border: 1px solid var(--border-color); }
  .logo { font-weight: 700; font-size: 1rem; color: #fff; }
  .status-container { display: flex; gap: 8px; }
  .btn-top { background: var(--bg-item); border: 1px solid var(--border-color); color: var(--text-main); padding: 6px 10px; border-radius: 20px; cursor: pointer; font-size: 0.75rem; font-weight: 600; }
  .btn-top.active { background: var(--accent-blue); border-color: var(--accent-blue); color: #fff; }

  .search-container { display: flex; gap: 8px; margin-bottom: 12px; }
  .search-input { flex: 1; padding: 12px 16px; border-radius: 20px; border: 1px solid var(--border-color); background: var(--bg-card); color: #fff; font-size: 0.9rem; }
  .btn-scan { padding: 12px 16px; background: var(--accent-blue); color: #fff; border: none; border-radius: 20px; font-weight: 600; cursor: pointer; }

  .pill-filters { display: flex; gap: 8px; margin-bottom: 15px; overflow-x: auto; padding-bottom: 5px; }
  .pill { padding: 6px 14px; border-radius: 20px; background: var(--bg-card); border: 1px solid var(--border-color); color: var(--text-muted); font-size: 0.8rem; font-weight: 600; cursor: pointer; white-space: nowrap; }
  .pill.active { background: #fff; color: #000; border-color: #fff; }

  .token-list { display: flex; flex-direction: column; gap: 15px; }
  .token-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 16px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.4); }
  
  .card-header { padding: 15px; border-bottom: 1px solid var(--border-color); display: flex; justify-content: space-between; align-items: flex-start; background: #151c2c; }
  .token-title { font-size: 1.1rem; font-weight: 700; color: #fff; display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
  .token-dex { font-size: 0.75rem; color: var(--text-muted); background: var(--bg-dark); padding: 3px 8px; border-radius: 6px; font-weight: normal; }
  .token-age { font-size: 0.8rem; color: var(--text-muted); margin-top: 6px; }
  
  .token-logo { width: 36px; height: 36px; border-radius: 50%; margin-right: 8px; border: 2px solid var(--border-color); object-fit: cover; background: var(--bg-item); }
  .token-logo-fallback { width: 36px; height: 36px; border-radius: 50%; margin-right: 8px; border: 2px solid var(--border-color); background: var(--accent-blue); display: flex; align-items: center; justify-content: center; font-size: 16px; font-weight: bold; color: #fff; }

  .narrative-tags { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 8px; }
  .narrative-tag { font-size: 0.7rem; font-weight: 700; padding: 3px 8px; border-radius: 4px; background: rgba(59, 130, 246, 0.2); color: var(--accent-blue); border: 1px solid rgba(59, 130, 246, 0.4); }

  .hype-badge { font-size: 0.75rem; font-weight: 700; padding: 4px 8px; border-radius: 6px; margin-top: 8px; display: inline-block; background: rgba(255,255,255,0.05); border: 1px solid var(--border-color); }

  .verdict-badge { font-size: 0.8rem; font-weight: 700; padding: 8px 12px; border-radius: 8px; text-transform: uppercase; white-space: nowrap; }
  .badge-green { background: rgba(16,185,129,0.2); color: var(--accent-green); border: 1px solid var(--accent-green); }
  .badge-yellow { background: rgba(245,158,11,0.2); color: var(--accent-yellow); border: 1px solid var(--accent-yellow); }
  .badge-red { background: rgba(239,68,68,0.2); color: var(--accent-red); border: 1px solid var(--accent-red); }

  .card-body { padding: 15px; }

  .ca-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; background: var(--bg-item); padding: 10px; border-radius: 10px; border: 1px solid var(--border-color); }
  .ca-text { font-family: monospace; font-size: 0.85rem; color: var(--accent-blue); }
  .social-links { display: flex; gap: 8px; }
  .social-btn { text-decoration: none; font-size: 0.9rem; background: var(--bg-card); padding: 4px 8px; border-radius: 6px; border: 1px solid var(--border-color); color: var(--text-main); }

  .section-title { font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 8px; margin-top: 15px; font-weight: 700; }
  
  .data-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .data-item { background: var(--bg-item); padding: 10px; border-radius: 10px; border: 1px solid var(--border-color); display: flex; flex-direction: column; }
  .data-label { font-size: 0.75rem; color: var(--text-muted); margin-bottom: 4px; }
  .data-val { font-size: 0.9rem; font-weight: 600; }

  .buy-sell-container { background: var(--bg-item); padding: 12px; border-radius: 10px; border: 1px solid var(--border-color); margin-top: 10px; }
  .bar-labels { display: flex; justify-content: space-between; font-size: 0.8rem; font-weight: 600; margin-bottom: 6px; }
  .bar-container { width: 100%; height: 8px; background: #334155; border-radius: 4px; display: flex; overflow: hidden; }
  .bar-buy { height: 100%; background: var(--accent-green); }
  .bar-sell { height: 100%; background: var(--accent-red); }
  .bar-txns { display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted); margin-top: 6px; }

  .quick-buy-grid { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 8px; margin-top: 8px; }
  .quick-buy-btn { text-align: center; padding: 10px 2px; background: var(--bg-item); border-radius: 10px; font-size: 0.75rem; color: #fff; text-decoration: none; border: 1px solid var(--border-color); font-weight: 600; }
  .quick-buy-btn:active { background: var(--accent-blue); }

  .card-actions { display: flex; gap: 10px; margin-top: 15px; }
  .btn-action { flex: 1; padding: 14px; border-radius: 10px; border: none; font-size: 0.85rem; font-weight: 700; cursor: pointer; text-align: center; text-transform: uppercase; }
  .btn-trade { background: var(--accent-blue); color: #fff; text-decoration: none; display: block; box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4); }
  .btn-shield { background: transparent; border: 1px solid var(--border-color); color: var(--text-main); }

  /* ✅ SHIELD MODAL (Cyberpunk Style) */
  .modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.9); backdrop-filter: blur(4px); z-index: 1000; align-items: flex-end; }
  .modal-content { background: #000; border-radius: 16px 16px 0 0; width: 100%; max-height: 90vh; overflow-y: auto; padding: 20px; border: 2px solid var(--accent-green); border-bottom: none; animation: slideUp 0.3s ease; font-family: monospace; }
  @keyframes slideUp { from { transform: translateY(100%); } to { transform: translateY(0); } }
  
  .drag-bar { width: 40px; height: 4px; background: var(--border-color); border-radius: 2px; margin: 0 auto 15px auto; }
  .shield-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; border-bottom: 2px solid var(--accent-green); padding-bottom: 10px; color: var(--accent-green); }
  .shield-title { font-size: 1.1rem; font-weight: 700; text-transform: uppercase; }
  .close-btn { background: none; border: none; color: var(--accent-green); font-size: 1.5rem; cursor: pointer; font-family: monospace; }
  
  .shield-status { display: flex; justify-content: space-between; background: #111; padding: 10px; border: 1px solid var(--border-color); margin-bottom: 15px; font-size: 0.9rem; }
  
  .audit-group { margin-bottom: 20px; }
  .audit-group-title { font-size: 0.9rem; color: var(--accent-yellow); font-weight: 700; margin-bottom: 8px; }
  .check-item { display: flex; justify-content: space-between; padding: 6px 0; font-size: 0.85rem; border-bottom: 1px dashed #222; }
  .check-status { font-weight: bold; }
  .check-status.pass { color: var(--accent-green); }
  .check-status.fail { color: var(--accent-red); }
  
  .shield-footer { text-align: center; margin-top: 20px; padding-top: 15px; border-top: 2px solid var(--accent-green); }
  .btn-close-terminal { background: transparent; border: 1px solid var(--accent-green); color: var(--accent-green); padding: 10px 20px; font-family: monospace; font-size: 0.9rem; cursor: pointer; text-transform: uppercase; }

  #loader { display: none; text-align: center; padding: 20px; color: var(--text-muted); font-size: 1rem; }
</style>
</head>
<body>

  <div class="topbar">
    <div class="logo">⚡ ALPHA VANGUARD</div>
    <div class="status-container">
      <button class="btn-top" id="modeBtn" onclick="toggleMode()">✨ NEW</button>
      <button class="btn-top" id="soundBtn" onclick="toggleSound()">🔇</button>
    </div>
  </div>

  <div class="search-container">
    <input type="text" id="manualCa" class="search-input" placeholder="Search Token or Paste CA...">
    <button class="btn-scan" onclick="checkManual()">SCAN</button>
  </div>

  <div id="loader">🔄 Scanning Blockchain... Please wait...</div>

  <div class="pill-filters">
    <div class="pill active" id="pill-all" onclick="setFilter('all')">ALL (<span id="kpi-total">0</span>)</div>
    <div class="pill" id="pill-safe" onclick="setFilter('safe')">SAFE 🟢 (<span id="kpi-safe">0</span>)</div>
    <div class="pill" id="pill-risk" onclick="setFilter('risk')">RISK 🔴 (<span id="kpi-risk">0</span>)</div>
    <div class="pill" id="pill-hype" onclick="setFilter('hype')">🔥 HYPE (<span id="kpi-hype">0</span>)</div>
  </div>

  <div class="token-list" id="tokenList">
    <div style="text-align:center; padding:20px; color:var(--text-muted);">Loading tokens...</div>
  </div>

  <div class="modal" id="intelModal" onclick="closeModal(event)">
    <div class="modal-content" onclick="event.stopPropagation()">
      <div class="drag-bar"></div>
      
      <div class="shield-header">
        <div class="shield-title" id="m-name">🛡️ SECURITY PROTOCOL</div>
        <button class="close-btn" onclick="document.getElementById('intelModal').style.display='none'">[ X ]</button>
      </div>
      
      <div class="shield-status">
        <span>STATUS: <span id="m-status-text"></span></span>
        <span>SCORE: <span id="m-score">--/24</span></span>
      </div>

      <div id="m-checks-container"></div>

      <div class="shield-footer">
        <button class="btn-close-terminal" onclick="document.getElementById('intelModal').style.display='none'">[ CLOSE TERMINAL ]</button>
      </div>
    </div>
  </div>

<script>
let allTokens = [];
let displayTokens = [];
let autoRefreshTimer = null;
let isManualMode = false;
let currentFilter = 'all';
let isFetching = false;
let currentMode = 'new';

let soundEnabled = false;
let audioCtx;
let playedMints = new Set();

function toggleMode() {
    currentMode = currentMode === 'new' ? 'trending' : 'new';
    const btn = document.getElementById('modeBtn');
    if (currentMode === 'trending') {
        btn.innerHTML = '🔥 TREND';
        btn.classList.add('active');
    } else {
        btn.innerHTML = '✨ NEW';
        btn.classList.remove('active');
    }
    document.getElementById('tokenList').innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-muted);">Loading tokens...</div>';
    loadLiveTokens();
}

function toggleSound() {
    soundEnabled = !soundEnabled;
    const btn = document.getElementById('soundBtn');
    btn.innerHTML = soundEnabled ? '🔊' : '🔇';
    btn.classList.toggle('active', soundEnabled);
    if (soundEnabled && !audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
}

function playBeep() {
    if (!soundEnabled) return;
    try {
        if (audioCtx.state === 'suspended') audioCtx.resume();
        let osc = audioCtx.createOscillator();
        let gain = audioCtx.createGain();
        osc.type = 'sine';
        osc.frequency.setValueAtTime(880, audioCtx.currentTime);
        gain.gain.setValueAtTime(0.1, audioCtx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.3);
        osc.connect(gain);
        gain.connect(audioCtx.destination);
        osc.start();
        osc.stop(audioCtx.currentTime + 0.3);
    } catch(e) { console.log("Audio error", e); }
}

function setFilter(type) {
    currentFilter = type;
    document.querySelectorAll('.pill').forEach(c => c.classList.remove('active'));
    document.getElementById('pill-' + type).classList.add('active');
    updateUI();
}

function copyCA(ca) {
    navigator.clipboard.writeText(ca);
    alert("CA Copied!");
}

async function checkManual() {
    const ca = document.getElementById('manualCa').value.trim();
    if (!ca) return;
    isManualMode = true;
    document.getElementById('loader').style.display = 'block';
    if(autoRefreshTimer) clearInterval(autoRefreshTimer);

    try {
        const res = await fetch('/api/manual-scan', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({mints: [ca]}) });
        allTokens = await res.json();
        currentFilter = 'all';
        setFilter('all');
    } catch(e) { alert("Error checking CA."); }
    finally { document.getElementById('loader').style.display = 'none'; }
}

async function loadLiveTokens() {
    if (isFetching) return; 
    isFetching = true;
    isManualMode = false;
    try {
        const res = await fetch(`/api/live-scan?mode=${currentMode}`);
        allTokens = await res.json();
        updateUI();
    } catch(e) { console.error(e); }
    finally { 
        isFetching = false;
        startAutoRefresh(); 
    }
}

function startAutoRefresh() {
    if(autoRefreshTimer) clearInterval(autoRefreshTimer);
    autoRefreshTimer = setInterval(() => { if(!isManualMode) loadLiveTokens(); }, 30000);
}

function updateUI() {
    document.getElementById('kpi-total').innerText = allTokens.length;
    document.getElementById('kpi-safe').innerText = allTokens.filter(t => t.verdict === 'GOOD').length;
    document.getElementById('kpi-risk').innerText = allTokens.filter(t => t.verdict !== 'GOOD').length;
    document.getElementById('kpi-hype').innerText = allTokens.filter(t => t.hype_label.includes('HIGH')).length;

    if (currentFilter === 'safe') {
        displayTokens = allTokens.filter(t => t.verdict === 'GOOD');
    } else if (currentFilter === 'risk') {
        displayTokens = allTokens.filter(t => t.verdict !== 'GOOD');
    } else if (currentFilter === 'hype') {
        displayTokens = allTokens.filter(t => t.hype_label.includes('HIGH'));
    } else {
        displayTokens = allTokens;
    }

    displayTokens.forEach(t => {
        if (t.verdict === 'GOOD' && !playedMints.has(t.mint)) {
            playBeep();
            playedMints.add(t.mint);
        }
    });

    renderList();
}

function renderList() {
    const list = document.getElementById('tokenList');
    list.innerHTML = '';

    if (!displayTokens || displayTokens.length === 0) {
        list.innerHTML = '<div style="text-align:center; padding:20px; color:var(--text-muted);">No tokens found for this filter.</div>';
        return;
    }

    displayTokens.forEach((t, index) => {
        let verdictClass = t.verdict === "GOOD" ? "badge-green" : (t.verdict === "CAUTION" ? "badge-yellow" : "badge-red");
        let verdictText = t.verdict === "GOOD" ? "🟢 GOOD" : (t.verdict === "CAUTION" ? "🟡 WARN" : "🔴 RISK");

        let pumpColor = t.m5_pump >= 0 ? 'var(--accent-green)' : 'var(--accent-red)';
        let h24Color = t.h24_pump >= 0 ? 'var(--accent-green)' : 'var(--accent-red)';
        let top10Color = t.top10_now <= 15 ? 'var(--accent-green)' : 'var(--accent-red)';
        let largestColor = t.largest_now <= 5 ? 'var(--accent-green)' : (t.largest_now <= 10 ? 'var(--accent-yellow)' : 'var(--accent-red)');
        let velocityColor = t.velocity > 5 ? 'var(--accent-green)' : (t.velocity > 0 ? 'var(--accent-yellow)' : 'var(--accent-red)');
        let volMcapColor = t.vol_mcap_ratio > 1 ? 'var(--accent-green)' : (t.vol_mcap_ratio > 0.5 ? 'var(--accent-yellow)' : 'var(--accent-red)');
        let devColor = t.rugged_tokens > 0 ? 'var(--accent-red)' : (t.prev_tokens > 0 ? 'var(--accent-yellow)' : 'var(--accent-green)');
        
        let devText = `${t.prev_tokens} Prev` + (t.rugged_tokens > 0 ? ` (${t.rugged_tokens} Rugged!)` : '');

        let vol_mcap_emoji = t.vol_mcap_ratio >= 2 ? ' 🔥' : (t.vol_mcap_ratio >= 1 ? ' ⚡' : '');
        let m5_emoji = t.m5_pump >= 10 ? ' 🔥' : (t.m5_pump >= 5 ? ' ⚡' : '');
        let velocity_emoji = t.velocity >= 10 ? ' 🔥' : (t.velocity >= 5 ? ' ⚡' : '');

        let narrativeHtml = `<div class="narrative-tags">`;
        if(t.narratives && t.narratives.length > 0) {
            t.narratives.forEach(n => { narrativeHtml += `<span class="narrative-tag">${n}</span>`; });
        }
        narrativeHtml += `</div>`;

        let socialHtml = `<div class="social-links">`;
        if(t.web) socialHtml += `<a href="${t.web}" target="_blank" class="social-btn">🌐</a>`;
        if(t.tw) socialHtml += `<a href="${t.tw}" target="_blank" class="social-btn">🐦</a>`;
        if(t.tg) socialHtml += `<a href="${t.tg}" target="_blank" class="social-btn">✈️</a>`;
        socialHtml += `</div>`;

        let logoHtml = '';
        if (t.image_url) {
            logoHtml = `<img src="${t.image_url}" class="token-logo" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">`;
            logoHtml += `<div class="token-logo-fallback" style="display:none;">${t.symbol.charAt(0)}</div>`;
        } else {
            logoHtml = `<div class="token-logo-fallback">${t.symbol.charAt(0)}</div>`;
        }

        let arrowM5 = t.m5_pump >= 0 ? '▲' : '▼';
        let arrowH24 = t.h24_pump >= 0 ? '▲' : '▼';

        let hypeHtml = `<div class="hype-badge" style="color:${t.hype_color};">${t.hype_label}</div>`;

        const div = document.createElement('div');
        div.className = 'token-card';
        div.innerHTML = `
            <div class="card-header">
                <div>
                    <div class="token-title">
                        <div style="display:flex; align-items:center;">${logoHtml}</div>
                        ${t.name} (${t.symbol}) 
                        <span class="token-dex">${t.dex_id}</span>
                    </div>
                    <div class="token-age">⏱️ ${t.age}</div>
                    ${narrativeHtml}
                    ${hypeHtml}
                </div>
                <div class="verdict-badge ${verdictClass}">${verdictText} ${t.score}</div>
            </div>

            <div class="card-body">
                <div class="ca-row">
                    <span class="ca-text">${t.mint.slice(0,8)}...${t.mint.slice(-6)}</span>
                    <div style="display:flex; gap:8px; align-items:center;">
                        <button class="copy-btn" style="background:none; border:none; color:var(--text-muted); cursor:pointer;" onclick="copyCA('${t.mint}')">📋</button>
                        ${socialHtml}
                    </div>
                </div>

                <div class="section-title">📊 MARKET DATA</div>
                <div class="data-grid">
                    <div class="data-item">
                        <span class="data-label">MCAP (Init ➔ Now)</span>
                        <span class="data-val">${t.mcap}</span>
                    </div>
                    <div class="data-item">
                        <span class="data-label">LIQ (Init ➔ Now)</span>
                        <span class="data-val">${t.liq}</span>
                    </div>
                    <div class="data-item">
                        <span class="data-label">5M CHANGE</span>
                        <span class="data-val" style="color:${pumpColor}">${arrowM5} ${t.pump_str}${m5_emoji}</span>
                    </div>
                    <div class="data-item">
                        <span class="data-label">24H CHANGE</span>
                        <span class="data-val" style="color:${h24Color}">${arrowH24} ${t.h24_pump_str}</span>
                    </div>
                    <div class="data-item"><span class="data-label">VOL/MCAP RATIO</span><span class="data-val" style="color:${volMcapColor}">${t.vol_mcap_str}${vol_mcap_emoji}</span></div>
                    <div class="data-item"><span class="data-label">DEV HISTORY</span><span class="data-val" style="color:${devColor}">${devText}</span></div>
                </div>

                <!-- 🌊 NEW POOL COMPOSITION SECTION -->
                <div class="section-title">🌊 POOL COMPOSITION</div>
                <div class="data-grid">
                    <div class="data-item">
                        <span class="data-label">POOLED ${t.base_symbol} (Base)</span>
                        <span class="data-val">${t.base_display} <span style="color:var(--text-muted); font-size:0.75rem;">(${t.base_usd_val})</span></span>
                    </div>
                    <div class="data-item">
                        <span class="data-label">POOLED ${t.quote_symbol} (Quote)</span>
                        <span class="data-val">${t.quote_display} <span style="color:var(--text-muted); font-size:0.75rem;">(${t.quote_usd_val})</span></span>
                    </div>
                </div>

                <div class="section-title">👥 HOLDERS & MOMENTUM</div>
                <div class="data-grid">
                    <div class="data-item"><span class="data-label">TOP 10 HOLDERS</span><span class="data-val" style="color:${top10Color}">${t.top10_display}</span></div>
                    <div class="data-item"><span class="data-label">LARGEST HOLDER</span><span class="data-val" style="color:${largestColor}">${t.largest_display}</span></div>
                    <div class="data-item"><span class="data-label">TOTAL HOLDERS</span><span class="data-val">${t.total_holders_display}</span></div>
                    <div class="data-item"><span class="data-label">HOLDER VELOCITY</span><span class="data-val" style="color:${velocityColor}">${t.velocity_str}${velocity_emoji}</span></div>
                </div>

                <div class="section-title">📈 ORDER FLOW (24H)</div>
                <div class="buy-sell-container">
                    <div class="bar-labels">
                        <span style="color:var(--accent-green);">🟢 BUY: ${t.buy_vol} (${t.buy_pct}%)</span>
                        <span style="color:var(--accent-red);">SELL: ${t.sell_vol} (${t.sell_pct}%) 🔴</span>
                    </div>
                    <div class="bar-container">
                        <div class="bar-buy" style="width:${t.buy_pct}%;"></div>
                        <div class="bar-sell" style="width:${t.sell_pct}%;"></div>
                    </div>
                    <div class="bar-txns">
                        <span>${t.buys} Buys 🟢</span>
                        <span>${t.sells} Sells 🔴</span>
                    </div>
                </div>

                <div class="section-title">⚡ QUICK BUY</div>
                <div class="quick-buy-grid">
                    <a href="https://jup.ag/swap/SOL-${t.mint}" target="_blank" class="quick-buy-btn">🪐 Jupiter</a>
                    <a href="https://photon-sol.tinyastro.io/en/lp/${t.mint}" target="_blank" class="quick-buy-btn">⚡ Photon</a>
                    <a href="https://neo.bullx.io/terminal?chainId=1399811149&address=${t.mint}" target="_blank" class="quick-buy-btn">🐂 BullX</a>
                    <a href="https://axiom.trade/t/${t.mint}" target="_blank" class="quick-buy-btn">🅰️ Axiom</a>
                </div>

                <div class="card-actions">
                    <a href="${t.dex_url}" target="_blank" class="btn-action btn-trade">🚀 TRADE ON DEX</a>
                    <button class="btn-action btn-shield" onclick="openModal(${index})">🛡️ 24-POINT SHIELD</button>
                </div>
            </div>
        `;
        list.appendChild(div);
    });
}

function openModal(index) {
    const t = displayTokens[index];
    document.getElementById('m-name').innerHTML = `🛡️ SECURITY PROTOCOL [${t.symbol}]`;
    document.getElementById('m-score').innerText = `${t.score}`;
    
    let statusColor = t.verdict === 'GOOD' ? 'var(--accent-green)' : (t.verdict === 'CAUTION' ? 'var(--accent-yellow)' : 'var(--accent-red)');
    document.getElementById('m-status-text').innerHTML = `<span style="color:${statusColor};">${t.verdict}</span>`;

    const container = document.getElementById('m-checks-container');
    
    let authChecks = t.results.filter(c => [1,2,3,4].includes(c.idx));
    let liqChecks = t.results.filter(c => [5,6,7,8,15,16,17,19,20].includes(c.idx));
    let holderChecks = t.results.filter(c => [9,10,11,12,13,14,18,21,22,23,24].includes(c.idx));

    function renderGroup(title, checks) {
        let html = `<div class="audit-group"><div class="audit-group-title">${title}</div>`;
        checks.forEach(c => {
            let icon = c.status === 'pass' ? '🟩' : '🟥';
            let col = c.status === 'pass' ? 'var(--accent-green)' : 'var(--accent-red)';
            html += `
            <div class="check-item">
                <span>${c.idx}. ${c.name}</span>
                <span class="check-status ${c.status}" style="color:${col};">${icon} ${c.detail}</span>
            </div>`;
        });
        html += `</div>`;
        return html;
    }

    container.innerHTML = 
        renderGroup("[SYS_AUTH]", authChecks) +
        renderGroup("[SYS_LIQ]", liqChecks) +
        renderGroup("[SYS_HOLDERS]", holderChecks);

    document.getElementById('intelModal').style.display = 'flex';
}

function closeModal(e) {
    if(!e || e.target.id === 'intelModal' || e.target.className === 'close-btn') {
        document.getElementById('intelModal').style.display = 'none';
    }
}

window.onload = () => loadLiveTokens();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(PAGE)

@app.route("/api/manual-scan", methods=["POST"])
def manual_scan():
    req_data = request.get_json()
    mints = req_data.get("mints", [])
    results = []
    def process_manual(mint):
        try:
            res = analyze(mint)
            save_to_history(res)
            return res
        except Exception: return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_manual, m) for m in mints]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
    results.sort(key=lambda x: x["age_seconds"])
    return jsonify(results)

@app.route("/api/live-scan")
def live_scan():
    mode = request.args.get("mode", "new")
    new_tokens_data = fetch_new_tokens(limit=5, mode=mode) 
    results = []

    def process_token(dex_data):
        mint = dex_data.get("baseToken", {}).get("address")
        if not mint: return None
        try:
            res = analyze(mint, dex_data)

            if res["mcap_gained"]:
                save_to_history(res)
                return res
            return None
        except Exception: return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: 
        futures = [executor.submit(process_token, data) for data in new_tokens_data]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
    results.sort(key=lambda x: x["age_seconds"])
    return jsonify(results)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
