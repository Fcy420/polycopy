"""
Polymarket YES+NO Arbitrage Bot  —  Event-Driven Edition
=========================================================
Strategy:
  On any binary market, YES + NO must resolve to exactly $1.00.
  If best_ask(YES) + best_ask(NO) < ARB_THRESHOLD (e.g. $0.97),
  buying both locks in a guaranteed profit at resolution.

Event-driven execution:
  Every WebSocket price_change/book event immediately triggers an arb
  check for that token's market via asyncio.create_task(). No polling
  loop — latency from price move to order submission is milliseconds,
  not up to SCAN_INTERVAL seconds.

  The polling loop still runs but only for window rollovers and stats.
"""

import asyncio
import aiohttp
import json
import os
import time
import sqlite3
import websockets
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
from py_clob_client.constants import POLYGON

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

PRIVATE_KEY     = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS  = os.getenv("FUNDER_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

HOST    = "https://clob.polymarket.com"
WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = POLYGON

ASSETS    = ["BTC", "ETH", "SOL", "XRP"]
INTERVALS = ["5m"]

# Only enter if combined YES+NO ask < this.
# 0.97 = 3% gross edge minimum. After ~2% taker fees ~1% net.
ARB_THRESHOLD   = 0.97

MAX_POSITION_USD = 4.00   # max USD to spend per arb (both sides)
MIN_SHARES       = 5      # Polymarket minimum order size
MIN_BALANCE_USD  = 2.00   # pause if wallet drops below this

DRY_RUN  = False
DB_PATH  = "arb_state.db"

# Cooldown per market key after an arb fires — prevents hammering
# the same opportunity multiple times from rapid WebSocket events
ARB_COOLDOWN_SECS = 5.0

# ══════════════════════════════════════════════════════════════════════════════
# CLOB CLIENT
# ══════════════════════════════════════════════════════════════════════════════

_executor = ThreadPoolExecutor(max_workers=8)

async def run_blocking(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(_executor, fn, *args)

client = ClobClient(
    host=HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID,
    signature_type=1, funder=FUNDER_ADDRESS,
)
credentials = client.create_or_derive_api_creds()
client.set_api_creds(credentials)

def _get_usdc_balance(self) -> float:
    params = BalanceAllowanceParams(signature_type=1, asset_type=AssetType.COLLATERAL)
    d = self.get_balance_allowance(params=params)
    return int(d["balance"]) / 10**6

ClobClient.get_usdc_balance = _get_usdc_balance


# ══════════════════════════════════════════════════════════════════════════════
# SQLITE
# ══════════════════════════════════════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS arb_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            asset        TEXT NOT NULL,
            interval     TEXT NOT NULL,
            yes_token    TEXT NOT NULL,
            no_token     TEXT NOT NULL,
            yes_ask      REAL NOT NULL,
            no_ask       REAL NOT NULL,
            combined     REAL NOT NULL,
            shares       REAL NOT NULL,
            cost         REAL NOT NULL,
            expected_pnl REAL NOT NULL,
            executed     INTEGER NOT NULL,
            reason       TEXT,
            dry_run      INTEGER NOT NULL
        )
    """)
    con.commit()
    return con

db = init_db()
_db_lock = asyncio.Lock()

async def log_arb(asset, interval, yes_token, no_token, yes_ask, no_ask,
                  shares, cost, expected_pnl, executed, reason=""):
    async with _db_lock:
        db.execute("""INSERT INTO arb_log
            (asset,interval,yes_token,no_token,yes_ask,no_ask,combined,
             shares,cost,expected_pnl,executed,reason,dry_run)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (asset, interval, yes_token, no_token, yes_ask, no_ask,
             yes_ask + no_ask, shares, cost, expected_pnl,
             int(executed), reason, int(DRY_RUN)))
        db.commit()


# ══════════════════════════════════════════════════════════════════════════════
# LIVE ORDER BOOK
# ══════════════════════════════════════════════════════════════════════════════

_live_books: dict[str, dict] = {}
_subscribed_tokens: set[str] = set()
_ws_send_queue: asyncio.Queue | None = None

# Maps token_id → market key ("BTC-5m") for fast reverse lookup in event handler
_token_to_market_key: dict[str, str] = {}

# aiohttp session reference — set in main(), used by event-driven arb trigger
_session: aiohttp.ClientSession | None = None

# Per-market-key cooldown: last time an arb was triggered
_last_arb_ts: dict[str, float] = {}

# In-flight arb task per market key — prevents stacking concurrent tasks
_arb_tasks: dict[str, asyncio.Task] = {}


def _parse_book_event(msg: dict) -> None:
    """
    Called synchronously for every WebSocket message.
    Updates the in-memory book, then immediately schedules an arb check
    via asyncio.create_task() if this token belongs to a tracked market.
    """
    token_id = msg.get("asset_id")
    if not token_id:
        return

    book = _live_books.setdefault(token_id, {"bids": {}, "asks": {}})
    ev   = msg.get("event_type")

    if ev == "book":
        book["bids"] = {float(b["price"]): float(b["size"]) for b in msg.get("bids", [])}
        book["asks"] = {float(a["price"]): float(a["size"]) for a in msg.get("asks", [])}
    elif ev == "price_change":
        for c in msg.get("changes", []):
            p, s = float(c["price"]), float(c["size"])
            d = book["bids"] if c.get("side", "").upper() == "BUY" else book["asks"]
            if s == 0:
                d.pop(p, None)
            else:
                d[p] = s
    else:
        return  # last_trade_price or other event — no action needed

    # ── Event-driven arb trigger ───────────────────────────────────────────
    # Only fire for tokens we're tracking and only if it's an ask-side change
    # (bid changes don't affect our arb entry which only uses asks)
    market_key = _token_to_market_key.get(token_id)
    if not market_key or _session is None:
        return

    # Cooldown check — avoid spawning a task for every tick
    now = time.monotonic()
    if now - _last_arb_ts.get(market_key, 0) < ARB_COOLDOWN_SECS:
        return

    # Don't stack tasks — if one is already running for this market, skip
    existing = _arb_tasks.get(market_key)
    if existing and not existing.done():
        return

    # Schedule the arb check as a fire-and-forget task
    mkt = _current_markets.get(market_key)
    if mkt:
        task = asyncio.create_task(_event_arb_check(market_key, mkt))
        _arb_tasks[market_key] = task


async def _event_arb_check(market_key: str, mkt: dict) -> None:
    """
    Lightweight arb check fired directly from the WebSocket event handler.
    Does the quick price check first before touching any async resources.
    """
    yes_id = mkt["yes"]
    no_id  = mkt["no"]

    # Quick check — if no edge, bail immediately without awaiting anything
    ya = best_ask(yes_id)
    na = best_ask(no_id)
    if ya is None or na is None or (ya + na) >= ARB_THRESHOLD:
        return

    # Update cooldown timestamp immediately to block duplicate tasks
    _last_arb_ts[market_key] = time.monotonic()

    # Full arb attempt
    await try_arb(_session, mkt)


def best_ask(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return min(b["asks"]) if b and b["asks"] else None

def best_bid(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return max(b["bids"]) if b and b["bids"] else None

def ask_liquidity_at(token_id: str, price: float) -> float:
    b = _live_books.get(token_id)
    if not b:
        return 0.0
    return sum(s for p, s in b["asks"].items() if p <= price)


async def subscribe_tokens(token_ids: list[str]) -> None:
    new = [t for t in token_ids if t not in _subscribed_tokens]
    if not new:
        return
    for t in new:
        _subscribed_tokens.add(t)
    if _ws_send_queue:
        await _ws_send_queue.put(json.dumps({"assets_ids": new, "operation": "subscribe"}))
    print(f"[WS] Subscribed to {len(new)} new token(s).")


async def ws_listener() -> None:
    global _ws_send_queue
    _ws_send_queue = asyncio.Queue()
    print("[WS] Waiting for tokens before connecting...")
    while not _subscribed_tokens:
        await asyncio.sleep(0.5)

    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=None) as ws:
                print(f"[WS] Connected — {len(_subscribed_tokens)} token(s). Event-driven arb active.")
                await ws.send(json.dumps({
                    "assets_ids": list(_subscribed_tokens), "type": "market"
                }))
                sender = asyncio.create_task(_ws_sender(ws))
                pinger = asyncio.create_task(_ws_pinger(ws))
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msgs = json.loads(raw)
                            for m in (msgs if isinstance(msgs, list) else [msgs]):
                                _parse_book_event(m)
                        except json.JSONDecodeError:
                            pass
                finally:
                    sender.cancel(); pinger.cancel()
        except Exception as exc:
            print(f"[WS] Disconnected: {exc} — reconnecting in 3s...")
            await asyncio.sleep(3)

async def _ws_sender(ws):
    while True:
        msg = await _ws_send_queue.get()
        await ws.send(msg)

async def _ws_pinger(ws):
    while True:
        await asyncio.sleep(10)
        await ws.send("PING")


# ══════════════════════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════

def current_window_ts(interval_minutes: int) -> int:
    interval_secs = interval_minutes * 60
    now = int(time.time())
    return now - (now % interval_secs)

def market_slug(asset: str, interval: str) -> str:
    minutes = int(interval.replace("m", ""))
    ts = current_window_ts(minutes)
    return f"{asset.lower()}-updown-{interval}-{ts}"

async def fetch_market_tokens(session: aiohttp.ClientSession,
                               asset: str, interval: str) -> dict | None:
    slug = market_slug(asset, interval)
    url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
    except Exception as exc:
        print(f"[Market] Fetch failed for {slug}: {exc}")
        return None

    if not data:
        return None

    event   = data[0]
    markets = event.get("markets", [])
    if not markets:
        return None

    market        = markets[0] if isinstance(markets, list) else markets
    token_ids_raw = market.get("clobTokenIds", "[]")
    try:
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
    except Exception:
        return None

    if len(token_ids) < 2:
        return None

    yes_token = token_ids[0]
    no_token  = token_ids[1]
    minutes   = int(interval.replace("m", ""))
    end_ts    = current_window_ts(minutes) + minutes * 60

    return {
        "asset":    asset,
        "interval": interval,
        "slug":     slug,
        "yes":      yes_token,
        "no":       no_token,
        "end_ts":   end_ts,
        "accepting": market.get("acceptingOrders", True),
        "closed":    market.get("closed", False),
    }


_current_markets: dict[str, dict] = {}
_last_window_ts:  dict[str, int]  = {}
_entered_this_window: set[str]    = set()


async def refresh_markets(session: aiohttp.ClientSession) -> None:
    """
    Re-fetch market token IDs whenever a new 5-min window starts.
    Registers token→market_key mapping for event-driven dispatch.
    """
    for asset in ASSETS:
        for interval in INTERVALS:
            key     = f"{asset}-{interval}"
            minutes = int(interval.replace("m", ""))
            win_ts  = current_window_ts(minutes)

            if _last_window_ts.get(key) != win_ts:
                _last_window_ts[key] = win_ts

                # Clear entered/cooldown state for this key
                _entered_this_window.discard(key)
                _last_arb_ts.pop(key, None)

                mkt = await fetch_market_tokens(session, asset, interval)
                if mkt:
                    _current_markets[key] = mkt

                    # Register token→market mapping for event-driven dispatch
                    _token_to_market_key[mkt["yes"]] = key
                    _token_to_market_key[mkt["no"]]  = key

                    await subscribe_tokens([mkt["yes"], mkt["no"]])
                    secs = mkt["end_ts"] - int(time.time())
                    print(f"[Market] {key} — new window, {secs}s remaining. "
                          f"Event-driven arb armed.")
                else:
                    print(f"[Market] {key} — not found yet")


# ══════════════════════════════════════════════════════════════════════════════
# BALANCE
# ══════════════════════════════════════════════════════════════════════════════

_cached_balance: float = 0.0
_balance_ts:     float = 0.0
BALANCE_TTL = 15

async def get_balance() -> float:
    global _cached_balance, _balance_ts
    if DRY_RUN:
        return 10.0
    now = time.monotonic()
    if now - _balance_ts < BALANCE_TTL:
        return _cached_balance
    try:
        _cached_balance = await run_blocking(client.get_usdc_balance)
        _balance_ts = now
    except Exception as exc:
        print(f"[Balance] {exc}")
    return _cached_balance


# ══════════════════════════════════════════════════════════════════════════════
# DISCORD
# ══════════════════════════════════════════════════════════════════════════════

async def send_discord(session, msg, color=0x00b0f4, title="Arb Bot"):
    if not DISCORD_WEBHOOK:
        return
    payload = {"embeds": [{"title": title, "description": msg, "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "footer": {"text": f"{'DRY RUN' if DRY_RUN else 'LIVE'} | event-driven"}}]}
    try:
        async with session.post(DISCORD_WEBHOOK, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            r.raise_for_status()
    except Exception as exc:
        print(f"[Discord] {exc}")


async def discord_arb(session, mkt, yes_ask, no_ask, shares, cost, pnl, executed, reason=""):
    combined  = yes_ask + no_ask
    gross_pct = (1.0 - combined) * 100
    net_pct   = (pnl / cost * 100) if cost > 0 else 0
    status    = (f"✅ Executed" if executed else f"⚠️ Skipped") + (f" — {reason}" if reason else "")

    msg = (
        f"**Market:** {mkt['asset']} {mkt['interval']}  "
        f"`{datetime.utcnow().strftime('%H:%M:%S')} UTC`\n"
        f"**YES ask:** ${yes_ask:.4f}  |  **NO ask:** ${no_ask:.4f}\n"
        f"**Combined:** ${combined:.4f}  |  **Gross edge:** {gross_pct:.2f}%\n"
        f"**Shares:** {shares:.4f}  |  **Cost:** ${cost:.2f}  |  "
        f"**Expected PNL:** ${pnl:.3f} ({net_pct:.2f}%)\n"
        f"**Resolves:** <t:{mkt['end_ts']}:R>\n\n{status}"
    )
    await send_discord(session, msg,
                       color=0x57F287 if executed else 0xFEE75C,
                       title=f"💹 ARB — {mkt['asset']} {mkt['interval']}")


# ══════════════════════════════════════════════════════════════════════════════
# ARB EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

async def try_arb(session, mkt: dict) -> None:
    """
    Full arb check and execution. Called both from the event-driven path
    and the periodic fallback loop.
    """
    yes_id = mkt["yes"]
    no_id  = mkt["no"]
    key    = mkt["slug"]

    if key in _entered_this_window:
        return

    if mkt.get("closed") or not mkt.get("accepting", True):
        return

    ya = best_ask(yes_id)
    na = best_ask(no_id)

    if ya is None or na is None:
        return

    combined = ya + na
    if combined >= ARB_THRESHOLD:
        return

    balance = await get_balance()
    if balance < MIN_BALANCE_USD:
        print(f"[Arb] Balance ${balance:.2f} below minimum — pausing.")
        return

    usable     = min(MAX_POSITION_USD, balance * 0.40)
    shares_raw = usable / combined
    shares     = max(MIN_SHARES, int(shares_raw * 10000) / 10000)
    cost       = round(shares * combined, 2)

    # Liquidity check
    yes_liq = ask_liquidity_at(yes_id, ya)
    no_liq  = ask_liquidity_at(no_id,  na)
    if yes_liq < shares or no_liq < shares:
        shares = int(min(yes_liq, no_liq, shares_raw) * 10000) / 10000
        if shares < MIN_SHARES:
            return
        cost = round(shares * combined, 2)

    gross_pnl = round(shares * (1.0 - combined), 4)
    est_fees  = round(cost * 0.02, 4)
    net_pnl   = round(gross_pnl - est_fees, 4)

    if net_pnl <= 0:
        return

    secs_left = mkt["end_ts"] - int(time.time())
    if secs_left < 15:
        return

    gross_pct = (gross_pnl / cost) * 100

    print(
        f"[ARB⚡] {mkt['asset']} {mkt['interval']} — "
        f"YES ${ya:.3f} + NO ${na:.3f} = ${combined:.3f}  "
        f"edge {gross_pct:.2f}%  shares={shares}  cost=${cost:.2f}  "
        f"net ~${net_pnl:.3f}  ({secs_left}s left)"
    )

    if DRY_RUN:
        _entered_this_window.add(key)
        await log_arb(mkt["asset"], mkt["interval"], yes_id, no_id,
                      ya, na, shares, cost, net_pnl, True, "dry_run")
        await discord_arb(session, mkt, ya, na, shares, cost, net_pnl, True)
        print(f"  [DRY RUN] Would buy {shares} YES @ ${ya:.4f} + {shares} NO @ ${na:.4f}")
        return

    # ── Place both orders simultaneously ──────────────────────────────────
    try:
        yes_price  = round(ya, 2)
        no_price   = round(na, 2)
        yes_shares = int(shares * 10000) / 10000
        no_shares  = int(shares * 10000) / 10000

        yes_signed, no_signed = await asyncio.gather(
            run_blocking(client.create_order,
                OrderArgs(token_id=yes_id, price=yes_price, size=yes_shares, side="BUY")),
            run_blocking(client.create_order,
                OrderArgs(token_id=no_id,  price=no_price,  size=no_shares,  side="BUY")),
        )

        await asyncio.gather(
            run_blocking(client.post_order, yes_signed, OrderType.FAK),
            run_blocking(client.post_order, no_signed,  OrderType.FAK),
        )

        _entered_this_window.add(key)
        await log_arb(mkt["asset"], mkt["interval"], yes_id, no_id,
                      ya, na, shares, cost, net_pnl, True)
        await discord_arb(session, mkt, ya, na, shares, cost, net_pnl, True)

    except Exception as exc:
        reason = str(exc)
        print(f"  [ARB] Order failed: {reason}")
        await log_arb(mkt["asset"], mkt["interval"], yes_id, no_id,
                      ya, na, shares, cost, net_pnl, False, reason)
        await discord_arb(session, mkt, ya, na, shares, cost, net_pnl, False, reason)


# ══════════════════════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════════════════════

async def print_stats(session) -> None:
    rows = db.execute("""
        SELECT COUNT(*), 
               SUM(CASE WHEN executed=1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN executed=1 THEN expected_pnl ELSE 0 END),
               SUM(CASE WHEN executed=1 THEN cost ELSE 0 END),
               AVG(CASE WHEN executed=1 THEN combined ELSE NULL END)
        FROM arb_log WHERE dry_run=?
    """, (int(DRY_RUN),)).fetchone()

    if not rows or rows[0] == 0:
        return

    total, executed, pnl, cost, avg_comb = rows
    roi = (pnl / cost * 100) if cost and cost > 0 else 0

    msg = (
        f"**Opportunities seen:** {total}\n"
        f"**Executed:** {executed}\n"
        f"**Total expected PNL:** ${pnl:.3f}\n"
        f"**Total capital deployed:** ${cost:.2f}\n"
        f"**Avg combined price:** ${avg_comb:.4f}\n"
        f"**ROI:** {roi:.2f}%"
    )
    print(f"\n[Stats] {msg.replace('**','').replace(chr(10),' | ')}")
    await send_discord(session, msg, color=0x5865F2, title="📊 Arb Stats")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    global _session

    async with aiohttp.ClientSession() as session:
        _session = session  # make available to event-driven dispatch

        print("=" * 62)
        print("  Polymarket YES+NO Arbitrage Bot  [EVENT-DRIVEN]")
        print(f"  Mode         : {'DRY RUN' if DRY_RUN else 'LIVE'}")
        print(f"  Assets       : {', '.join(ASSETS)}")
        print(f"  Intervals    : {', '.join(INTERVALS)}")
        print(f"  Threshold    : combined ask < ${ARB_THRESHOLD:.2f}")
        print(f"  Max position : ${MAX_POSITION_USD:.2f} per arb")
        print(f"  Cooldown     : {ARB_COOLDOWN_SECS}s per market after trigger")
        print(f"  Latency      : milliseconds from price change to order")
        print("=" * 62)

        await send_discord(session,
            f"**{'Dry run' if DRY_RUN else 'LIVE'}** arb bot started — **event-driven**\n"
            f"Watching: {', '.join(ASSETS)} | Threshold: **<${ARB_THRESHOLD:.2f}** | "
            f"Max: **${MAX_POSITION_USD:.2f}**\n"
            f"Orders fire within milliseconds of a price change.",
            color=0x5865F2 if DRY_RUN else 0xED4245,
            title="💹 Arb Bot Started",
        )

        await refresh_markets(session)
        ws_task = asyncio.create_task(ws_listener())

        cycle = 0
        while True:
            cycle += 1

            # Periodic: handle window rollovers and subscribe new markets
            await refresh_markets(session)

            # Fallback poll — catches any opportunities missed between events
            # (e.g. while WS was reconnecting)
            for key, mkt in list(_current_markets.items()):
                existing = _arb_tasks.get(key)
                if not existing or existing.done():
                    await try_arb(session, mkt)

            if cycle % 60 == 0:
                bal = await get_balance()
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Balance: ${bal:.2f}  "
                      f"Markets: {len(_current_markets)}  Books: {len(_live_books)}  "
                      f"Tasks fired: {sum(1 for t in _arb_tasks.values() if not t.done())}")
                await print_stats(session)

            if ws_task.done():
                print("[WS] Task died — restarting...")
                ws_task = asyncio.create_task(ws_listener())

            await asyncio.sleep(5)  # poll loop only for housekeeping now


if __name__ == "__main__":
    asyncio.run(main())