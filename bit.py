"""
Polymarket 5-Minute Directional Bot  —  Limit Order (Maker) Edition
=====================================================================
Strategy:
  Streams real-time prices from Binance WebSocket for BTC/ETH/SOL.
  Detects momentum moves and posts GTC limit orders positioned just
  inside the spread — earning maker rebate instead of paying 2% taker.

  Order lifecycle:
    1. Momentum signal detected + Polymarket mispricing confirmed
    2. GTC limit posted at (best_ask - LIMIT_OFFSET) → rests in book
    3. order_manager polls every 2s for fill confirmation
    4. If window closes in < CANCEL_BEFORE_EXPIRY seconds → cancel
       (unfilled orders on an expired market would buy worthless shares)

  Maker vs taker:
    Taker (FAK): crosses the spread, fills instantly, pays ~2% fee
    Maker (GTC): rests in book, fills when someone hits it, pays ~0% fee
    On $3 per bet, that's $0.06 saved every single trade.

  Assumes low latency to Polymarket servers — limit orders require
  fast placement and cancellation to avoid adverse selection.
"""

import asyncio
import aiohttp
import json
import os
import time
import sqlite3
import websockets
from collections import deque
from datetime import datetime, timezone
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
DISCORD_WEBHOOK = os.getenv("BIT_WEBHOOK")

HOST     = "https://clob.polymarket.com"
CHAIN_ID = POLYGON

# Assets to watch — each needs a Polymarket 5m up/down market and a Binance symbol
ASSETS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
}

# Binance combined stream (no auth needed)
_binance_streams = "/".join(f"{sym}@trade" for sym in ASSETS.values())
BINANCE_WS = f"wss://stream.binance.com:9443/stream?streams={_binance_streams}"

# Polymarket WebSocket for order book
POLY_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Strategy ──────────────────────────────────────────────────────────────────

# How far back to measure momentum (seconds)
MOMENTUM_WINDOW     = 120       # 2 minutes of BTC price history

# Minimum % price move to consider a clear signal
MOMENTUM_THRESHOLD  = 0.0015    # 0.15% move in 2 minutes

# Only bet when Polymarket price disagrees with signal by this much
# e.g. signal says UP but YES is only $0.40 — that's a 10c discount vs fair $0.50
MIN_EDGE            = 0.06      # 6 cents minimum edge vs implied 50/50

# Don't enter if less than this many seconds remain in the window
MIN_SECONDS_REMAINING = 90

# Max USD to bet per window
MAX_POSITION_USD    = 3.00
MIN_BALANCE_USD     = 2.00

# Cooldown after a bet fires — prevents double-entry on rapid price moves
BET_COOLDOWN_SECS   = 30.0

# ── Limit order config ────────────────────────────────────────────────────────

# Post this many cents BELOW the best ask — keeps us as maker in the book.
# 0.01 = 1 cent inside the spread. Tighter = more likely to fill but less edge.
# 0.02 = 2 cents inside. More conservative, fills less often.
LIMIT_OFFSET        = 0.01

# Cancel open orders this many seconds before window expiry.
# Must be enough time for the cancel API call to process (~2-5s).
CANCEL_BEFORE_EXPIRY = 25

# How often the order manager polls for fill status (seconds).
ORDER_POLL_INTERVAL = 2

DRY_RUN = True   # ← set False to go live
DB_PATH = "bit_state.db"

# ══════════════════════════════════════════════════════════════════════════════
# CLOB CLIENT
# ══════════════════════════════════════════════════════════════════════════════

_executor = ThreadPoolExecutor(max_workers=4)

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
        CREATE TABLE IF NOT EXISTS bet_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ts               INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            asset            TEXT NOT NULL DEFAULT 'BTC',
            window_ts        INTEGER NOT NULL,
            side             TEXT NOT NULL,
            token_id         TEXT NOT NULL,
            price            REAL NOT NULL,
            shares           REAL NOT NULL,
            cost             REAL NOT NULL,
            momentum_pct     REAL NOT NULL,
            yes_ask          REAL NOT NULL,
            no_ask           REAL NOT NULL,
            edge             REAL NOT NULL,
            order_id         TEXT,
            filled           INTEGER NOT NULL DEFAULT 0,
            executed         INTEGER NOT NULL,
            reason           TEXT,
            outcome          TEXT,
            won              INTEGER,
            dry_run          INTEGER NOT NULL
        )
    """)
    # Migrations for existing DBs
    for col, defn in [
        ("asset",    "TEXT NOT NULL DEFAULT 'BTC'"),
        ("order_id", "TEXT"),
        ("filled",   "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            con.execute(f"ALTER TABLE bet_log ADD COLUMN {col} {defn}")
            con.commit()
            print(f"[DB] Migrated: added bet_log.{col}")
        except Exception:
            pass
    con.commit()
    return con

db     = init_db()
_dlock = asyncio.Lock()


async def log_bet(asset, window_ts, side, token_id, price, shares, cost,
                  momentum_pct, yes_ask, no_ask, edge, executed,
                  reason="", order_id=None) -> int:
    """Inserts a bet record and returns its row id."""
    async with _dlock:
        cur = db.execute("""INSERT INTO bet_log
            (asset,window_ts,side,token_id,price,shares,cost,momentum_pct,
             yes_ask,no_ask,edge,order_id,executed,reason,dry_run)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (asset, window_ts, side, token_id, price, shares, cost, momentum_pct,
             yes_ask, no_ask, edge, order_id, int(executed), reason, int(DRY_RUN)))
        db.commit()
        return cur.lastrowid


async def mark_filled(row_id: int) -> None:
    async with _dlock:
        db.execute("UPDATE bet_log SET filled=1 WHERE id=?", (row_id,))
        db.commit()


async def mark_cancelled(row_id: int) -> None:
    async with _dlock:
        db.execute("UPDATE bet_log SET reason='cancelled_unfilled' WHERE id=?", (row_id,))
        db.commit()


# ══════════════════════════════════════════════════════════════════════════════
# PRICE FEED  (Binance combined stream)
# ══════════════════════════════════════════════════════════════════════════════

# Per-asset: rolling (monotonic_ts, price) deque and latest price
_prices:  dict[str, deque] = {a: deque() for a in ASSETS}
_latest:  dict[str, float] = {a: 0.0    for a in ASSETS}

# Maps lowercase Binance symbol → asset key  e.g. "btcusdt" → "BTC"
_sym_to_asset = {sym: asset for asset, sym in ASSETS.items()}


def get_momentum(asset: str) -> tuple[float, str]:
    """
    Returns (momentum_pct, signal) for the given asset.
    signal is 'UP', 'DOWN', or 'FLAT'.
    """
    prices = _prices[asset]
    latest = _latest[asset]
    now    = time.monotonic()

    while prices and now - prices[0][0] > MOMENTUM_WINDOW:
        prices.popleft()

    if len(prices) < 5 or latest == 0:
        return 0.0, "FLAT"

    pct = (latest - prices[0][1]) / prices[0][1]

    if pct >= MOMENTUM_THRESHOLD:
        return pct, "UP"
    elif pct <= -MOMENTUM_THRESHOLD:
        return pct, "DOWN"
    return pct, "FLAT"


async def binance_feed() -> None:
    """Streams trade prices for all assets from Binance combined stream."""
    print(f"[Binance] Connecting — watching {', '.join(ASSETS.keys())}...")
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20) as ws:
                print("[Binance] Connected.")
                async for raw in ws:
                    try:
                        # Combined stream wraps messages: {"stream": "btcusdt@trade", "data": {...}}
                        outer = json.loads(raw)
                        msg   = outer.get("data", outer)
                        sym   = msg.get("s", "").lower().replace("@trade", "")
                        asset = _sym_to_asset.get(sym)
                        if not asset:
                            continue
                        price = float(msg["p"])
                        _latest[asset] = price
                        _prices[asset].append((time.monotonic(), price))
                        if len(_prices[asset]) > 3000:
                            _prices[asset].popleft()
                    except Exception:
                        pass
        except Exception as exc:
            print(f"[Binance] Disconnected: {exc} — reconnecting in 3s...")
            await asyncio.sleep(3)


# ══════════════════════════════════════════════════════════════════════════════
# POLYMARKET ORDER BOOK  (WebSocket)
# ══════════════════════════════════════════════════════════════════════════════

_live_books:        dict[str, dict]  = {}
_subscribed_tokens: set[str]         = set()
_ws_send_queue:     asyncio.Queue | None = None


def _parse_book_event(msg: dict) -> None:
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


def best_ask(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return min(b["asks"]) if b and b["asks"] else None

def best_bid(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return max(b["bids"]) if b and b["bids"] else None


async def subscribe_tokens(token_ids: list[str]) -> None:
    new = [t for t in token_ids if t not in _subscribed_tokens]
    if not new:
        return
    for t in new:
        _subscribed_tokens.add(t)
    if _ws_send_queue:
        await _ws_send_queue.put(json.dumps({"assets_ids": new, "operation": "subscribe"}))
    print(f"[WS] Subscribed to {len(new)} token(s).")


async def poly_ws_listener() -> None:
    global _ws_send_queue
    _ws_send_queue = asyncio.Queue()
    while not _subscribed_tokens:
        await asyncio.sleep(0.5)
    while True:
        try:
            async with websockets.connect(POLY_WSS, ping_interval=None) as ws:
                print(f"[WS] Connected — {len(_subscribed_tokens)} token(s).")
                await ws.send(json.dumps({"assets_ids": list(_subscribed_tokens), "type": "market"}))
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
                    sender.cancel()
                    pinger.cancel()
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

def current_window_ts() -> int:
    now = int(time.time())
    return now - (now % 300)  # 5-minute boundary



# ══════════════════════════════════════════════════════════════════════════════
# BALANCE
# ══════════════════════════════════════════════════════════════════════════════

_cached_balance: float = 0.0
_balance_ts:     float = 0.0
BALANCE_TTL = 20

async def get_balance() -> float:
    global _cached_balance, _balance_ts
    if DRY_RUN:
        return 20.0
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

async def send_discord(session, msg, color=0x00b0f4, title="BTC Bot") -> None:
    if not DISCORD_WEBHOOK:
        return
    payload = {"embeds": [{"title": title, "description": msg, "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"{'DRY RUN' if DRY_RUN else 'LIVE'} | BTC 5m"}}]}
    try:
        async with session.post(DISCORD_WEBHOOK, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            r.raise_for_status()
    except Exception as exc:
        print(f"[Discord] {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# OUTCOME CHECKER  — resolves past bets
# ══════════════════════════════════════════════════════════════════════════════

async def outcome_checker(session: aiohttp.ClientSession) -> None:
    """
    Every 2 minutes, check unresolved bets against Polymarket outcomePrices.
    Updates won/outcome in bet_log and prints a running P&L.
    """
    await asyncio.sleep(60)
    while True:
        try:
            rows = db.execute("""
                SELECT id, side, token_id, cost, price
                FROM bet_log
                WHERE executed=1 AND won IS NULL AND dry_run=?
            """, (int(DRY_RUN),)).fetchall()

            for row_id, side, token_id, cost, entry_price in rows:
                try:
                    url = f"https://gamma-api.polymarket.com/markets?clob_token_ids={token_id}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json()
                    if not data:
                        continue
                    market = data[0]
                    raw    = market.get("outcomePrices", "[]")
                    prices = json.loads(raw) if isinstance(raw, str) else raw
                    if len(prices) < 2:
                        continue
                    yes_price = float(prices[0])
                    if yes_price not in (0.0, 1.0):
                        continue  # not resolved yet

                    winning_side = "YES" if yes_price == 1.0 else "NO"
                    won          = int(side == winning_side)
                    pnl          = round((cost / entry_price) - cost, 4) if won else -cost

                    async with _dlock:
                        db.execute(
                            "UPDATE bet_log SET outcome=?, won=? WHERE id=?",
                            (winning_side, won, row_id)
                        )
                        db.commit()

                    result = "WIN" if won else "LOSS"
                    print(f"[Outcome] Bet {row_id} ({side}) → {result}  PNL ~${pnl:+.3f}")

                except Exception as exc:
                    print(f"[Outcome] Error on bet {row_id}: {exc}")

        except Exception as exc:
            print(f"[Outcome] Monitor error: {exc}")

        await asyncio.sleep(120)


# ══════════════════════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════════════════════

async def print_stats(session) -> None:
    rows = db.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN executed=1 THEN 1 ELSE 0 END) as executed,
            SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN won=1 THEN (cost/price)-cost ELSE CASE WHEN won=0 THEN -cost ELSE 0 END END) as est_pnl
        FROM bet_log WHERE dry_run=?
    """, (int(DRY_RUN),)).fetchone()

    if not rows or rows[0] == 0:
        return

    _, executed, wins, losses, pnl = rows
    resolved = (wins or 0) + (losses or 0)
    win_rate = f"{wins/resolved*100:.0f}%" if resolved else "pending"
    pnl      = pnl or 0

    msg = (
        f"**Bets placed:** {executed}  |  **Resolved:** {resolved}\n"
        f"**Wins:** {wins or 0}  |  **Losses:** {losses or 0}  |  **W/R:** {win_rate}\n"
        f"**Est. PNL:** ${pnl:.3f}\n"
        f"**Prices:** " + "  ".join(f"{a}=${_latest[a]:,.2f}" for a in ASSETS if _latest[a])
    )
    print(f"[Stats] {msg.replace('**','').replace(chr(10),' | ')}")
    await send_discord(session, msg, color=0x5865F2, title="📊 BTC Bot Stats")


# ══════════════════════════════════════════════════════════════════════════════
# CORE LOGIC
# ══════════════════════════════════════════════════════════════════════════════

# Per-asset: set of win_ts already bet, and per-asset cooldown timestamp
_entered_windows: dict[str, set]   = {a: set()  for a in ASSETS}
_last_bet_ts:     dict[str, float] = {a: 0.0    for a in ASSETS}
_current_markets: dict[str, dict]  = {}   # asset → market dict

# Tracks live GTC orders awaiting fill or cancellation
# asset → {order_id, row_id, token_id, win_ts, shares, price, side}
_open_orders: dict[str, dict | None] = {a: None for a in ASSETS}


async def fetch_market(session: aiohttp.ClientSession, asset: str) -> dict | None:
    win_ts = current_window_ts()
    slug   = f"{asset.lower()}-updown-5m-{win_ts}"
    url    = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            data = await r.json()
    except Exception as exc:
        print(f"[Market:{asset}] Fetch failed: {exc}")
        return None

    if not data:
        return None

    markets = data[0].get("markets", [])
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

    return {
        "asset":     asset,
        "slug":      slug,
        "yes":       token_ids[0],
        "no":        token_ids[1],
        "end_ts":    win_ts + 300,
        "win_ts":    win_ts,
        "accepting": market.get("acceptingOrders", True),
        "closed":    market.get("closed", False),
    }


async def maybe_bet(session: aiohttp.ClientSession, asset: str) -> None:
    """
    Called every second per asset. When signal + edge conditions are met,
    posts a GTC limit order just inside the spread (maker position).
    The order_manager task handles fill tracking and pre-expiry cancellation.
    """
    mkt = _current_markets.get(asset)
    if not mkt:
        return

    win_ts = mkt["win_ts"]
    if win_ts in _entered_windows[asset]:
        return

    # Don't place a new order if one is already resting in the book
    if _open_orders[asset] is not None:
        return

    if mkt.get("closed") or not mkt.get("accepting", True):
        return

    secs_left = mkt["end_ts"] - int(time.time())
    if secs_left < MIN_SECONDS_REMAINING:
        return

    if time.monotonic() - _last_bet_ts[asset] < BET_COOLDOWN_SECS:
        return

    momentum_pct, signal = get_momentum(asset)
    if signal == "FLAT":
        return

    yes_ask = best_ask(mkt["yes"])
    no_ask  = best_ask(mkt["no"])
    if yes_ask is None or no_ask is None:
        return

    balance = await get_balance()
    if balance < MIN_BALANCE_USD:
        print(f"[{asset}] Balance ${balance:.2f} below minimum.")
        return

    if signal == "UP":
        side     = "YES"
        token_id = mkt["yes"]
        ask      = yes_ask
        edge     = 0.50 - yes_ask
    else:
        side     = "NO"
        token_id = mkt["no"]
        ask      = no_ask
        edge     = 0.50 - no_ask

    if edge < MIN_EDGE:
        return

    # Limit price: sit just below the ask so we rest as maker.
    # If someone sells into the book at our price, we fill for free.
    limit_price = round(max(0.01, ask - LIMIT_OFFSET), 2)
    usable      = min(MAX_POSITION_USD, balance * 0.25)
    shares      = max(1, int(usable / limit_price))
    cost        = round(shares * limit_price, 2)

    print(
        f"[LIMIT:{asset}] {side}  momentum={momentum_pct*100:+.3f}%  "
        f"ask=${ask:.3f}  limit=${limit_price:.3f}  edge={edge:.3f}  "
        f"shares={shares}  cost=${cost:.2f}  ({secs_left}s left)"
    )

    if DRY_RUN:
        _entered_windows[asset].add(win_ts)
        _last_bet_ts[asset] = time.monotonic()
        row_id = await log_bet(asset, win_ts, side, token_id, limit_price,
                               shares, cost, momentum_pct, yes_ask, no_ask,
                               edge, True, "dry_run_limit")
        # Simulate an open order so order_manager can exercise the cancel path
        _open_orders[asset] = {
            "order_id": f"dry-{row_id}",
            "row_id":   row_id,
            "token_id": token_id,
            "win_ts":   win_ts,
            "shares":   shares,
            "price":    limit_price,
            "side":     side,
            "asset":    asset,
        }
        await send_discord(session,
            f"**[DRY RUN]** GTC limit {side} @ **${limit_price:.3f}**\n"
            f"Momentum: **{momentum_pct*100:+.3f}%** over {MOMENTUM_WINDOW}s\n"
            f"Best ask: **${ask:.3f}**  |  Limit: **${limit_price:.3f}** (maker)\n"
            f"Edge: **{edge:.3f}**  |  Shares: **{shares}**  |  Max cost: **${cost:.2f}**\n"
            f"Window closes in **{secs_left}s**",
            color=0x57F287 if side == "YES" else 0xED4245,
            title=f"📋 {asset} {side} LIMIT — DRY RUN"
        )
        return

    try:
        signed = await run_blocking(client.create_order,
            OrderArgs(token_id=token_id, price=limit_price,
                      size=float(shares), side="BUY"))
        resp = await run_blocking(client.post_order, signed, OrderType.GTC)

        # Extract order id from response
        order_id = resp.get("orderID") or resp.get("order_id") or str(resp)

        _entered_windows[asset].add(win_ts)
        _last_bet_ts[asset] = time.monotonic()

        row_id = await log_bet(asset, win_ts, side, token_id, limit_price,
                               shares, cost, momentum_pct, yes_ask, no_ask,
                               edge, True, order_id=order_id)

        _open_orders[asset] = {
            "order_id": order_id,
            "row_id":   row_id,
            "token_id": token_id,
            "win_ts":   win_ts,
            "shares":   shares,
            "price":    limit_price,
            "side":     side,
            "asset":    asset,
        }

        await send_discord(session,
            f"**GTC limit placed** — {side} @ **${limit_price:.3f}**\n"
            f"Momentum: **{momentum_pct*100:+.3f}%** over {MOMENTUM_WINDOW}s\n"
            f"Best ask: **${ask:.3f}**  |  Limit: **${limit_price:.3f}** (maker)\n"
            f"Edge: **{edge:.3f}**  |  Shares: **{shares}**  |  Max cost: **${cost:.2f}**\n"
            f"Window closes in **{secs_left}s**  |  Order: `{order_id}`",
            color=0x57F287 if side == "YES" else 0xED4245,
            title=f"📋 {asset} {side} LIMIT — LIVE"
        )

    except Exception as exc:
        reason = str(exc)
        print(f"[LIMIT:{asset}] Order failed: {reason}")
        await log_bet(asset, win_ts, side, token_id, limit_price,
                      shares, cost, momentum_pct, yes_ask, no_ask,
                      edge, False, reason)


async def order_manager(session: aiohttp.ClientSession) -> None:
    """
    Polls open GTC limit orders every ORDER_POLL_INTERVAL seconds.
    - Marks filled orders in the DB
    - Cancels orders when the window is about to expire
    """
    while True:
        await asyncio.sleep(ORDER_POLL_INTERVAL)
        for asset in list(ASSETS.keys()):
            order = _open_orders[asset]
            if order is None:
                continue

            mkt       = _current_markets.get(asset)
            secs_left = (mkt["end_ts"] - int(time.time())) if mkt else 0
            order_id  = order["order_id"]
            row_id    = order["row_id"]

            # ── Cancel before window expiry ────────────────────────────────
            if secs_left <= CANCEL_BEFORE_EXPIRY:
                if DRY_RUN:
                    print(f"[OrderMgr:{asset}] DRY RUN — would cancel {order_id} ({secs_left}s left)")
                    await mark_cancelled(row_id)
                    _open_orders[asset] = None
                    continue

                try:
                    await run_blocking(client.cancel, order_id)
                    print(f"[OrderMgr:{asset}] Cancelled {order_id} — {secs_left}s left")
                    await mark_cancelled(row_id)
                    await send_discord(session,
                        f"**Order cancelled** — window closing\n"
                        f"Order `{order_id}` | {secs_left}s remaining",
                        color=0xFEE75C, title=f"⏱ {asset} Order Cancelled"
                    )
                except Exception as exc:
                    print(f"[OrderMgr:{asset}] Cancel failed: {exc}")
                finally:
                    _open_orders[asset] = None
                continue

            # ── Check fill status ──────────────────────────────────────────
            if DRY_RUN:
                # In dry run, simulate fill if bid has risen to our limit price
                bid = best_bid(order["token_id"])
                if bid is not None and bid >= order["price"]:
                    print(f"[OrderMgr:{asset}] DRY RUN — simulated fill @ ${bid:.3f}")
                    await mark_filled(row_id)
                    _open_orders[asset] = None
                    await send_discord(session,
                        f"**[DRY RUN]** Limit filled — {order['side']} @ **${order['price']:.3f}**\n"
                        f"Shares: **{order['shares']}**  |  {secs_left}s left in window",
                        color=0x57F287, title=f"✅ {asset} LIMIT FILLED — DRY RUN"
                    )
                continue

            try:
                resp   = await run_blocking(client.get_order, order_id)
                status = resp.get("status", "").upper()

                if status in ("MATCHED", "FILLED"):
                    print(f"[OrderMgr:{asset}] Filled: {order_id}")
                    await mark_filled(row_id)
                    _open_orders[asset] = None
                    await send_discord(session,
                        f"**Limit filled** — {order['side']} @ **${order['price']:.3f}**\n"
                        f"Shares: **{order['shares']}**  |  {secs_left}s left in window\n"
                        f"Order: `{order_id}`",
                        color=0x57F287, title=f"✅ {asset} LIMIT FILLED"
                    )
                elif status in ("CANCELLED", "EXPIRED"):
                    print(f"[OrderMgr:{asset}] Order {status}: {order_id}")
                    await mark_cancelled(row_id)
                    _open_orders[asset] = None

            except Exception as exc:
                print(f"[OrderMgr:{asset}] Status check failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    async with aiohttp.ClientSession() as session:

        assets_str = ", ".join(ASSETS.keys())
        print("=" * 60)
        print(f"  Polymarket 5-Minute Directional Bot")
        print(f"  Assets       : {assets_str}")
        print(f"  Mode         : {'DRY RUN' if DRY_RUN else 'LIVE'}")
        print(f"  Momentum win : {MOMENTUM_WINDOW}s  |  threshold: {MOMENTUM_THRESHOLD*100:.2f}%")
        print(f"  Min edge     : {MIN_EDGE:.2f}  |  Max position: ${MAX_POSITION_USD:.2f}")
        print(f"  Min time left: {MIN_SECONDS_REMAINING}s")
        print(f"  Limit offset : {LIMIT_OFFSET:.2f}  |  Cancel before: {CANCEL_BEFORE_EXPIRY}s")
        print("=" * 60)

        await send_discord(session,
            f"**{'Dry run' if DRY_RUN else 'LIVE'}** 5m bot started — **{assets_str}**\n"
            f"Momentum window: **{MOMENTUM_WINDOW}s** | Threshold: **{MOMENTUM_THRESHOLD*100:.2f}%**\n"
            f"Min edge: **{MIN_EDGE:.2f}** | Max bet: **${MAX_POSITION_USD:.2f}**",
            color=0x5865F2 if DRY_RUN else 0xED4245,
            title="🚀 Directional Bot Started"
        )

        binance_task  = asyncio.create_task(binance_feed())
        ws_task       = asyncio.create_task(poly_ws_listener())
        outcome_task  = asyncio.create_task(outcome_checker(session))
        ordermgr_task = asyncio.create_task(order_manager(session))

        # Wait for at least one price to arrive
        print("[Init] Waiting for price feeds...")
        while not any(_latest[a] for a in ASSETS):
            await asyncio.sleep(0.5)
        print("[Init] Feeds live — " + "  ".join(f"{a}=${_latest[a]:,.2f}" for a in ASSETS if _latest[a]))
        print(f"[Init] Warming up {MOMENTUM_WINDOW}s momentum window...")

        cycle    = 0
        last_win = 0

        while True:
            cycle += 1

            # Refresh all markets on each new 5-minute window
            win_ts = current_window_ts()
            if win_ts != last_win:
                last_win = win_ts
                secs     = win_ts + 300 - int(time.time())
                for asset in ASSETS:
                    mkt = await fetch_market(session, asset)
                    if mkt:
                        _current_markets[asset] = mkt
                        await subscribe_tokens([mkt["yes"], mkt["no"]])
                        print(f"[Market:{asset}] New window — {secs}s remaining  slug={mkt['slug']}")
                    else:
                        print(f"[Market:{asset}] Not found yet")

            # Check each asset for a bet opportunity
            for asset in ASSETS:
                await maybe_bet(session, asset)

            if cycle % 60 == 0:
                ts = datetime.now().strftime('%H:%M:%S')
                for asset in ASSETS:
                    mkt  = _current_markets.get(asset)
                    pct, sig = get_momentum(asset)
                    ya   = best_ask(mkt["yes"]) if mkt else None
                    na   = best_ask(mkt["no"])  if mkt else None
                    secs = (mkt["end_ts"] - int(time.time())) if mkt else 0
                    price_str = f"${_latest[asset]:,.2f}" if _latest[asset] else "—"
                    book_str  = f"YES=${ya:.3f} NO=${na:.3f}" if ya and na else "no book"
                    print(f"[{ts}] {asset} {price_str}  {pct*100:+.3f}% ({sig})  {book_str}  {secs}s left")

            if cycle % 300 == 0:
                await print_stats(session)

            if binance_task.done():
                binance_task = asyncio.create_task(binance_feed())
            if ws_task.done():
                ws_task = asyncio.create_task(poly_ws_listener())
            if outcome_task.done():
                outcome_task = asyncio.create_task(outcome_checker(session))
            if ordermgr_task.done():
                ordermgr_task = asyncio.create_task(order_manager(session))

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
