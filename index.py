
"""
Polymarket Copy-Trade Bot
"""
 
import asyncio
import aiohttp
import json
import os
import sqlite3
import time
import websockets
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
 
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
from py_clob_client.constants import POLYGON
 
load_dotenv()
 
TARGET_WALLETS = [
    {
        "address":  "0x204f72f35326db932158cba6adff0b9a1da95e14",
        "label":    "swisstony",
        "copy_pct": 0.2,
    },
]
 
PRIVATE_KEY     = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS  = os.getenv("FUNDER_ADDRESS")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
 
HOST    = "https://clob.polymarket.com"
WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = POLYGON
 
COPY_PERCENT       = 0.2
MAX_WALLET_PERCENT = 0.40
MAX_MARKET_PERCENT = 0.15
MIN_TRADE_SIZE     = 0.20
SLIPPAGE_TOLERANCE = 0.06
MAX_SPREAD         = 0.12
STOP_LOSS_PCT      = 0.90
 
CONVICTION_MULTIPLIER = 0.0
CONVICTION_LOOKBACK   = 20

PRICE_LAG_MAX        = 0.15   # skip BUY if current ask is >15% above whale's price
WIN_RATE_CHECK_INTERVAL = 300  # check resolved markets every 5 minutes
 
DYNAMIC_TARGETS      = False
DYNAMIC_TARGET_COUNT = 10
DYNAMIC_ROTATE_EVERY = 30
 
POLL_INTERVAL      = 10
MAX_SEEN_IDS       = 1000
BALANCE_TTL        = 30
WS_PING_INTERVAL   = 10
STOP_LOSS_INTERVAL = 60
 
# Smart polling — faster during active hours, slower overnight
POLL_INTERVAL_ACTIVE  = 5    # seconds during US sports prime time
POLL_INTERVAL_QUIET   = 30   # seconds overnight
 
# Target quality: skip wallets with volume below this threshold
# High PNL on low volume = lucky one-off bet, not consistent edge
MIN_TARGET_VOLUME = 1000  # minimum $1000 traded volume this month
 
# Reconcile positions DB against live API every N cycles
POSITIONS_RECONCILE_CYCLES = 60
DRY_RUN            = False
 
DB_PATH = "bot_state.db"
 
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
 
 
def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
 
    # Migrations: safely add columns to existing tables
    for table, col, definition in [
        ("positions",    "entry_price", "REAL NOT NULL DEFAULT 0"),
        ("positions",    "peak_price",  "REAL NOT NULL DEFAULT 0"),
        ("trade_log",    "outcome",     "TEXT NOT NULL DEFAULT ''"),
        ("target_stats", "wins",        "INTEGER NOT NULL DEFAULT 0"),
        ("target_stats", "losses",      "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")
            con.commit()
            print(f"[DB] Migrated: added {table}.{col}.")
        except Exception:
            pass  # already exists
 
    con.execute("""
        CREATE TABLE IF NOT EXISTS seen_trades (
            address  TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            ts       INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            PRIMARY KEY (address, trade_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            target_addr  TEXT NOT NULL,
            target_label TEXT NOT NULL,
            token_id     TEXT NOT NULL,
            market_q     TEXT NOT NULL,
            side         TEXT NOT NULL,
            our_size     REAL NOT NULL,
            price        REAL NOT NULL,
            executed     INTEGER NOT NULL,
            reason       TEXT,
            dry_run      INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            token_id    TEXT PRIMARY KEY,
            shares      REAL NOT NULL DEFAULT 0,
            entry_price REAL NOT NULL DEFAULT 0,
            peak_price  REAL NOT NULL DEFAULT 0,
            updated_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS target_stats (
            address       TEXT PRIMARY KEY,
            label         TEXT NOT NULL,
            total_copied  INTEGER NOT NULL DEFAULT 0,
            total_skipped INTEGER NOT NULL DEFAULT 0,
            last_seen     INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS notified_redeems (
            asset_id TEXT PRIMARY KEY,
            ts       INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS resolution_checks (
            trade_log_id INTEGER PRIMARY KEY,
            outcome      TEXT NOT NULL,
            won          INTEGER NOT NULL,
            ts           INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    con.commit()
    return con
 
db = init_db()
_db_lock = asyncio.Lock()
 
 
async def is_seen(address: str, trade_id: str) -> bool:
    async with _db_lock:
        return db.execute(
            "SELECT 1 FROM seen_trades WHERE address=? AND trade_id=?",
            (address, trade_id)
        ).fetchone() is not None
 
 
async def mark_seen(address: str, trade_id: str) -> None:
    async with _db_lock:
        db.execute("INSERT OR IGNORE INTO seen_trades VALUES (?,?,strftime('%s','now'))",
                   (address, trade_id))
        db.execute("""DELETE FROM seen_trades WHERE address=? AND trade_id NOT IN (
            SELECT trade_id FROM seen_trades WHERE address=? ORDER BY ts DESC LIMIT ?
        )""", (address, address, MAX_SEEN_IDS))
        db.commit()
 
 
async def log_trade(
    target_addr, target_label, token_id, market_q,
    side, our_size, price, executed, reason, outcome="",
) -> None:
    async with _db_lock:
        db.execute("""INSERT INTO trade_log
            (target_addr,target_label,token_id,market_q,side,our_size,price,executed,reason,dry_run,outcome)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (target_addr, target_label, token_id, market_q, side,
             our_size, price, int(executed), reason, int(DRY_RUN), outcome))
        if executed:
            db.execute("""INSERT INTO target_stats (address,label,total_copied,total_skipped,last_seen)
                VALUES (?,?,1,0,strftime('%s','now'))
                ON CONFLICT(address) DO UPDATE SET
                    total_copied=total_copied+1, label=excluded.label,
                    last_seen=strftime('%s','now')""", (target_addr, target_label))
        else:
            db.execute("""INSERT INTO target_stats (address,label,total_copied,total_skipped,last_seen)
                VALUES (?,?,0,1,strftime('%s','now'))
                ON CONFLICT(address) DO UPDATE SET
                    total_skipped=total_skipped+1, label=excluded.label,
                    last_seen=strftime('%s','now')""", (target_addr, target_label))
        db.commit()
 
 
async def record_position(token_id: str, shares_delta: float, price: float) -> None:
    async with _db_lock:
        row = db.execute(
            "SELECT shares, entry_price, peak_price FROM positions WHERE token_id=?",
            (token_id,)
        ).fetchone()
        current_shares = row[0] if row else 0.0
        current_entry  = row[1] if row else 0.0
        current_peak   = row[2] if row else 0.0
 
        new_shares = max(0.0, round(current_shares + shares_delta, 6))
        new_entry  = current_entry if current_entry > 0 else price
        new_peak   = max(current_peak, price)
 
        db.execute("""INSERT INTO positions (token_id, shares, entry_price, peak_price, updated_at)
            VALUES (?,?,?,?,strftime('%s','now'))
            ON CONFLICT(token_id) DO UPDATE SET
                shares=excluded.shares,
                entry_price=CASE WHEN entry_price > 0 THEN entry_price ELSE excluded.entry_price END,
                peak_price=MAX(peak_price, excluded.peak_price),
                updated_at=excluded.updated_at""",
            (token_id, new_shares, new_entry, new_peak))
        db.commit()
 
 
def get_all_positions() -> list[dict]:
    rows = db.execute(
        "SELECT token_id, shares, entry_price, peak_price FROM positions WHERE shares > 0.001"
    ).fetchall()
    return [{"token_id": r[0], "shares": r[1], "entry_price": r[2], "peak_price": r[3]} for r in rows]
 
 
def get_held_token_ids() -> set[str]:
    """Return set of all token_ids we currently hold a position in."""
    rows = db.execute(
        "SELECT token_id FROM positions WHERE shares > 0.001"
    ).fetchall()
    return {r[0] for r in rows}
 
 
async def update_peak_price(token_id: str, current_price: float) -> None:
    """Update peak_price if current price is a new high for this position."""
    async with _db_lock:
        db.execute("""
            UPDATE positions SET
                peak_price = MAX(peak_price, ?),
                updated_at = strftime('%s','now')
            WHERE token_id = ? AND shares > 0.001
        """, (current_price, token_id))
        db.commit()
 
 
def get_market_invested(token_ids: list[str]) -> float:
    """
    Return total USD currently invested across all tokens of a market.
    Uses shares * entry_price as a conservative estimate of cost basis.
    """
    if not token_ids:
        return 0.0
    placeholders = ",".join("?" * len(token_ids))
    rows = db.execute(
        f"SELECT shares, entry_price FROM positions WHERE token_id IN ({placeholders}) AND shares > 0.001",
        token_ids
    ).fetchall()
    return sum(r[0] * r[1] for r in rows)
 
 
def get_target_stats() -> list[dict]:
    rows = db.execute(
        "SELECT address, label, total_copied, total_skipped, wins, losses FROM target_stats"
    ).fetchall()
    return [{"address": r[0], "label": r[1], "copied": r[2], "skipped": r[3],
             "wins": r[4], "losses": r[5]} for r in rows]
 
 
# ── Live order book ────────────────────────────────────────────────────────────
 
_live_books: dict[str, dict] = {}
_subscribed_tokens: set[str] = set()
_ws_send_queue: asyncio.Queue | None = None
 
 
def _parse_book_event(msg: dict) -> None:
    token_id = msg.get("asset_id")
    if not token_id:
        return
    book = _live_books.setdefault(token_id, {"bids": {}, "asks": {}})
    ev = msg.get("event_type")
    if ev == "book":
        book["bids"] = {float(b["price"]): float(b["size"]) for b in msg.get("bids", [])}
        book["asks"] = {float(a["price"]): float(a["size"]) for a in msg.get("asks", [])}
    elif ev == "price_change":
        for c in msg.get("changes", []):
            p, s = float(c["price"]), float(c["size"])
            side_dict = book["bids"] if c.get("side", "").upper() == "BUY" else book["asks"]
            if s == 0:
                side_dict.pop(p, None)
            else:
                side_dict[p] = s
 
 
def get_live_best_ask(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return min(b["asks"]) if b and b["asks"] else None
 
def get_live_best_bid(token_id: str) -> float | None:
    b = _live_books.get(token_id)
    return max(b["bids"]) if b and b["bids"] else None
 
def get_live_spread(token_id: str) -> float | None:
    ask = get_live_best_ask(token_id)
    bid = get_live_best_bid(token_id)
    if ask is not None and bid is not None:
        return round(ask - bid, 6)
    return None
 
 
async def subscribe_token(token_id: str) -> None:
    if token_id in _subscribed_tokens:
        return
    _subscribed_tokens.add(token_id)
    if _ws_send_queue:
        await _ws_send_queue.put(json.dumps({"assets_ids": [token_id], "operation": "subscribe"}))
    print(f"[WS] Subscribed {token_id[:16]}…")
 
 
async def ws_book_listener() -> None:
    global _ws_send_queue
    _ws_send_queue = asyncio.Queue()
    print("[WS] Waiting for first token before connecting…")
    while not _subscribed_tokens:
        await asyncio.sleep(1)
 
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=None) as ws:
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
                    sender.cancel(); pinger.cancel()
        except Exception as exc:
            print(f"[WS] Disconnected: {exc} — reconnecting in 5s…")
            await asyncio.sleep(5)
 
 
async def _ws_sender(ws) -> None:
    while True:
        msg = await _ws_send_queue.get()
        await ws.send(msg)
 
async def _ws_pinger(ws) -> None:
    while True:
        await asyncio.sleep(WS_PING_INTERVAL)
        await ws.send("PING")
 
 
# ── Dynamic targets ────────────────────────────────────────────────────────────
 
_dynamic_targets: list[dict] = []
_dynamic_cycle_count: int = 0
 
 
async def fetch_leaderboard_targets(session: aiohttp.ClientSession) -> list[dict]:
    try:
        async with session.get(
            "https://data-api.polymarket.com/v1/leaderboard",
            params={"timePeriod": "month", "orderBy": "PNL", "category": "overall",
                    "limit": DYNAMIC_TARGET_COUNT * 3},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
 
        targets = []
        for entry in data:
            addr = entry.get("address") or entry.get("proxyWallet")
            if not addr:
                continue
            pnl        = float(entry.get("pnl", 0))
            
            if pnl <= 0:
                continue  # skip losing wallets
 
            vol = float(entry.get("vol", 0))
            if vol < MIN_TARGET_VOLUME:
                print(f"[Targets] Skipping {addr[:8]}… — vol ${vol:.0f} below min ${MIN_TARGET_VOLUME}")
                continue  # skip low-volume wallets (one-hit-wonders)
 
            targets.append({
                "address":  addr,
                "label":    entry.get("userName") or f"{addr[:8]}…",
                "copy_pct": COPY_PERCENT,
                "pnl":      pnl,
                "vol":      vol,
            })
            if len(targets) >= DYNAMIC_TARGET_COUNT:
                break
 
        if targets:
            print(f"[Targets] Loaded {len(targets)} from leaderboard.")
            return targets
 
    except Exception as exc:
        print(f"[Targets] Leaderboard fetch failed: {exc}")
 
    print("[Targets] Falling back to static TARGET_WALLETS.")
    return TARGET_WALLETS
 
 
def get_pinned_targets() -> list[dict]:
    """
    Return wallets we've copied that we still hold open positions from.
    Prevents rotation from dropping a wallet mid-position.
    """
    rows = db.execute("""
        SELECT DISTINCT tl.target_addr, tl.target_label
        FROM trade_log tl
        JOIN positions p ON tl.token_id = p.token_id
        WHERE tl.executed = 1 AND p.shares > 0.001
    """).fetchall()
    return [{"address": r[0], "label": r[1], "copy_pct": COPY_PERCENT, "pinned": True}
            for r in rows]


async def get_active_targets(session: aiohttp.ClientSession) -> list[dict]:
    global _dynamic_targets, _dynamic_cycle_count
    if not DYNAMIC_TARGETS:
        return TARGET_WALLETS
    _dynamic_cycle_count += 1
    if not _dynamic_targets or _dynamic_cycle_count % DYNAMIC_ROTATE_EVERY == 0:
        _dynamic_targets = await fetch_leaderboard_targets(session)

    # Re-add any wallets rotated out that still have open copied positions
    current_addrs = {t["address"] for t in _dynamic_targets}
    pinned = [t for t in get_pinned_targets() if t["address"] not in current_addrs]
    if pinned:
        for t in pinned:
            print(f"[Targets] Pinned {t['label']} ({t['address'][:8]}…) — open position held.")
    return _dynamic_targets + pinned
 
 
# ── Conviction filter ──────────────────────────────────────────────────────────
 
_trade_size_history: dict[str, list[float]] = defaultdict(list)
 
 
def record_trade_size(address: str, size_usd: float) -> None:
    history = _trade_size_history[address]
    history.append(size_usd)
    if len(history) > CONVICTION_LOOKBACK:
        history.pop(0)


def seed_conviction_history() -> None:
    """
    Pre-populate _trade_size_history from trade_log on startup.
    Without this, the first few trades after a restart set an arbitrary
    baseline and rejected trades never enter history (one-way ratchet).
    """
    rows = db.execute("""
        SELECT target_addr, our_size
        FROM trade_log
        WHERE our_size > 0
        ORDER BY ts DESC
        LIMIT ?
    """, (CONVICTION_LOOKBACK * 20,)).fetchall()

    # Replay oldest-first so history ends up in chronological order
    for addr, size in reversed(rows):
        history = _trade_size_history[addr]
        history.append(size)
        if len(history) > CONVICTION_LOOKBACK:
            history.pop(0)

    seeded = sum(len(v) for v in _trade_size_history.values())
    print(f"[Init] Conviction history seeded — {seeded} trade(s) across {len(_trade_size_history)} wallet(s).")
 
 
def is_high_conviction(address: str, size_usd: float) -> tuple[bool, str]:
    if CONVICTION_MULTIPLIER <= 0:
        return True, "conviction filter disabled"
    history = _trade_size_history[address]
    if len(history) < 3:
        return True, f"insufficient history ({len(history)} trades) — passing through"
    avg = mean(history)
    threshold = avg * CONVICTION_MULTIPLIER
    if size_usd >= threshold:
        return True, f"${size_usd:.2f} >= {CONVICTION_MULTIPLIER}x avg ${avg:.2f} high conviction"
    return False, f"${size_usd:.2f} < {CONVICTION_MULTIPLIER}x avg ${avg:.2f} — low conviction"
 
 
# ── Balance & sizing ───────────────────────────────────────────────────────────
 
_cached_balance: float = 0.0
_balance_fetched_at: float = 0.0
_cycle_exposure: dict[str, float] = {}
 
 
def reset_cycle_exposure() -> None:
    _cycle_exposure.clear()
 
def get_cycle_exposure(token_id: str) -> float:
    return _cycle_exposure.get(token_id, 0.0)
 
def add_cycle_exposure(token_id: str, usd: float) -> None:
    _cycle_exposure[token_id] = _cycle_exposure.get(token_id, 0.0) + usd
 
 
async def get_my_balance() -> float:
    global _cached_balance, _balance_fetched_at
    if DRY_RUN:
        return 20.0
    now = time.monotonic()
    if now - _balance_fetched_at < BALANCE_TTL:
        return _cached_balance
    try:
        _cached_balance = await run_blocking(client.get_usdc_balance)
        _balance_fetched_at = now
    except Exception as exc:
        print(f"[Balance] Fetch failed: {exc}")
    return _cached_balance
 
 
async def compute_trade_size(
    token_id: str, shares_traded: float, price: float, copy_pct: float,
    all_market_tokens: list[str] = None,
) -> tuple[float, str]:
    target_usd  = round(shares_traded * price, 2)
    my_balance  = await get_my_balance()
 
    if my_balance <= 0:
        return 0.0, "wallet balance $0 — check credentials"
 
    copied_size = round(target_usd * copy_pct, 2)
    wallet_cap  = round(my_balance * MAX_WALLET_PERCENT, 2)
 
    # Persistent market cap: sum of already-invested + current cycle exposure
    already_invested = get_market_invested(all_market_tokens or [token_id])
    cycle_exposure   = get_cycle_exposure(token_id)
    total_in_market  = already_invested + cycle_exposure
    market_cap       = round(my_balance * MAX_MARKET_PERCENT, 2)
    mkt_remain       = max(0.0, market_cap - total_in_market)
 
    final = round(min(copied_size, wallet_cap, mkt_remain), 2)
 
    caps = []
    if copied_size > wallet_cap:
        caps.append(f"wallet cap ${wallet_cap:.2f}")
    if copied_size > mkt_remain:
        caps.append(
            f"market cap ${market_cap:.2f} "
            f"(${already_invested:.2f} invested + ${cycle_exposure:.2f} this cycle)"
        )
 
    note = (
        f"Target {shares_traded}sh x ${price:.4f} = ${target_usd:.2f} "
        f"x {copy_pct*100:.0f}% = ${copied_size:.2f}"
        + (f" -> capped ({', '.join(caps)})" if caps else "")
        + f" = **${final:.2f}**"
    )
    return final, note
 
 
# ── REST helpers ───────────────────────────────────────────────────────────────
 
_market_cache: dict[str, dict] = {}
_market_cache_ttl: dict[str, float] = {}
MARKET_CACHE_TTL = 300
 
 
async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None) -> any:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
        r.raise_for_status()
        return await r.json()
 
 
async def get_activity(session: aiohttp.ClientSession, address: str, limit: int = 20) -> list[dict]:
    return await fetch_json(
        session, "https://data-api.polymarket.com/activity",
        {"limit": limit, "user": address, "sortBy": "TIMESTAMP", "sortDirection": "DESC"},
    )
 
 
async def get_market(session: aiohttp.ClientSession, token_id: str) -> dict:
    now = time.monotonic()
    if token_id in _market_cache and now - _market_cache_ttl.get(token_id, 0) < MARKET_CACHE_TTL:
        return _market_cache[token_id]
    data = await fetch_json(session, "https://gamma-api.polymarket.com/markets",
                            {"clob_token_ids": token_id})
    market = data[0] if data else {}
    if market:
        _market_cache[token_id] = market
        _market_cache_ttl[token_id] = now
    return market
 
 
async def get_my_positions(session: aiohttp.ClientSession) -> dict[str, float]:
    if not FUNDER_ADDRESS:
        print("[Positions] FUNDER_ADDRESS not set — skipping.")
        return {}
    try:
        data = await fetch_json(session, "https://data-api.polymarket.com/positions",
                                {"user": FUNDER_ADDRESS, "sizeThreshold": 0.01, "limit": 500})
        return {p["asset"]: float(p["size"]) for p in data if float(p.get("size", 0)) > 0}
    except Exception as exc:
        print(f"[Positions] Fetch failed: {exc}")
        return {}
 
 
def get_market_url(market: dict) -> str:
    events = market.get("events", [])
    if events:
        slug = events[0].get("slug", "")
        if slug:
            return f"https://polymarket.com/event/{slug}"
    slug = market.get("slug", "")
    if slug:
        return f"https://polymarket.com/event/{slug}"
    cid = market.get("conditionId", "")
    return f"https://polymarket.com/event/{cid}" if cid else "https://polymarket.com"
 
 
def _trade_fingerprint(act: dict) -> str:
    return f"{act.get('asset')}_{act.get('timestamp')}_{act.get('side')}_{act.get('size')}"
 
 
def _rest_best_ask(token_id: str) -> float | None:
    try:
        book = client.get_order_book(token_id)
        return float(min(a.price for a in book.asks)) if book.asks else None
    except Exception:
        return None
 
def _rest_best_bid(token_id: str) -> float | None:
    try:
        book = client.get_order_book(token_id)
        return float(max(b.price for b in book.bids)) if book.bids else None
    except Exception:
        return None
 
 
def _safe_shares(usd: float, price: float) -> float:
    """Returns whole-number shares, minimum order value $1."""
    shares = max(1, int(usd / price))
    while shares * price < 1.0:
        shares += 1
    return float(shares)
 
 
# ── Discord ────────────────────────────────────────────────────────────────────
 
async def send_discord(
    session: aiohttp.ClientSession, message: str,
    color: int = 0x00b0f4, title: str = "Polymarket Copy Trade",
) -> None:
    if not DISCORD_WEBHOOK:
        return
    payload = {"embeds": [{"title": title, "description": message, "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "footer": {"text": f"{'DRY RUN' if DRY_RUN else 'LIVE'}  |  {len(TARGET_WALLETS)} target(s)"}}]}
    try:
        async with session.post(DISCORD_WEBHOOK, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
    except Exception as exc:
        print(f"[Discord] Send failed: {exc}")
 
 
async def discord_trade_alert(
    session, act, label, address, question, market_url,
    our_size, sizing_note, executed, conviction_note="", reason="",
) -> None:
    side    = act.get("side", "?").upper()
    price   = float(act.get("price", 0))
    outcome = act.get("outcome", "?")
    asset   = act.get("asset", "?")
    color   = 0x57F287 if side == "BUY" else 0xED4245
 
    spread = get_live_spread(asset)
    spread_str = f"{spread:.4f}" if spread is not None else "unknown"
 
    if DRY_RUN:
        status = f"Dry run — would place ${our_size:.2f} FAK order"
    elif executed:
        status = f"Copied — placed ${our_size:.2f} FAK order"
    else:
        status = f"Skipped — {reason}"
 
    msg = (
        f"**Trader:** {label} (`{address[:8]}...{address[-6:]}`)\n"
        f"**Market:** [{question}]({market_url})\n"
        f"**Side:** {side}  |  **Outcome:** {outcome}\n"
        f"**Price:** {price:.4f}  |  **Spread:** {spread_str}\n"
        f"{sizing_note}\n"
        + (f"{conviction_note}\n" if conviction_note else "")
        + f"\n{status}"
    )
    await send_discord(session, msg, color=color,
                       title=f"{'BUY' if side == 'BUY' else 'SELL'} — {label}")
 
 
async def discord_error(session, context: str, exc: Exception) -> None:
    await send_discord(session, f"**Context:** {context}\n```{exc}```",
                       color=0xFEE75C, title="Error")
 
 
async def discord_stop_loss(session, token_id, question, shares, entry, current, pnl_pct) -> None:
    msg = (
        f"**Token:** `{token_id[:16]}...`\n"
        f"**Market:** {question}\n"
        f"**Shares:** {shares:.4f}  |  **Entry:** {entry:.4f}  |  **Now:** {current:.4f}\n"
        f"**PNL:** {pnl_pct*100:.1f}%  — stop-loss at -{STOP_LOSS_PCT*100:.0f}%"
    )
    await send_discord(session, msg, color=0xFF6B35, title="Stop-Loss Triggered")
 
 
async def discord_stats(session, targets) -> None:
    stats = get_target_stats()
    if not stats:
        return
    lines = ["**Per-target performance:**\n"]
    for s in stats:
        total    = s["copied"] + s["skipped"]
        copy_hit = f"{s['copied']/total*100:.0f}%" if total else "—"
        resolved = s["wins"] + s["losses"]
        win_rate = f"{s['wins']/resolved*100:.0f}% W/R ({s['wins']}W/{s['losses']}L)" if resolved else "no resolved trades yet"
        lines.append(f"• **{s['label']}** — {s['copied']} copied ({copy_hit}) | {win_rate}")
    await send_discord(session, "\n".join(lines), color=0x5865F2, title="Stats")
 
 
# ── Auto-redeem ───────────────────────────────────────────────────────────────
#
# Polymarket positions are held in a Safe (proxy multisig). The official SDK
# has no redeem endpoint (issue #295). We call redeemPositions on the CTF
# contract via web3.py through the Safe's execTransaction.
#
# Requirements:  pip install web3
# You also need a small amount of POL (MATIC) in your wallet for gas.
#
POLYGON_RPC          = "https://polygon-rpc.com"
CTF_ADDRESS          = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS         = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
SAFE_ADDRESS         = FUNDER_ADDRESS   # your proxy wallet = the Safe
HASH_ZERO            = "0x" + "00" * 32
 
REDEEM_INTERVAL = 120   # check for redeemable positions every 2 minutes
 
# ABI fragments — only what we need
CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    }
]
 
SAFE_ABI = [
    {
        "name": "execTransaction",
        "type": "function",
        "inputs": [
            {"name": "to",            "type": "address"},
            {"name": "value",         "type": "uint256"},
            {"name": "data",          "type": "bytes"},
            {"name": "operation",     "type": "uint8"},
            {"name": "safeTxGas",     "type": "uint256"},
            {"name": "baseGas",       "type": "uint256"},
            {"name": "gasPrice",      "type": "uint256"},
            {"name": "gasToken",      "type": "address"},
            {"name": "refundReceiver","type": "address"},
            {"name": "signatures",    "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "nonpayable",
    },
    {
        "name": "getTransactionHash",
        "type": "function",
        "inputs": [
            {"name": "to",            "type": "address"},
            {"name": "value",         "type": "uint256"},
            {"name": "data",          "type": "bytes"},
            {"name": "operation",     "type": "uint8"},
            {"name": "safeTxGas",     "type": "uint256"},
            {"name": "baseGas",       "type": "uint256"},
            {"name": "gasPrice",      "type": "uint256"},
            {"name": "gasToken",      "type": "address"},
            {"name": "refundReceiver","type": "address"},
            {"name": "nonce",         "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
    },
    {
        "name": "nonce",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]
 
 
def _build_web3():
    """Build a web3 instance. Returns None if web3 not installed."""
    try:
        from web3 import Web3
        from eth_account import Account
        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
        return w3, Account.from_key(PRIVATE_KEY)
    except ImportError:
        print("[Redeem] web3 not installed — run: pip install web3")
        return None, None
 
 
def _redeem_position_sync(condition_id: str) -> bool:
    """
    Calls redeemPositions on the CTF contract through the Safe.
    Runs synchronously — called via run_blocking().
    Returns True on success.
    """
    from web3 import Web3
 
    w3, account = _build_web3()
    if not w3 or not account:
        return False
 
    try:
        ctf  = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
        safe = w3.eth.contract(address=Web3.to_checksum_address(SAFE_ADDRESS), abi=SAFE_ABI)
 
        # Encode the redeemPositions call
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        data = ctf.encodeABI(
            fn_name="redeemPositions",
            args=[
                Web3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(HASH_ZERO.replace("0x", "")),
                cid_bytes,
                [1, 2],  # index sets for YES and NO
            ]
        )
 
        nonce = safe.functions.nonce().call()
 
        # Build Safe transaction hash
        tx_hash = safe.functions.getTransactionHash(
            Web3.to_checksum_address(CTF_ADDRESS),  # to
            0,                                       # value
            data,                                    # data
            0,                                       # operation (CALL)
            0, 0, 0,                                 # safeTxGas, baseGas, gasPrice
            "0x0000000000000000000000000000000000000000",  # gasToken
            "0x0000000000000000000000000000000000000000",  # refundReceiver
            nonce,
        ).call()
 
        # Sign the Safe tx hash
        signed = account.signHash(tx_hash)
        # Pack r, s, v into 65-byte signature
        sig = (
            signed.r.to_bytes(32, "big") +
            signed.s.to_bytes(32, "big") +
            bytes([signed.v])
        )
 
        # Submit via execTransaction
        tx = safe.functions.execTransaction(
            Web3.to_checksum_address(CTF_ADDRESS),
            0,
            data,
            0,
            0, 0, 0,
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            sig,
        ).build_transaction({
            "from":     account.address,
            "nonce":    w3.eth.get_transaction_count(account.address),
            "gas":      300_000,
            "gasPrice": w3.eth.gas_price,
        })
 
        signed_tx = account.sign_transaction(tx)
        tx_hash_sent = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash_sent, timeout=60)
        return receipt.status == 1
 
    except Exception as exc:
        print(f"[Redeem] on-chain error: {exc}")
        return False
 
 
async def auto_redeem_monitor(session: aiohttp.ClientSession) -> None:
    """
    Checks every REDEEM_INTERVAL seconds for resolved positions with value > 0.
    Sends a Discord alert so you can redeem manually at polymarket.com.
 
    Note: Programmatic redemption is not possible for email/Magic proxy wallets
    without the Polymarket Builder Relayer program. This monitor alerts you so
    you never miss a winning position.
    """
    await asyncio.sleep(30)
 
    while True:
        try:
            if not FUNDER_ADDRESS:
                await asyncio.sleep(REDEEM_INTERVAL)
                continue
 
            # Load persisted notified set from DB (survives restarts)
            async with _db_lock:
                already_notified = {
                    r[0] for r in db.execute("SELECT asset_id FROM notified_redeems").fetchall()
                }
 
            async with session.get(
                "https://data-api.polymarket.com/positions",
                params={"user": FUNDER_ADDRESS, "sizeThreshold": 0.01, "limit": 500},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                positions = await resp.json()
 
            redeemable = [
                p for p in positions
                if p.get("redeemable", False)
                and float(p.get("currentValue", 0)) > 0
                and p.get("asset", "") not in already_notified
            ]
 
            if redeemable:
                total = sum(float(p.get("currentValue", 0)) for p in redeemable)
                lines = "\n".join(
                    f"• {p.get('title', p.get('asset','?'))[:50]} — **${float(p.get('currentValue', 0)):.2f}**"
                    for p in redeemable
                )
                print(f"[Redeem] {len(redeemable)} position(s) ready — ${total:.2f} total")
                await send_discord(session,
                    f"**{len(redeemable)} position(s) ready to redeem** — total **${total:.2f}**\n\n"
                    f"{lines}\n\n"
                    f"➡️ Go to **polymarket.com/portfolio** to redeem.",
                    color=0x57F287,
                    title="💰 Positions Ready to Redeem"
                )
                # Persist to DB so restarts don't re-alert
                async with _db_lock:
                    for p in redeemable:
                        db.execute(
                            "INSERT OR IGNORE INTO notified_redeems (asset_id) VALUES (?)",
                            (p.get("asset", ""),)
                        )
                    db.commit()
 
        except Exception as exc:
            print(f"[Redeem] Monitor error: {exc}")
 
        await asyncio.sleep(REDEEM_INTERVAL)
 
 
# ── Stop-loss monitor ──────────────────────────────────────────────────────────
 
async def stop_loss_monitor(session: aiohttp.ClientSession) -> None:
    """
    Trailing stop: tracks peak price and triggers when price drops
    STOP_LOSS_PCT% from the peak — protects gains, not just entry.
    """
    while True:
        await asyncio.sleep(STOP_LOSS_INTERVAL)
        positions = get_all_positions()
        if not positions:
            continue
 
        for pos in positions:
            token_id = pos["token_id"]
            shares   = pos["shares"]
            entry    = pos["entry_price"]
            peak     = pos.get("peak_price", entry)
            if entry <= 0:
                continue
 
            current = get_live_best_bid(token_id)
            if current is None:
                current = await run_blocking(_rest_best_bid, token_id)
            if current is None:
                continue
 
            # Update peak if new high
            if current > peak:
                await update_peak_price(token_id, current)
                peak = current
 
            # Trailing: measure drop from peak (or entry if peak not set)
            reference = peak if peak > 0 else entry
            drop_pct  = (reference - current) / reference
 
            if drop_pct <= STOP_LOSS_PCT:
                continue
 
            market   = _market_cache.get(token_id, {})
            question = market.get("question", token_id[:20])
            gain_pct = (peak - entry) / entry * 100 if entry > 0 else 0
 
            print(f"[TrailingStop] {question[:40]} — entry ${entry:.2f} peak ${peak:.2f} now ${current:.2f} drop {drop_pct*100:.1f}%")
            await send_discord(session,
                f"**Market:** {question}\n"
                f"**Shares:** {shares:.0f} | **Entry:** ${entry:.4f} | **Peak:** ${peak:.4f} | **Now:** ${current:.4f}\n"
                f"**Peak gain:** +{gain_pct:.1f}% | **Drop from peak:** -{drop_pct*100:.1f}%\n"
                f"Trailing stop triggered at -{STOP_LOSS_PCT*100:.0f}% from peak",
                color=0xFF6B35, title="🔻 Trailing Stop Triggered"
            )
 
            if not DRY_RUN:
                try:
                    # Fetch live share count from Polymarket API to avoid
                    # selling more than we actually hold (DB can drift)
                    live_positions = await get_my_positions(session)
                    live_shares = live_positions.get(token_id, 0.0)
 
                    if live_shares <= 0:
                        print(f"  [TrailingStop] No live position found — clearing DB entry")
                        await record_position(token_id, -shares, current)
                        continue
 
                    # Use live share count, not DB count
                    # For sells: use exact share count (no minimum).
                    # int() truncates to whole shares to avoid decimal errors.
                    # Do NOT enforce max(1,...) — we might hold less than 1 share.
                    sell_size = float(int(live_shares))
                    if sell_size <= 0:
                        # Fractional share under 1 — sell the fraction as-is
                        sell_size = round(live_shares, 4)
                    order = OrderArgs(
                        token_id=token_id,
                        price=round(current, 2),
                        size=sell_size,
                        side="SELL"
                    )
                    signed = await run_blocking(client.create_order, order)
                    await run_blocking(client.post_order, signed, OrderType.FAK)
                    await record_position(token_id, -live_shares, current)
                except Exception as exc:
                    await discord_error(session, f"trailing stop SELL {token_id}", exc)
            else:
                print(f"  [DRY RUN] Would SELL {shares} shares @ {current:.4f}")
                await record_position(token_id, -shares, current)
 
 
# ── Trade execution ────────────────────────────────────────────────────────────
 
async def execute_trade(
    session: aiohttp.ClientSession, token_id: str, side: str,
    our_size: float, reference_price: float, positions: dict[str, float],
) -> tuple[bool, str]:
    try:
        if side == "BUY":
            price = get_live_best_ask(token_id) or await run_blocking(_rest_best_ask, token_id)
            if price is None:
                return False, "no ask in order book"
            slip = abs(price - reference_price) / reference_price
            if slip > SLIPPAGE_TOLERANCE:
                return False, f"slippage {slip*100:.1f}% > {SLIPPAGE_TOLERANCE*100:.0f}%"
            price  = round(price, 2)                 # max 2dp on price
            shares = _safe_shares(our_size, price)   # truncated 4dp, no float precision errors
            if shares <= 0:
                return False, "computed share size is zero after rounding"
            if not DRY_RUN:
                print(str(shares) + " shares and cost " + str(price))
                signed = await run_blocking(client.create_order,
                    OrderArgs(token_id=token_id, price=price, size=shares, side="BUY"))
                await run_blocking(client.post_order, signed, OrderType.FAK)
            await record_position(token_id, +shares, price)
 
        elif side == "SELL":
            # Check both live API positions and local DB
            held = positions.get(token_id, 0.0)
            if held <= 0:
                held = db.execute(
                    "SELECT shares FROM positions WHERE token_id=? AND shares > 0.001",
                    (token_id,)
                ).fetchone()
                held = held[0] if held else 0.0
            if held <= 0:
                return False, "no position held — cannot sell"
            price = get_live_best_bid(token_id) or await run_blocking(_rest_best_bid, token_id)
            if price is None:
                return False, "no bid in order book"
            slip = abs(price - reference_price) / reference_price
            if slip > SLIPPAGE_TOLERANCE:
                return False, f"slippage {slip*100:.1f}% > {SLIPPAGE_TOLERANCE*100:.0f}%"
            price  = round(price, 2)                              # max 2dp on price
            shares = min(_safe_shares(our_size, price), held)   # truncated 4dp, capped at held
            if shares <= 0:
                return False, "computed share size is zero after rounding"
            if not DRY_RUN:
                signed = await run_blocking(client.create_order,
                    OrderArgs(token_id=token_id, price=price, size=shares, side="SELL"))
                await run_blocking(client.post_order, signed, OrderType.FAK)
            await record_position(token_id, -shares, price)
 
        else:
            return False, f"unknown side '{side}'"
 
        return True, ""
 
    except Exception as exc:
        return False, str(exc)
 
 
# ── Per-target scan ────────────────────────────────────────────────────────────
 
async def scan_target(
    session: aiohttp.ClientSession, target: dict, positions: dict[str, float],
) -> None:
    address  = target["address"]
    label    = target["label"]
    copy_pct = target.get("copy_pct", COPY_PERCENT)
 
    try:
        activities = await get_activity(session, address, limit=20)
    except Exception as exc:
        print(f"  [{label}] Activity fetch failed: {exc}")
        await discord_error(session, f"get_activity() for {label}", exc)
        return
 
    new_trades = []
    for a in activities:
        if a.get("type") != "TRADE":
            continue
        if not await is_seen(address, a.get("id") or _trade_fingerprint(a)):
            new_trades.append(a)
 
    if not new_trades:
        return
 
    print(f"  [{label}] {len(new_trades)} new trade(s).")
 
    for act in new_trades:
        trade_id      = act.get("id") or _trade_fingerprint(act)
        await mark_seen(address, trade_id)
 
        token_id      = act.get("asset")
        side          = act.get("side", "").upper()
        trade_price   = float(act.get("price", 0))
        shares_traded = float(act.get("size", 0))
        trade_usd     = round(shares_traded * trade_price, 2)
        outcome       = act.get("outcome", "")
 
        try:
            market = await get_market(session, token_id)
        except Exception as exc:
            market = {}
            await discord_error(session, f"get_market({token_id})", exc)
 
        if market.get("closed", False) or not market.get("acceptingOrders", True):
            print(f"    [skip] market closed")
            continue
 
        question   = market.get("question", token_id)
        market_url = get_market_url(market)
 
        if trade_price <= 0:
            print(f"    [skip] price zero")
            continue

        # ── Price lag filter ───────────────────────────────────────────────
        # Skip BUY if price has moved too far since the whale entered.
        if side == "BUY" and PRICE_LAG_MAX > 0:
            current_ask = get_live_best_ask(token_id) or await run_blocking(_rest_best_ask, token_id)
            if current_ask is not None:
                lag_pct = (current_ask - trade_price) / trade_price
                if lag_pct > PRICE_LAG_MAX:
                    reason = f"price lag {lag_pct*100:.1f}% (whale ${trade_price:.3f} → now ${current_ask:.3f})"
                    print(f"    [skip] {reason}")
                    await log_trade(address, label, token_id, question, side, 0, trade_price, False, reason, outcome)
                    await discord_trade_alert(session, act, label, address, question, market_url,
                                              0, "—", False, reason=reason)
                    continue

        # ── Opposite-side guard ────────────────────────────────────────────
        # Check if we already hold the opposite outcome of this market.
        # YES and NO share the same conditionId. If we hold YES and the target
        # is now buying NO (or vice versa), skip — we'd be hedging ourselves.
        try:
            token_ids_raw = market.get("clobTokenIds", "[]")
            import json as _json
            all_tokens = _json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            held_tokens = get_held_token_ids()
            opposite_tokens = [t for t in all_tokens if t != token_id]
            if any(t in held_tokens for t in opposite_tokens):
                reason = f"already hold opposite side of this market"
                print(f"    [skip] {reason}")
                await log_trade(address, label, token_id, question, side, 0, trade_price, False, reason)
                await discord_trade_alert(session, act, label, address, question, market_url,
                                          0, "—", False, reason=reason)
                continue
        except Exception:
            pass  # if we can't parse tokens, proceed normally
 
        # Spread filter — only apply when WS book is live (spread > 0 means real data)
        spread = get_live_spread(token_id)
        if spread is not None and spread > 0 and spread > MAX_SPREAD:
            reason = f"spread {spread:.4f} > max {MAX_SPREAD:.4f}"
            print(f"    [skip] {reason}")
            await log_trade(address, label, token_id, question, side, 0, trade_price, False, reason)
            await discord_trade_alert(session, act, label, address, question, market_url,
                                      0, "—", False, reason=reason)
            continue
 
        # Conviction filter — record ALL sizes (pass or fail) so the baseline
        # reflects the whale's true behaviour and doesn't ratchet upward.
        record_trade_size(address, trade_usd)
        high_conviction, conviction_note = is_high_conviction(address, trade_usd)
        if not high_conviction:
            print(f"    [skip] {conviction_note}")
            await log_trade(address, label, token_id, question, side, 0, trade_price, False, conviction_note)
            await discord_trade_alert(session, act, label, address, question, market_url,
                                      0, "—", False, conviction_note=conviction_note,
                                      reason=conviction_note)
            continue
 
        # Pass all token IDs for this market so cap accounts for existing positions
        try:
            _tids_raw = market.get("clobTokenIds", "[]")
            import json as _j
            _all_tids = _j.loads(_tids_raw) if isinstance(_tids_raw, str) else _tids_raw
        except Exception:
            _all_tids = [token_id]
        our_size, sizing_note = await compute_trade_size(token_id, shares_traded, trade_price, copy_pct, _all_tids)
 
        if our_size <= 0 and "balance" in sizing_note:
            await discord_error(session, "Zero balance", Exception(sizing_note))
            continue
 
        if our_size < MIN_TRADE_SIZE:
            reason = f"${our_size:.2f} below minimum ${MIN_TRADE_SIZE:.2f}"
            print(f"    [skip] {reason}")
            await log_trade(address, label, token_id, question, side, our_size, trade_price, False, reason, outcome)
            await discord_trade_alert(session, act, label, address, question, market_url,
                                      our_size, sizing_note, False, conviction_note, reason)
            continue
 
        # Subscribe to live order book only now — after all filters passed
        await subscribe_token(token_id)
 
        if DRY_RUN:
            executed, reason = True, ""
            book_src = "WS" if get_live_best_ask(token_id) else "pending"
            print(f"    [~] {side} ${our_size:.2f}  '{question[:45]}'  [{book_src}]  {conviction_note}")
        else:
            executed, reason = await execute_trade(
                session, token_id, side, our_size, trade_price, positions)
            if executed:
                add_cycle_exposure(token_id, our_size)
 
        await log_trade(address, label, token_id, question, side, our_size, trade_price, executed, reason, outcome)
        await discord_trade_alert(session, act, label, address, question, market_url,
                                  our_size, sizing_note, executed, conviction_note, reason)
 
        mark = "~" if DRY_RUN else ("✓" if executed else "✗")
        print(f"    [{mark}] {side} ${our_size:.2f}  '{question[:45]}'"
              + (f"  — {reason}" if reason else ""))
 
 
# ── Win rate monitor ──────────────────────────────────────────────────────────

async def win_rate_monitor(session: aiohttp.ClientSession) -> None:
    """
    Periodically checks executed BUY trades against resolved markets and
    updates wins/losses per target in target_stats.
    Uses resolution_checks to avoid re-processing the same trade twice.
    """
    await asyncio.sleep(60)  # let the bot settle on startup

    while True:
        try:
            # Fetch executed BUY trades not yet resolution-checked
            rows = db.execute("""
                SELECT tl.id, tl.target_addr, tl.token_id, tl.outcome
                FROM trade_log tl
                LEFT JOIN resolution_checks rc ON rc.trade_log_id = tl.id
                WHERE tl.executed = 1 AND tl.side = 'BUY'
                  AND rc.trade_log_id IS NULL
            """).fetchall()

            for row_id, target_addr, token_id, stored_outcome in rows:
                try:
                    market = await get_market(session, token_id)
                    if not market:
                        continue

                    # Parse outcomePrices — ["1","0"] = YES won, ["0","1"] = NO won
                    raw = market.get("outcomePrices", "[]")
                    prices = json.loads(raw) if isinstance(raw, str) else raw
                    if len(prices) < 2:
                        continue
                    yes_price = float(prices[0])
                    no_price  = float(prices[1])

                    # Only process fully resolved markets
                    if yes_price not in (0.0, 1.0) or no_price not in (0.0, 1.0):
                        continue

                    winning_outcome = "Yes" if yes_price == 1.0 else "No"
                    won = stored_outcome.lower() == winning_outcome.lower() if stored_outcome else None
                    if won is None:
                        continue

                    async with _db_lock:
                        db.execute(
                            "INSERT OR IGNORE INTO resolution_checks (trade_log_id, outcome, won) VALUES (?,?,?)",
                            (row_id, winning_outcome, int(won))
                        )
                        if won:
                            db.execute(
                                "UPDATE target_stats SET wins=wins+1 WHERE address=?",
                                (target_addr,)
                            )
                        else:
                            db.execute(
                                "UPDATE target_stats SET losses=losses+1 WHERE address=?",
                                (target_addr,)
                            )
                        db.commit()

                    result = "WIN" if won else "LOSS"
                    print(f"[WinRate] trade {row_id} resolved → {result} ({target_addr[:8]}…)")

                except Exception as exc:
                    print(f"[WinRate] Error checking trade {row_id}: {exc}")

        except Exception as exc:
            print(f"[WinRate] Monitor error: {exc}")

        await asyncio.sleep(WIN_RATE_CHECK_INTERVAL)


# ── Seed + main ────────────────────────────────────────────────────────────────
 
 
 
 
 
async def reconcile_positions(session: aiohttp.ClientSession) -> None:
    """
    Sync the local positions SQLite table against the live Polymarket API.
    Replaces whatever is in the DB with what Polymarket actually says you hold.
    Prevents ghost stop-losses and missed sells caused by DB drift from
    manual trades, partial fills, or bot restarts.
    """
    if not FUNDER_ADDRESS:
        return
    try:
        live = await get_my_positions(session)
        if not live and not DRY_RUN:
            return  # empty response — don't wipe local state on API error
 
        async with _db_lock:
            # Get all token_ids currently tracked locally
            local_rows = db.execute(
                "SELECT token_id, shares, entry_price, peak_price FROM positions WHERE shares > 0.001"
            ).fetchall()
            local = {r[0]: {"shares": r[1], "entry": r[2], "peak": r[3]} for r in local_rows}
 
            synced = 0
            removed = 0
 
            # Update or insert live positions
            for token_id, live_shares in live.items():
                loc = local.get(token_id)
                entry = loc["entry"] if loc and loc["entry"] > 0 else 0.0
                peak  = loc["peak"]  if loc else 0.0
                db.execute("""INSERT INTO positions (token_id, shares, entry_price, peak_price, updated_at)
                    VALUES (?,?,?,?,strftime('%s','now'))
                    ON CONFLICT(token_id) DO UPDATE SET
                        shares=excluded.shares,
                        updated_at=excluded.updated_at""",
                    (token_id, live_shares, entry, peak))
                synced += 1
 
            # Zero out positions we hold locally but Polymarket says we don't
            for token_id in local:
                if token_id not in live:
                    db.execute(
                        "UPDATE positions SET shares=0, updated_at=strftime('%s','now') WHERE token_id=?",
                        (token_id,)
                    )
                    removed += 1
 
            db.commit()
 
        if synced or removed:
            print(f"[Reconcile] Synced {synced} position(s), removed {removed} stale position(s).")
 
    except Exception as exc:
        print(f"[Reconcile] Failed: {exc}")
 
def get_poll_interval() -> int:
    """
    Return a shorter poll interval during US sports prime time
    (6pm-midnight ET = 22:00-05:00 UTC) and longer overnight.
    Most NBA/NHL games tip off between 7-10pm ET.
    """
    utc_hour = datetime.utcnow().hour
    # Active window: 22:00 UTC to 05:00 UTC (6pm-midnight ET)
    if utc_hour >= 22 or utc_hour < 5:
        return POLL_INTERVAL_ACTIVE
    return POLL_INTERVAL_QUIET
 
async def seed_seen_trades(session: aiohttp.ClientSession, targets: list[dict]) -> None:
    for t in targets:
        try:
            acts = await get_activity(session, t["address"], limit=50)
            count = 0
            for a in acts:
                tid = a.get("id") or _trade_fingerprint(a)
                if not await is_seen(t["address"], tid):
                    await mark_seen(t["address"], tid)
                    count += 1
            print(f"[Init] {t['label']} — seeded {count} trade(s).")
        except Exception as exc:
            print(f"[Init] Could not seed {t['label']}: {exc}")
 
 
async def main() -> None:
    async with aiohttp.ClientSession() as session:
        print("=" * 62)
        print("  Polymarket Copy-Trade Bot")
        print(f"  Mode          : {'DRY RUN ($20 simulated)' if DRY_RUN else 'LIVE'}")
        print(f"  Targets       : {'dynamic (leaderboard)' if DYNAMIC_TARGETS else 'static'}")
        print(f"  Conviction    : >= {CONVICTION_MULTIPLIER}x rolling avg ({CONVICTION_LOOKBACK} window)")
        print(f"  Spread filter : <= {MAX_SPREAD:.2f}")
        print(f"  Stop-loss     : -{STOP_LOSS_PCT*100:.0f}%")
        print(f"  Wallet cap    : {MAX_WALLET_PERCENT*100:.0f}%  |  Market cap: {MAX_MARKET_PERCENT*100:.0f}%")
        print(f"  Slippage      : {SLIPPAGE_TOLERANCE*100:.0f}%  |  Min size: ${MIN_TRADE_SIZE:.2f}")
        print(f"  Order type    : FAK  |  Poll: {POLL_INTERVAL}s")
        print("=" * 62)
 
        seed_conviction_history()           # restore trade size baselines from DB
        targets = await get_active_targets(session)
        await seed_seen_trades(session, targets)
        await reconcile_positions(session)  # sync positions DB on startup
 
        target_lines = "\n".join(
            f"• **{t['label']}** `{t['address'][:8]}...`  ({t.get('copy_pct', COPY_PERCENT)*100:.0f}%)"
            for t in targets
        )
        await send_discord(session,
            f"**{'Dry run' if DRY_RUN else 'LIVE'}** — {len(targets)} target(s):\n{target_lines}\n\n"
            f"Conviction **>={CONVICTION_MULTIPLIER}x** | "
            f"Spread **<={MAX_SPREAD:.2f}** | "
            f"Stop-loss **-{STOP_LOSS_PCT*100:.0f}%** | FAK orders",
            color=0x5865F2 if DRY_RUN else 0xED4245,
            title="Dry Run Started" if DRY_RUN else "Live Bot Started",
        )
 
        ws_task        = asyncio.create_task(ws_book_listener())
        stop_loss_task = asyncio.create_task(stop_loss_monitor(session))
        redeem_task    = asyncio.create_task(auto_redeem_monitor(session))
        win_rate_task  = asyncio.create_task(win_rate_monitor(session))
        cycle          = 0
 
        while True:
            cycle += 1
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [{'DRY RUN' if DRY_RUN else 'LIVE'}] cycle {cycle}")
 
            reset_cycle_exposure()
            targets   = await get_active_targets(session)
            positions = await get_my_positions(session)
 
            results = await asyncio.gather(
                *[scan_target(session, t, positions) for t in targets],
                return_exceptions=True,
            )
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    lbl = targets[i]["label"] if i < len(targets) else f"target[{i}]"
                    print(f"  [{lbl}] CRASH: {result}")
                    await discord_error(session, f"scan_target {lbl}", result)
 
            if cycle % POSITIONS_RECONCILE_CYCLES == 0:
                await reconcile_positions(session)
 
            if cycle % 30 == 0:
                await discord_stats(session, targets)
 
            if ws_task.done():
                ws_task = asyncio.create_task(ws_book_listener())
            if stop_loss_task.done():
                stop_loss_task = asyncio.create_task(stop_loss_monitor(session))
            if redeem_task.done():
                redeem_task = asyncio.create_task(auto_redeem_monitor(session))
            if win_rate_task.done():
                win_rate_task = asyncio.create_task(win_rate_monitor(session))
 
            # Smart poll: faster during US sports prime time, slower overnight
            interval = get_poll_interval()
            await asyncio.sleep(interval)
 
 
if __name__ == "__main__":
    asyncio.run(main())