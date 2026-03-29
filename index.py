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
        "address":  "0xd8f8c13644ea84d62e1ec88c5d1215e436eb0f11",
        "label":    "Whale A",
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
MAX_WALLET_PERCENT = 0.30
MAX_MARKET_PERCENT = 0.15
MIN_TRADE_SIZE     = 0.20
SLIPPAGE_TOLERANCE = 0.05
MAX_SPREAD         = 0.12
STOP_LOSS_PCT      = 0.25

CONVICTION_MULTIPLIER = 0
CONVICTION_LOOKBACK   = 20

DYNAMIC_TARGETS      = True
DYNAMIC_TARGET_COUNT = 10
DYNAMIC_ROTATE_EVERY = 30

POLL_INTERVAL      = 10
MAX_SEEN_IDS       = 1000
BALANCE_TTL        = 30
WS_PING_INTERVAL   = 10
STOP_LOSS_INTERVAL = 60
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

    # Migration: safely add entry_price if missing from an older DB
    try:
        con.execute("ALTER TABLE positions ADD COLUMN entry_price REAL NOT NULL DEFAULT 0")
        con.commit()
        print("[DB] Migrated: added entry_price column.")
    except Exception:
        pass  # column already exists

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
    side, our_size, price, executed, reason,
) -> None:
    async with _db_lock:
        db.execute("""INSERT INTO trade_log
            (target_addr,target_label,token_id,market_q,side,our_size,price,executed,reason,dry_run)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (target_addr, target_label, token_id, market_q, side,
             our_size, price, int(executed), reason, int(DRY_RUN)))
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
        row = db.execute("SELECT shares FROM positions WHERE token_id=?", (token_id,)).fetchone()
        current = row[0] if row else 0.0
        new_shares = max(0.0, round(current + shares_delta, 6))
        db.execute("""INSERT INTO positions (token_id, shares, entry_price, updated_at)
            VALUES (?,?,?,strftime('%s','now'))
            ON CONFLICT(token_id) DO UPDATE SET
                shares=excluded.shares,
                entry_price=CASE WHEN excluded.shares>0 THEN excluded.entry_price ELSE entry_price END,
                updated_at=excluded.updated_at""",
            (token_id, new_shares, price))
        db.commit()


def get_all_positions() -> list[dict]:
    rows = db.execute(
        "SELECT token_id, shares, entry_price FROM positions WHERE shares > 0.001"
    ).fetchall()
    return [{"token_id": r[0], "shares": r[1], "entry_price": r[2]} for r in rows]


def get_held_token_ids() -> set[str]:
    """Return set of all token_ids we currently hold a position in."""
    rows = db.execute(
        "SELECT token_id FROM positions WHERE shares > 0.001"
    ).fetchall()
    return {r[0] for r in rows}


def get_target_stats() -> list[dict]:
    rows = db.execute(
        "SELECT address, label, total_copied, total_skipped FROM target_stats"
    ).fetchall()
    return [{"address": r[0], "label": r[1], "copied": r[2], "skipped": r[3]} for r in rows]


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
            pnl = float(entry.get("pnl", 0))
            if pnl <= 0:
                continue
            targets.append({
                "address":  addr,
                "label":    entry.get("name") or f"{addr[:8]}…",
                "copy_pct": COPY_PERCENT,
                "pnl":      pnl,
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


async def get_active_targets(session: aiohttp.ClientSession) -> list[dict]:
    global _dynamic_targets, _dynamic_cycle_count
    if not DYNAMIC_TARGETS:
        return TARGET_WALLETS
    _dynamic_cycle_count += 1
    if not _dynamic_targets or _dynamic_cycle_count % DYNAMIC_ROTATE_EVERY == 0:
        _dynamic_targets = await fetch_leaderboard_targets(session)
    return _dynamic_targets


# ── Conviction filter ──────────────────────────────────────────────────────────

_trade_size_history: dict[str, list[float]] = defaultdict(list)


def record_trade_size(address: str, size_usd: float) -> None:
    history = _trade_size_history[address]
    history.append(size_usd)
    if len(history) > CONVICTION_LOOKBACK:
        history.pop(0)


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
) -> tuple[float, str]:
    target_usd  = round(shares_traded * price, 2)
    my_balance  = await get_my_balance()

    if my_balance <= 0:
        return 0.0, "wallet balance $0 — check credentials"

    copied_size = round(target_usd * copy_pct, 2)
    wallet_cap  = round(my_balance * MAX_WALLET_PERCENT, 2)
    already     = get_cycle_exposure(token_id)
    market_cap  = round(my_balance * MAX_MARKET_PERCENT, 2)
    mkt_remain  = max(0.0, market_cap - already)
    final       = round(min(copied_size, wallet_cap, mkt_remain), 2)

    caps = []
    if copied_size > wallet_cap:
        caps.append(f"wallet cap ${wallet_cap:.2f}")
    if copied_size > mkt_remain:
        caps.append(f"market cap ${mkt_remain:.2f}")

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
    """
    Compute shares = usd / price rounded DOWN to 1 decimal place.
    Polymarket maker orders support max 2dp but 1dp is safest to avoid rejections.
    Uses integer truncation (not round()) to avoid floating point issues.
    """
    print(int(usd / price * 10) / 10)
    return int(usd / price * 10) / 10


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
        total = s["copied"] + s["skipped"]
        hit   = f"{s['copied']/total*100:.0f}%" if total else "—"
        lines.append(f"• **{s['label']}** — {s['copied']} copied, {s['skipped']} skipped ({hit})")
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
    Every REDEEM_INTERVAL seconds, fetch closed positions from the Data API
    and redeem any that have resolved with a non-zero payout.
    A position is redeemable if its current value > 0 AND the market is closed.
    """
    # Give the bot time to start up before first check
    await asyncio.sleep(30)

    while True:
        try:
            if not FUNDER_ADDRESS:
                await asyncio.sleep(REDEEM_INTERVAL)
                continue

            async with session.get(
                "https://data-api.polymarket.com/positions",
                params={"user": FUNDER_ADDRESS, "sizeThreshold": 0.01,
                        "limit": 500, "redeemable": "true"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                positions = await resp.json()

            redeemable = [
                p for p in positions
                if float(p.get("currentValue", 0)) > 0
                and p.get("redeemable", False)
            ]

            if not redeemable:
                await asyncio.sleep(REDEEM_INTERVAL)
                continue

            print(f"[Redeem] {len(redeemable)} redeemable position(s) found.")

            for pos in redeemable:
                condition_id = pos.get("conditionId") or pos.get("market", "")
                token_id     = pos.get("asset", "")
                value        = float(pos.get("currentValue", 0))
                title        = pos.get("title", token_id[:20])

                print(f"  [Redeem] {title[:50]} — value ${value:.2f}  conditionId={condition_id[:16]}…")

                success = await run_blocking(_redeem_position_sync, condition_id)

                if success:
                    print(f"  [Redeem] ✓ Redeemed ${value:.2f}")
                    await send_discord(session,
                        f"**Market:** {title}"
                        f"**Redeemed:** ${value:.2f} USDC returned to wallet",
                        color=0x57F287, title="✅ Position Redeemed"
                    )
                    # Clear from local position tracking
                    await record_position(token_id, -999, 0)
                else:
                    print(f"  [Redeem] ✗ Failed — may need manual redemption on Polymarket")
                    await send_discord(session,
                        f"**Market:** {title}"
                        f"**Value:** ${value:.2f}"
                        f"Auto-redeem failed — please redeem manually at polymarket.com",
                        color=0xFEE75C, title="⚠️ Redeem Failed — Manual Action Needed"
                    )

        except Exception as exc:
            print(f"[Redeem] Monitor error: {exc}")

        await asyncio.sleep(REDEEM_INTERVAL)


# ── Stop-loss monitor ──────────────────────────────────────────────────────────

async def stop_loss_monitor(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(STOP_LOSS_INTERVAL)
        positions = get_all_positions()
        if not positions:
            continue

        for pos in positions:
            token_id = pos["token_id"]
            shares   = pos["shares"]
            entry    = pos["entry_price"]
            if entry <= 0:
                continue

            current = get_live_best_bid(token_id)
            if current is None:
                current = await run_blocking(_rest_best_bid, token_id)
            if current is None:
                continue

            pnl_pct = (current - entry) / entry
            if pnl_pct > -STOP_LOSS_PCT:
                continue

            market   = _market_cache.get(token_id, {})
            question = market.get("question", token_id[:20])

            print(f"[StopLoss] {question[:40]} — PNL {pnl_pct*100:.1f}% — SELL")
            await discord_stop_loss(session, token_id, question, shares, entry, current, pnl_pct)

            if not DRY_RUN:
                try:
                    # FIX: price rounded to 2dp, size to 4dp
                    order = OrderArgs(
                        token_id=token_id,
                        price=round(current, 2),
                        size=int(shares * 10) / 10,  # truncate to 1dp
                        side="SELL"
                    )
                    signed = await run_blocking(client.create_order, order)
                    await run_blocking(client.post_order, signed, OrderType.FAK)
                    await record_position(token_id, -shares, current)
                except Exception as exc:
                    await discord_error(session, f"stop_loss SELL {token_id}", exc)
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

        await subscribe_token(token_id)

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

        # Conviction filter — evaluated BEFORE recording this trade in history
        high_conviction, conviction_note = is_high_conviction(address, trade_usd)
        if not high_conviction:
            print(f"    [skip] {conviction_note}")
            await log_trade(address, label, token_id, question, side, 0, trade_price, False, conviction_note)
            await discord_trade_alert(session, act, label, address, question, market_url,
                                      0, "—", False, conviction_note=conviction_note,
                                      reason=conviction_note)
            continue

        # Record AFTER check so this trade isn't in its own baseline
        record_trade_size(address, trade_usd)

        our_size, sizing_note = await compute_trade_size(token_id, shares_traded, trade_price, copy_pct)

        if our_size <= 0 and "balance" in sizing_note:
            await discord_error(session, "Zero balance", Exception(sizing_note))
            continue

        if our_size < MIN_TRADE_SIZE:
            reason = f"${our_size:.2f} below minimum ${MIN_TRADE_SIZE:.2f}"
            print(f"    [skip] {reason}")
            await log_trade(address, label, token_id, question, side, our_size, trade_price, False, reason)
            await discord_trade_alert(session, act, label, address, question, market_url,
                                      our_size, sizing_note, False, conviction_note, reason)
            continue

        if DRY_RUN:
            executed, reason = True, ""
            book_src = "WS" if get_live_best_ask(token_id) else "pending"
            print(f"    [~] {side} ${our_size:.2f}  '{question[:45]}'  [{book_src}]  {conviction_note}")
        else:
            executed, reason = await execute_trade(
                session, token_id, side, our_size, trade_price, positions)
            if executed:
                add_cycle_exposure(token_id, our_size)

        await log_trade(address, label, token_id, question, side, our_size, trade_price, executed, reason)
        await discord_trade_alert(session, act, label, address, question, market_url,
                                  our_size, sizing_note, executed, conviction_note, reason)

        mark = "~" if DRY_RUN else ("✓" if executed else "✗")
        print(f"    [{mark}] {side} ${our_size:.2f}  '{question[:45]}'"
              + (f"  — {reason}" if reason else ""))


# ── Seed + main ────────────────────────────────────────────────────────────────

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

        targets = await get_active_targets(session)
        await seed_seen_trades(session, targets)

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

            if cycle % 30 == 0:
                await discord_stats(session, targets)

            if ws_task.done():
                ws_task = asyncio.create_task(ws_book_listener())
            if stop_loss_task.done():
                stop_loss_task = asyncio.create_task(stop_loss_monitor(session))
            if redeem_task.done():
                redeem_task = asyncio.create_task(auto_redeem_monitor(session))

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())