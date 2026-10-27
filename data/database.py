"""
data/database.py
================
Worker de base de datos asíncrono — Cold Path.

Correcciones integradas:
- PRAGMA journal_mode=WAL para lecturas/escrituras simultáneas sin bloqueo
- Escrituras via asyncio.Queue (no bloquea el WebSocket)
- Estado STARTING/RUNNING/HALTED para el watchdog (Corrección #5)
- Esquema Parquet versionado (Gap #1)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = Path("data/db/bot.db")
PARQUET_DIR = Path("data/parquet")


class BotProcessState(str, Enum):
    """
    CORRECCIÓN #5 (Watchdog Bootstrap):
    El bot escribe STARTING antes de conectar al WebSocket.
    El Watchdog no alerta si state=STARTING y age < 60s.
    """
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    HALTED = "HALTED"


@dataclass
class DBEvent:
    """Evento encolado para escritura asíncrona en la BD."""
    table: str
    data: dict[str, Any]
    event_type: str = "insert"   # "insert" | "update" | "upsert"


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS bot_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    state       TEXT NOT NULL DEFAULT 'STARTING',
    updated_at  REAL NOT NULL,
    metadata    TEXT
);

CREATE TABLE IF NOT EXISTS candles_1m (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT NOT NULL,
    ts_ms       INTEGER NOT NULL UNIQUE,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      REAL NOT NULL,
    created_at  REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
);

CREATE TABLE IF NOT EXISTS decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id       TEXT NOT NULL UNIQUE,
    symbol          TEXT NOT NULL,
    strategy        TEXT NOT NULL,
    side            TEXT NOT NULL,
    regime          TEXT NOT NULL,
    obi             REAL,
    cvd_1m          REAL,
    cvd_5m          REAL,
    basis_bps       REAL,
    oi_change_5m    REAL,
    proposed_price  REAL,
    proposed_size   REAL,
    stop_loss       REAL,
    shadow_mode     INTEGER NOT NULL DEFAULT 1,
    vetoed          INTEGER NOT NULL DEFAULT 0,
    veto_reason     TEXT,
    created_at      REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
);

CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cloid           TEXT NOT NULL UNIQUE,
    signal_id       TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    order_type      TEXT NOT NULL,
    size            REAL NOT NULL,
    limit_price     REAL,
    stop_price      REAL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    hl_oid          INTEGER,
    fill_price      REAL,
    fill_size       REAL,
    fee_paid        REAL,
    created_at      REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
    updated_at      REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
);

CREATE TABLE IF NOT EXISTS shadow_pnl (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id     INTEGER NOT NULL REFERENCES decisions(id),
    horizon_label   TEXT NOT NULL,   -- '5m', '15m', '1h'
    entry_price     REAL NOT NULL,
    exit_price      REAL,
    pnl_pct         REAL,
    resolved_at     REAL,
    created_at      REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
);

CREATE TABLE IF NOT EXISTS daily_risk (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date_utc        TEXT NOT NULL UNIQUE,
    equity_open     REAL NOT NULL,
    equity_peak     REAL NOT NULL,
    realized_pnl    REAL NOT NULL DEFAULT 0.0,
    unrealized_pnl  REAL NOT NULL DEFAULT 0.0,
    paused_until    REAL,
    kill_switch     INTEGER NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT (unixepoch('now', 'subsec'))
);

CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles_1m(ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_orders_cloid ON orders(cloid);
CREATE INDEX IF NOT EXISTS idx_decisions_created ON decisions(created_at DESC);
"""


async def init_db() -> aiosqlite.Connection:
    """
    Inicializa la base de datos y aplica el esquema.
    Llama esto UNA VEZ al arrancar el bot.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    await db.executescript(SCHEMA_SQL)
    await db.commit()
    return db


async def set_bot_state(
    db: aiosqlite.Connection,
    state: BotProcessState,
    metadata: dict[str, Any] | None = None,
) -> None:
    """
    CORRECCIÓN #5: Escribe el estado del proceso en la BD.
    Se llama PRIMERO (STARTING) antes de conectar al WebSocket.
    """
    await db.execute(
        """
        INSERT INTO bot_state (id, state, updated_at, metadata)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            state = excluded.state,
            updated_at = excluded.updated_at,
            metadata = excluded.metadata
        """,
        (
            state.value,
            time.time(),
            json.dumps(metadata) if metadata else None,
        ),
    )
    await db.commit()
    logger.info("Bot state → %s", state.value)


class DatabaseWorker:
    """
    Worker asíncrono de escritura en BD.
    Recibe eventos via asyncio.Queue y los persiste sin bloquear el Hot Path.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db
        self._queue: asyncio.Queue[DBEvent | None] = asyncio.Queue(maxsize=10_000)
        self._running = False

    async def put(self, event: DBEvent) -> None:
        """Encola un evento de escritura. No bloquea el caller."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("DB queue full, dropping event for table=%s", event.table)

    async def run(self) -> None:
        """
        Bucle principal del worker.
        Procesa eventos en batches de hasta 50 cada 100ms para eficiencia.
        """
        self._running = True
        logger.info("DatabaseWorker started")

        while self._running:
            batch: list[DBEvent] = []

            try:
                # Espera hasta 100ms por el primer evento
                event = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                if event is None:
                    break
                batch.append(event)

                # Drena el resto de la cola sin esperar (batch)
                while not self._queue.empty() and len(batch) < 50:
                    ev = self._queue.get_nowait()
                    if ev is None:
                        self._running = False
                        break
                    batch.append(ev)

            except asyncio.TimeoutError:
                continue

            if batch:
                await self._process_batch(batch)

        logger.info("DatabaseWorker stopped")

    async def _process_batch(self, events: list[DBEvent]) -> None:
        try:
            async with self._db.execute("BEGIN"):
                pass
            for event in events:
                await self._write_event(event)
            await self._db.commit()
        except Exception:
            logger.exception("DB batch write failed, rolling back")
            await self._db.rollback()

    async def _write_event(self, event: DBEvent) -> None:
        cols = ", ".join(event.data.keys())
        placeholders = ", ".join(f":{k}" for k in event.data.keys())

        if event.event_type == "insert":
            sql = f"INSERT OR IGNORE INTO {event.table} ({cols}) VALUES ({placeholders})"
        elif event.event_type == "upsert":
            updates = ", ".join(f"{k} = excluded.{k}" for k in event.data.keys())
            # FIX #3: SQLite requiere especificar el target de conflicto para upsert
            if event.table == "candles_1m":
                # candles_1m tiene UNIQUE en ts_ms
                sql = (
                    f"INSERT INTO {event.table} ({cols}) VALUES ({placeholders}) "
                    f"ON CONFLICT(ts_ms) DO UPDATE SET {updates}"
                )
            else:
                sql = (
                    f"INSERT INTO {event.table} ({cols}) VALUES ({placeholders}) "
                    f"ON CONFLICT DO UPDATE SET {updates}"
                )
        else:
            raise ValueError(f"Unknown event_type: {event.event_type}")

        await self._db.execute(sql, event.data)

    async def stop(self) -> None:
        """Envía señal de parada al worker."""
        await self._queue.put(None)
        self._running = False


async def dump_trades_to_parquet(
    trades: list[dict[str, Any]],
    symbol: str,
    schema_version: int = 1,
) -> None:
    """
    GAP #1 (Parquet sin versión de esquema):
    Vuelca trades a Parquet con metadatos de versión de esquema.
    Se llama cada 5 minutos desde el ingestor.
    """
    if not trades:
        return

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema([
            pa.field("timestamp_ms", pa.int64()),
            pa.field("price", pa.float64()),
            pa.field("size", pa.float64()),
            pa.field("side", pa.string()),
            pa.field("symbol", pa.string()),
        ])

        table = pa.table(
            {
                "timestamp_ms": [t["timestamp_ms"] for t in trades],
                "price": [t["price"] for t in trades],
                "size": [t["size"] for t in trades],
                "side": [t["side"] for t in trades],
                "symbol": [symbol] * len(trades),
            },
            schema=schema,
        )

        # Metadatos de versión para compatibilidad futura
        metadata = {
            b"schema_version": str(schema_version).encode(),
            b"hl_api_version": b"1",
            b"created_at": str(time.time()).encode(),
            b"symbol": symbol.encode(),
        }
        table = table.replace_schema_metadata(metadata)

        ts = int(time.time())
        out_path = PARQUET_DIR / f"{symbol}_trades_{ts}.parquet"
        pq.write_table(table, str(out_path), compression="snappy")
        logger.debug("Dumped %d trades to %s", len(trades), out_path)

    except ImportError:
        logger.warning("pyarrow not installed, skipping Parquet dump")
    except Exception:
        logger.exception("Failed to dump trades to Parquet")
