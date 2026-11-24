"""
tests/test_ingestor.py
======================
Tests del ciclo de conexión del HyperliquidIngestor (Track B):
no dejar loops huérfanos al desconectar, y reconectar en cierre limpio.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import data.ingestor as ingestor_mod
from data.ingestor import HyperliquidIngestor
from data.state import MarketState


class _FakeWSConnect:
    """Imita websockets.connect(...) como async context manager."""

    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return False


def _make_ingestor(monkeypatch):
    ing = HyperliquidIngestor(state=MarketState(), db_worker=MagicMock(), symbol="BTC")
    ing._db_conn = MagicMock()
    monkeypatch.setattr(ingestor_mod, "set_bot_state", AsyncMock())
    monkeypatch.setattr(
        ingestor_mod.websockets, "connect", lambda *a, **k: _FakeWSConnect(MagicMock())
    )
    ing._subscribe = AsyncMock()
    return ing


@pytest.mark.asyncio
async def test_connect_and_ingest_cancels_siblings_on_disconnect(monkeypatch):
    """Si el message loop cae, las sub-tareas (reconcile/parquet) se cancelan,
    en vez de quedar como loops huérfanos que se multiplican en cada reconexión."""
    ing = _make_ingestor(monkeypatch)
    cancelled = {"reconcile": False, "parquet": False}

    async def fake_message_loop(ws):
        await asyncio.sleep(0.01)
        raise RuntimeError("ws closed")

    async def fake_reconcile_loop():
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled["reconcile"] = True
            raise

    async def fake_parquet_loop():
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled["parquet"] = True
            raise

    ing._message_loop = fake_message_loop
    ing._reconcile_loop = fake_reconcile_loop
    ing._parquet_dump_loop = fake_parquet_loop

    with pytest.raises(RuntimeError):
        await ing._connect_and_ingest()

    assert cancelled["reconcile"] is True
    assert cancelled["parquet"] is True


@pytest.mark.asyncio
async def test_connect_and_ingest_reconnects_on_clean_close(monkeypatch):
    """Si el message loop termina SIN error (cierre limpio del server), igual se
    fuerza la reconexión (antes el bot quedaba sordo esperando los loops infinitos)."""
    ing = _make_ingestor(monkeypatch)

    async def clean_message_loop(ws):
        await asyncio.sleep(0.01)
        return

    async def infinite_loop():
        await asyncio.sleep(100)

    ing._message_loop = clean_message_loop
    ing._reconcile_loop = infinite_loop
    ing._parquet_dump_loop = infinite_loop

    with pytest.raises(Exception):
        await ing._connect_and_ingest()
