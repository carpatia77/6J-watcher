from __future__ import annotations
"""
parser_tsdom.py
---------------
Responsável por transformar payloads brutos da ClusterDelta
(postados pelo MQL bridge) em objetos TapeEvent e DOMLevel.

Formato esperado do payload JSON:

  {
    "symbol": "6J",
    "timestamp": "2026-06-04 10:00:00",
    "tape": [
      { "timestamp": "2026-06-04 10:00:01",
        "price": 0.006760,
        "volume": 8,
        "side": "buy" },
      ...
    ],
    "dom": [
      { "timestamp": "2026-06-04 10:00:01",
        "price": 0.006760,
        "level_index": 1,
        "bid_volume": 120,
        "ask_volume": 80 },
      ...
    ]
  }

Legenda de side vindo da DLL ClusterDelta:
  "A" ou "ask" ou "sell"  → Side.SELL  (agressão de venda)
  "B" ou "bid" ou "buy"   → Side.BUY   (agressão de compra)

Formato de timestamp aceito:
  "YYYY-MM-DD HH:MM:SS"
  "YYYY-MM-DDTHH:MM:SS"
  epoch int/float
"""
from datetime import datetime
from typing import Any, Dict, List
from models import DOMLevel, Side, TapeEvent


_SIDE_MAP: Dict[str, Side] = {
    "b": Side.BUY, "buy": Side.BUY, "bid": Side.BUY,
    "a": Side.SELL, "sell": Side.SELL, "ask": Side.SELL,
}


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value)
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {value!r}")


def _parse_side(raw: str) -> Side:
    return _SIDE_MAP.get(str(raw).lower().strip(), Side.UNKNOWN)


def parse_tape_rows(rows: List[Dict], symbol: str) -> List[TapeEvent]:
    events: List[TapeEvent] = []
    for row in rows:
        try:
            ts    = _to_datetime(row["timestamp"])
            price = float(row["price"])
            vol   = int(row.get("volume", 0))
            side  = _parse_side(row.get("side", ""))
            if price <= 0 or vol < 0:
                continue
            events.append(TapeEvent(
                symbol=symbol, timestamp=ts,
                price=price, volume=vol, side=side, raw=row))
        except (KeyError, ValueError, TypeError):
            continue
    return events


def parse_dom_rows(rows: List[Dict], symbol: str) -> List[DOMLevel]:
    levels: List[DOMLevel] = []
    for row in rows:
        try:
            ts    = _to_datetime(row["timestamp"])
            price = float(row["price"])
            idx   = int(row.get("level_index", 0))
            bidv  = int(row.get("bid_volume", 0))
            askv  = int(row.get("ask_volume", 0))
            if price <= 0:
                continue
            levels.append(DOMLevel(
                symbol=symbol, timestamp=ts, price=price,
                level_index=idx, bid_volume=bidv, ask_volume=askv, raw=row))
        except (KeyError, ValueError, TypeError):
            continue
    return levels
