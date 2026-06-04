from __future__ import annotations
import json
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

@dataclass
class TapeEventRaw:
    timestamp: str
    price: float
    volume: int
    side: str

@dataclass
class DOMLevelRaw:
    timestamp: str
    price: float
    level_index: int
    bid_volume: int
    ask_volume: int


def parse_mql_tsdom_payload(payload: str) -> Tuple[List[Dict], List[Dict]]:
    tape_rows: List[Dict] = []
    dom_rows: List[Dict] = []
    if not payload:
        return tape_rows, dom_rows
    data = json.loads(payload)
    for item in data.get("tape", []):
        tape_rows.append({
            "timestamp": item["timestamp"],
            "price": item["price"],
            "volume": item["volume"],
            "side": item.get("side", "unknown"),
        })
    for item in data.get("dom", []):
        dom_rows.append({
            "timestamp": item["timestamp"],
            "price": item["price"],
            "level_index": item.get("level_index", 0),
            "bid_volume": item.get("bid_volume", 0),
            "ask_volume": item.get("ask_volume", 0),
        })
    return tape_rows, dom_rows
