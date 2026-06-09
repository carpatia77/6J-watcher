from __future__ import annotations
import databento as db
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

FIXED_POINT = 1_000_000_000  # Databento: 1 unit = 1e-9


@dataclass
class BookSnapshot:
    timestamp_ns: int
    timestamp: datetime
    last_price: float
    bid_levels: List[Dict] = field(default_factory=list)
    ask_levels: List[Dict] = field(default_factory=list)

    def to_dom_rows(self) -> List[Dict]:
        """
        Emite rows compatíveis com parse_dom_rows().
        Timestamp em microsegundos (%f) para preservar ordenação causal no DuckDB.
        """
        rows = []
        ts = self.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")
        for lv in self.bid_levels:
            rows.append({
                "timestamp": ts,
                "price": lv["price"],
                "level_index": lv["idx"],
                "bid_volume": lv["size"],
                "ask_volume": 0,
            })
        for lv in self.ask_levels:
            rows.append({
                "timestamp": ts,
                "price": lv["price"],
                "level_index": lv["idx"],
                "bid_volume": 0,
                "ask_volume": lv["size"],
            })
        return rows


class BookReconstructor:
    def __init__(self, depth: int = 10):
        self.depth = depth

    def process_record(self, record) -> Optional[BookSnapshot]:
        """Constrói BookSnapshot a partir de record MBP-10."""
        if not hasattr(record, "levels") or not record.levels:
            return None

        ts_ns = record.ts_event
        ts = datetime.fromtimestamp(ts_ns / 1e9, timezone.utc)
        last_price = record.price / FIXED_POINT

        bid_levels, ask_levels = [], []
        for i, lv in enumerate(record.levels[:self.depth]):
            bid_levels.append({"price": lv.bid_px / FIXED_POINT, "size": lv.bid_sz, "idx": i})
            ask_levels.append({"price": lv.ask_px / FIXED_POINT, "size": lv.ask_sz, "idx": i})

        return BookSnapshot(
            timestamp_ns=ts_ns, timestamp=ts,
            last_price=last_price,
            bid_levels=bid_levels, ask_levels=ask_levels,
        )

    def extract_tape_event(self, record) -> Optional[Dict]:
        """
        Extrai trade de record MBP-10.
        action == 84 ('T') identifica trades no Databento.
        side: 'B' = bid aggressor (compra), 'A' = ask aggressor (venda).
        side 'N' (neutro) é descartado — não polui o CVD direcional.
        """
        action = getattr(record, "action", None)
        if getattr(action, "value", action) != 84:
            return None

        ts = datetime.fromtimestamp(record.ts_event / 1e9, timezone.utc)
        price = record.price / FIXED_POINT
        size = getattr(record, "size", 0)
        if size == 0:
            return None

        side_char = str(getattr(record, "side", "N")).upper()
        if side_char == "B":
            side = "buy"
        elif side_char == "A":
            side = "sell"
        else:
            # 'N' = neutro (spread leg, bloco institucional sem direção) — descarta
            return None

        return {
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "price": price,
            "volume": size,
            "side": side,
        }
