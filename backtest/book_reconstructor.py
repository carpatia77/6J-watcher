from __future__ import annotations
import databento as db
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

FIXED_POINT = 1_000_000_000  # Databento: 1 unit = 1e-9
ACTION_TRADE = 84             # ord('T') — identifica trades no MBP-10


@dataclass
class BookSnapshot:
    timestamp_ns: int
    timestamp: datetime
    last_price: Optional[float]   # None se o record não for um trade (BUG 5 FIX)
    bid_levels: List[Dict] = field(default_factory=list)
    ask_levels: List[Dict] = field(default_factory=list)

    def to_dom_rows(self) -> List[Dict]:
        """
        Emite rows compatíveis com parse_dom_rows().
        Inclui timestamp_ns (BIGINT). O timestamp string foi otimizado (Arrow C++ cuidara disso).
        """
        rows = []
        ts = ""
        for lv in self.bid_levels:
            rows.append({
                "timestamp":    ts,
                "timestamp_ns": self.timestamp_ns,
                "price":        lv["price"],
                "level_index":  lv["idx"],
                "bid_volume":   lv["size"],
                "ask_volume":   0,
            })
        for lv in self.ask_levels:
            rows.append({
                "timestamp":    ts,
                "timestamp_ns": self.timestamp_ns,
                "price":        lv["price"],
                "level_index":  lv["idx"],
                "bid_volume":   0,
                "ask_volume":   lv["size"],
            })
        return rows


class BookReconstructor:
    def __init__(self, depth: int = 10):
        self.depth = depth

    def process_record(self, record) -> Optional[BookSnapshot]:
        """
        Constrói BookSnapshot a partir de record MBP-10.

        BUG 5 FIX: record.price em MBP-10 é o bid_px do top-of-book
        para snapshots sem trade — não o último preço negociado.
        last_price agora é preenchido apenas quando action == 'T' (trade);
        para snapshots puros, last_price = None.
        Callers downstream devem guardar para None antes de usar last_price.
        """
        if not hasattr(record, "levels") or not record.levels:
            return None

        ts_ns = record.ts_event
        ts = None

        # Determina se é trade para decidir se last_price é confiável
        action = getattr(record, "action", None)
        action_val = getattr(action, "value", action)
        is_trade = (action_val == ACTION_TRADE)

        # last_price só é preenchido para trades — para snapshots seria o bid do topo
        last_price: Optional[float] = record.price / FIXED_POINT if is_trade else None

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
        if getattr(action, "value", action) != ACTION_TRADE:
            return None

        ts_ns = record.ts_event
        price = record.price / FIXED_POINT
        size = getattr(record, "size", 0)
        if not size:
            return None

        # Robust against str, enum, and 'Side.BID' / 'B' / 'BID'
        side_raw = str(getattr(record, "side", "N"))
        side_char = side_raw.split(".")[-1].strip().upper()[0]
        if side_char == "B":
            side = 'buy'
        elif side_char == "A":
            side = 'sell'
        else:
            return None

        return {
            "timestamp":    "",
            "timestamp_ns": ts_ns,
            "price":        price,
            "volume":       size,
            "side":         side,
        }
