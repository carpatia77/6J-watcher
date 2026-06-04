from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).parent

@dataclass
class Config:
    symbol:          str   = "6J"
    tick_size:       float = 0.00005
    db_path:         str   = str(BASE_DIR / "output" / "6j_liquidity.db")
    host:            str   = "127.0.0.1"
    port:            int   = 8765
    min_occurrences: int   = 3
    session_utc: dict = field(default_factory=lambda: {
        "ASIAN":    (0,  8),
        "LONDON":   (8,  13),
        "NEW_YORK": (13, 22),
    })

    def session_for(self, hour_utc: int) -> str:
        for name, (start, end) in self.session_utc.items():
            if start <= hour_utc < end:
                return name
        return "off_hours"
