from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
import time


class Side(str, Enum):
    BUY     = "buy"
    SELL    = "sell"
    UNKNOWN = "unknown"


class BehaviorSignature(str, Enum):
    ICEBERG_ACCUMULATION = "iceberg_accumulation"
    ICEBERG_DISTRIBUTION = "iceberg_distribution"
    SPOOFING_WALL        = "spoofing_wall"
    ABSORPTION_PASSIVE   = "absorption_passive"
    BREAKOUT_GENUINE     = "breakout_genuine"
    LIQUIDITY_VACUUM     = "liquidity_vacuum"
    MAGNET_EFFECT        = "magnet_effect" # DEPRECATED: Scheduled for cleanup
    DEFENSE_LINE         = "defense_line"
    UNKNOWN              = "unknown"


@dataclass
class TapeEvent:
    symbol:    str
    timestamp: datetime
    price:     float
    volume:    int
    side:      Side = Side.UNKNOWN
    raw:       Dict[str, Any] = field(default_factory=dict)


@dataclass
class DOMLevel:
    symbol:      str
    timestamp:   datetime
    price:       float
    level_index: int
    bid_volume:  int = 0
    ask_volume:  int = 0
    raw:         Dict[str, Any] = field(default_factory=dict)


@dataclass
class LiquidityCluster:
    symbol:             str
    timestamp:          datetime
    price:              float
    session:            str = "unknown"
    behavior_signature: BehaviorSignature = BehaviorSignature.UNKNOWN
    total_ask:          int   = 0
    total_bid:          int   = 0
    cumdelta:           int   = 0
    deltamin:           int   = 0
    deltamax:           int   = 0
    delta_price_ticks:  int   = 0
    confidence:         float = 0.5
    outcome:            Optional[str] = None
    batch_id:           str = field(default_factory=lambda: f"{time.time_ns()}")
    raw_payload:        Dict[str, Any] = field(default_factory=dict)

    @property
    def imbalance(self) -> int:
        return self.total_ask - self.total_bid

    @property
    def dominant_side(self) -> Side:
        if self.total_bid > self.total_ask:
            return Side.BUY
        if self.total_ask > self.total_bid:
            return Side.SELL
        return Side.UNKNOWN


@dataclass
class KeyLevel:
    symbol:             str
    price:              float
    occurrences:        int = 0
    first_seen:         Optional[datetime] = None
    last_seen:          Optional[datetime] = None
    dominant_signature: Optional[str] = None
    days_active:        int = 0
    reliability_score:  float = 0.0
