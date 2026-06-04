from __future__ import annotations
from collections import defaultdict, Counter
from datetime import datetime
from statistics import mean
from typing import Dict, List, Optional

from models import LiquidityCluster, DOMLevel, TapeEvent, BehaviorSignature

class LiquidityMatrix:
    def __init__(self, symbol: str, tick_size: float):
        self.symbol = symbol
        self.tick_size = tick_size
        self.matrix: Dict[float, Dict[str, List[LiquidityCluster]]] = defaultdict(lambda: defaultdict(list))
        self.dom_snapshots: Dict[float, Dict[str, List[DOMLevel]]] = defaultdict(lambda: defaultdict(list))
        self.tape_events: Dict[float, Dict[str, List[TapeEvent]]] = defaultdict(lambda: defaultdict(list))
        self.active_levels: Dict[float, List[LiquidityCluster]] = defaultdict(list)

    def normalize_price(self, price: float) -> float:
        return round(price / self.tick_size) * self.tick_size

    def time_bucket(self, ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M")

    def ingest_cluster(self, cluster: LiquidityCluster):
        p = self.normalize_price(cluster.price)
        t = self.time_bucket(cluster.timestamp)
        self.matrix[p][t].append(cluster)
        self.active_levels[p].append(cluster)

    def ingest_dom(self, dom: DOMLevel):
        p = self.normalize_price(dom.price)
        t = self.time_bucket(dom.timestamp)
        self.dom_snapshots[p][t].append(dom)

    def ingest_tape(self, tape: TapeEvent):
        p = self.normalize_price(tape.price)
        t = self.time_bucket(tape.timestamp)
        self.tape_events[p][t].append(tape)

    def build_from_events(self, tape_events: List[TapeEvent], dom_levels: List[DOMLevel], classify=None):
        for dom in dom_levels:
            self.ingest_dom(dom)
        for tape in tape_events:
            cluster = LiquidityCluster(
                symbol=tape.symbol,
                timestamp=tape.timestamp,
                price=tape.price,
                total_bid=tape.volume if tape.side.value == 'buy' else 0,
                total_ask=tape.volume if tape.side.value == 'sell' else 0,
                raw_payload=tape.raw,
            )
            if classify:
                cluster.behavior_signature = classify(cluster)
            self.ingest_cluster(cluster)
            self.ingest_tape(tape)

    def get_price_matrix(self, price: float) -> Dict:
        p = self.normalize_price(price)
        clusters = []
        for bucket in self.matrix.get(p, {}).values():
            clusters.extend(bucket)
        doms = []
        for bucket in self.dom_snapshots.get(p, {}).values():
            doms.extend(bucket)
        tapes = []
        for bucket in self.tape_events.get(p, {}).values():
            tapes.extend(bucket)
        return {
            "symbol": self.symbol,
            "price": p,
            "cluster_count": len(clusters),
            "dom_count": len(doms),
            "tape_count": len(tapes),
            "cluster_signature_distribution": dict(Counter(c.behavior_signature.value for c in clusters)),
            "dom_total_bid": sum(d.bid_volume for d in doms),
            "dom_total_ask": sum(d.ask_volume for d in doms),
            "tape_total_volume": sum(t.volume for t in tapes),
            "first_seen": min([c.timestamp for c in clusters] + [d.timestamp for d in doms] + [t.timestamp for t in tapes]) if (clusters or doms or tapes) else None,
            "last_seen": max([c.timestamp for c in clusters] + [d.timestamp for d in doms] + [t.timestamp for t in tapes]) if (clusters or doms or tapes) else None,
        }

    def hotspots(self, min_occurrences: int = 3) -> List[Dict]:
        out = []
        for price, clusters in self.active_levels.items():
            if len(clusters) < min_occurrences:
                continue
            sig = Counter([c.behavior_signature.value for c in clusters]).most_common(1)[0][0]
            out.append({
                "price": price,
                "occurrences": len(clusters),
                "dominant_signature": sig,
                "avg_confidence": mean(c.confidence for c in clusters),
                "first": min(c.timestamp for c in clusters),
                "last": max(c.timestamp for c in clusters),
            })
        return sorted(out, key=lambda x: x["occurrences"], reverse=True)
