from __future__ import annotations
from collections import defaultdict, Counter
from datetime import datetime
from statistics import mean
import copy
import threading
from typing import Callable, Dict, List, Optional
from models import BehaviorSignature, DOMLevel, LiquidityCluster, TapeEvent


class LiquidityMatrix:
    def __init__(self, symbol: str, tick_size: float):
        self.symbol   = symbol
        self.tick_size = tick_size
        self.matrix:        Dict[float, Dict[str, List[LiquidityCluster]]] = defaultdict(lambda: defaultdict(list))
        self.dom_snapshots: Dict[float, Dict[str, List[DOMLevel]]]         = defaultdict(lambda: defaultdict(list))
        self.tape_index:    Dict[float, Dict[str, List[TapeEvent]]]        = defaultdict(lambda: defaultdict(list))
        self.active_levels: Dict[float, List[LiquidityCluster]]            = defaultdict(list)
        self.lock = threading.RLock()

    def snapshot(self) -> Dict:
        """Capture list lengths so we can truncate back on restore."""
        with self.lock:
            return {
                "matrix":        {p: {t: len(lst) for t, lst in buckets.items()} for p, buckets in self.matrix.items()},
                "dom_snapshots": {p: {t: len(lst) for t, lst in buckets.items()} for p, buckets in self.dom_snapshots.items()},
                "tape_index":    {p: {t: len(lst) for t, lst in buckets.items()} for p, buckets in self.tape_index.items()},
                "active_levels": {p: len(lst) for p, lst in self.active_levels.items()},
            }

    def restore(self, snap: Dict):
        """Truncate lists back to snapshot lengths, removing anything added after."""
        with self.lock:
            for store, name in [
                (self.matrix, "matrix"),
                (self.dom_snapshots, "dom_snapshots"),
                (self.tape_index, "tape_index"),
            ]:
                snap_store = snap[name]
                for p in list(store.keys()):
                    if p not in snap_store:
                        del store[p]
                        continue
                    for t in list(store[p].keys()):
                        if t not in snap_store[p]:
                            del store[p][t]
                        else:
                            store[p][t] = store[p][t][:snap_store[p][t]]

            snap_al = snap["active_levels"]
            for p in list(self.active_levels.keys()):
                if p not in snap_al:
                    del self.active_levels[p]
                else:
                    self.active_levels[p] = self.active_levels[p][:snap_al[p]]

    def normalize_price(self, price: float) -> float:
        return round(price / self.tick_size) * self.tick_size

    def time_bucket(self, ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M")

    def ingest_cluster(self, cluster: LiquidityCluster):
        p = self.normalize_price(cluster.price)
        t = self.time_bucket(cluster.timestamp)
        with self.lock:
            self.matrix[p][t].append(cluster)
            self.active_levels[p].append(cluster)

    def ingest_dom(self, dom: DOMLevel):
        p = self.normalize_price(dom.price)
        t = self.time_bucket(dom.timestamp)
        with self.lock:
            self.dom_snapshots[p][t].append(dom)

    def ingest_tape(self, tape: TapeEvent):
        p = self.normalize_price(tape.price)
        t = self.time_bucket(tape.timestamp)
        with self.lock:
            self.tape_index[p][t].append(tape)

    def build_from_events(
        self,
        tape_events: List[TapeEvent],
        dom_levels:  List[DOMLevel],
        clusters:    Optional[List[LiquidityCluster]] = None,
        classify:    Optional[Callable] = None,
    ):
        for dom in dom_levels:
            self.ingest_dom(dom)

        for tape in tape_events:
            self.ingest_tape(tape)

        if clusters:
            for cluster in clusters:
                self.ingest_cluster(cluster)
        else:
            for tape in tape_events:
                cluster = LiquidityCluster(
                    symbol    = tape.symbol,
                    timestamp = tape.timestamp,
                    price     = tape.price,
                    total_bid = tape.volume if tape.side.value == "buy"  else 0,
                    total_ask = tape.volume if tape.side.value == "sell" else 0,
                    cumdelta  = tape.volume if tape.side.value == "buy"  else -tape.volume,
                    raw_payload = tape.raw,
                )
                if classify:
                    cluster.behavior_signature = classify(cluster)
                self.ingest_cluster(cluster)

    def get_price_matrix(self, price: float) -> Dict:
        p        = self.normalize_price(price)
        with self.lock:
            clusters = [c for bucket in self.matrix.get(p, {}).values() for c in bucket]
            doms     = [d for bucket in self.dom_snapshots.get(p, {}).values() for d in bucket]
            tapes    = [t for bucket in self.tape_index.get(p, {}).values() for t in bucket]
            all_ts   = [c.timestamp for c in clusters] + [d.timestamp for d in doms] + [t.timestamp for t in tapes]
        return {
            "symbol":                       self.symbol,
            "price":                        p,
            "cluster_count":                len(clusters),
            "dom_count":                    len(doms),
            "tape_count":                   len(tapes),
            "cluster_signature_distribution": dict(Counter(c.behavior_signature.value for c in clusters)),
            "dom_total_bid":                sum(d.bid_volume for d in doms),
            "dom_total_ask":                sum(d.ask_volume for d in doms),
            "tape_total_volume":            sum(t.volume for t in tapes),
            "tape_buy_volume":              sum(t.volume for t in tapes if t.side.value == "buy"),
            "tape_sell_volume":             sum(t.volume for t in tapes if t.side.value == "sell"),
            "avg_confidence":               mean(c.confidence for c in clusters) if clusters else 0.0,
            "first_seen":                   min(all_ts) if all_ts else None,
            "last_seen":                    max(all_ts) if all_ts else None,
        }

    def hotspots(self, min_occurrences: int = 3) -> List[Dict]:
        out = []
        with self.lock:
            for price, clusters in self.active_levels.items():
                if len(clusters) < min_occurrences:
                    continue
                sig = Counter(c.behavior_signature.value for c in clusters).most_common(1)[0][0]
                out.append({
                    "price":              price,
                    "occurrences":        len(clusters),
                    "dominant_signature": sig,
                    "avg_confidence":     mean(c.confidence for c in clusters),
                    "total_bid":          sum(c.total_bid for c in clusters),
                    "total_ask":          sum(c.total_ask for c in clusters),
                    "first":              min(c.timestamp for c in clusters),
                    "last":               max(c.timestamp for c in clusters),
                })
        return sorted(out, key=lambda x: x["occurrences"], reverse=True)

    def prune_stale_data(self, hours: int = 4):
        import datetime as dt
        cutoff_str = (dt.datetime.utcnow() - dt.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M")
        with self.lock:
            for p in list(self.matrix.keys()):
                for t in list(self.matrix[p].keys()):
                    if t < cutoff_str:
                        del self.matrix[p][t]
                if not self.matrix[p]:
                    del self.matrix[p]

            for p in list(self.dom_snapshots.keys()):
                for t in list(self.dom_snapshots[p].keys()):
                    if t < cutoff_str:
                        del self.dom_snapshots[p][t]
                if not self.dom_snapshots[p]:
                    del self.dom_snapshots[p]

            for p in list(self.tape_index.keys()):
                for t in list(self.tape_index[p].keys()):
                    if t < cutoff_str:
                        del self.tape_index[p][t]
                if not self.tape_index[p]:
                    del self.tape_index[p]

            self.active_levels.clear()
            for p, buckets in self.matrix.items():
                for t, clusters in buckets.items():
                    self.active_levels[p].extend(clusters)
