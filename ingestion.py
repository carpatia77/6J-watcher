from __future__ import annotations
"""
ingestion.py
------------
Orquestra o pipeline completo de ingestão:
  1. Parse T&S e DOM
  2. Persiste no DuckDB
  3. Alimenta a LiquidityMatrix
  4. Classifica assinatura comportamental (micro-janelas de 250ms)
  5. Devolve clusters gerados
"""
import bisect
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from config import Config
from models import LiquidityCluster
from parser_tsdom import parse_tape_rows, parse_dom_rows
from adaptive_pattern_engine import AdaptivePatternEngine
from liquidity_matrix import LiquidityMatrix
from repository_duckdb import DuckDBRepository

# Janela de micro-agregação em nanossegundos (250ms)
_WINDOW_NS = 250_000_000


class IngestionService:
    def __init__(self, repo: DuckDBRepository, matrix: LiquidityMatrix,
                 engine: AdaptivePatternEngine, cfg: Config, narrator=None):
        self.repo     = repo
        self.matrix   = matrix
        self.engine   = engine
        self.cfg      = cfg
        self.narrator = narrator

        row = repo.conn.execute(
            "SELECT price FROM tape_events WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            [cfg.symbol]
        ).fetchone()
        self.last_closed_price = row[0] if row else None
        if self.last_closed_price is not None:
            logging.info(
                "[IngestionService] Cold start: last_closed_price=%s (do DuckDB)",
                self.last_closed_price
            )

    # ── DOM index ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_dom_index(
        dom_rows: List[Dict],
        tick_size: float,
        top_n: int = 5,
    ) -> Dict[int, List[Tuple[int, int, int, int]]]:
        """
        Pré-indexa dom_rows uma única vez por batch.

        Estrutura retornada:
            index[price_key] = sorted list of (ts_ns, bid_sum, ask_sum, min_level)

        price_key = round(price / tick_size)  — inteiro, sem drift de float.
        Apenas os top_n níveis (level_index < top_n) são considerados.
        min_level = mínimo level_index visto no snapshot (ts_ns, price_key).
        Lista mantida ordenada por ts_ns para bisect O(log K) em _dom_at.

        Complexidade: O(D log D) build, O(log K) lookup.
        D = len(dom_rows), K = snapshots únicos por preço (tipicamente < 100).
        """
        if not dom_rows or tick_size <= 0:
            return {}

        # acc[(pk, ts_ns)] = (bid_sum, ask_sum, min_level)
        acc: Dict[Tuple[int, int], Tuple[int, int, int]] = {}
        for row in dom_rows:
            ts_ns = row.get("timestamp_ns")
            if ts_ns is None:
                continue
            lvl = row.get("level_index", top_n)
            if lvl >= top_n:
                continue
            pk  = round(row.get("price", 0.0) / tick_size)
            key = (pk, ts_ns)
            b, a, ml = acc.get(key, (0, 0, top_n))
            acc[key] = (
                b + row.get("bid_volume", 0),
                a + row.get("ask_volume", 0),
                min(ml, lvl),          # preserva o nível mais raso visto
            )

        # Agrupa por price_key e ordena por ts_ns — lista pronta para bisect
        grouped: Dict[int, List[Tuple[int, int, int, int]]] = defaultdict(list)
        for (pk, ts_ns), (b, a, ml) in acc.items():
            grouped[pk].append((ts_ns, b, a, ml))

        index: Dict[int, List[Tuple[int, int, int, int]]] = {}
        for pk, entries in grouped.items():
            entries.sort()          # ordena por ts_ns
            index[pk] = entries

        return index

    def _dom_at(
        self,
        dom_index: Dict[int, List[Tuple[int, int, int, int]]],
        price: float,
        end_ns: int,
    ) -> Tuple[int, int, int]:
        """
        Retorna (dom_bid, dom_ask, dom_min_level) do snapshot DOM mais recente
        com timestamp_ns <= end_ns para o price_key dado.

        O(log K) via bisect sobre lista pré-ordenada.
        Retorna (0, 0, 9) quando não há snapshot DOM disponível
        (9 = fallback mais profundo — neutro nos multiplicadores).
        """
        pk      = round(price / self.cfg.tick_size)
        entries = dom_index.get(pk)
        if not entries:
            return (0, 0, 9)

        lo, hi = 0, len(entries)
        while lo < hi:
            mid = (lo + hi) // 2
            if entries[mid][0] <= end_ns:
                lo = mid + 1
            else:
                hi = mid
        pos = lo - 1
        if pos < 0:
            return (0, 0, 9)
        _, b, a, ml = entries[pos]
        return (b, a, ml)

    # ── Micro-agregação em janelas de 250ms ────────────────────────────────────────

    def _build_clusters_from_windows(
        self,
        tape: list,
        dom_index: Dict,
        symbol: str,
        batch_id: str,
    ) -> List[LiquidityCluster]:
        """
        Agrega TapeEvents em janelas de 250ms.

        Dentro de cada janela:
          - total_bid / total_ask acumulam ambos os lados
          - delta_price_ticks = (last_price - first_price) / tick_size
          - cumdelta / deltamin / deltamax via CVD incremental
          - dom_bid / dom_ask / dom_min_level injetados via _dom_at() no fechamento

        Fallback para produção MQL5 sem timestamp_ns:
          Cada TapeEvent gera sua própria janela (sem regressão).
        """
        if not tape:
            return []

        clusters: List[LiquidityCluster] = []

        w_start_ns:    Optional[int]   = None
        w_first_price: Optional[float] = None
        w_last_price:  Optional[float] = None
        w_ts                           = None
        total_bid = total_ask = 0
        cumdelta = deltamin = deltamax = 0

        def _flush(end_ns: int) -> None:
            nonlocal total_bid, total_ask, cumdelta, deltamin, deltamax
            nonlocal w_start_ns, w_first_price, w_last_price, w_ts

            if w_ts is None:
                return

            dp = round((w_last_price - w_first_price) / self.cfg.tick_size)
            dom_bid, dom_ask, dom_min_level = self._dom_at(dom_index, w_last_price, end_ns)

            c = LiquidityCluster(
                symbol    = symbol,
                timestamp = w_ts,
                price     = w_last_price,
                session   = self.cfg.session_for(w_ts.hour),
                total_bid = total_bid,
                total_ask = total_ask,
                cumdelta  = cumdelta,
                deltamin  = deltamin,
                deltamax  = deltamax,
                delta_price_ticks = dp,
                batch_id  = batch_id,
                dom_min_level = dom_min_level,
                raw_payload = {
                    "window_ns":     w_start_ns,
                    "timestamp_ns":  w_start_ns,
                    "dom_bid":       dom_bid,
                    "dom_ask":       dom_ask,
                    "dom_min_level": dom_min_level,   # rastreabilidade no JSON
                },
            )
            sig, conf = self.engine.classify(c)
            c.behavior_signature = sig
            c.confidence = conf
            clusters.append(c)
            self.last_closed_price = w_last_price

            total_bid = total_ask = cumdelta = deltamin = deltamax = 0
            w_start_ns = w_first_price = w_last_price = w_ts = None

        for e in tape:
            e_ns = e.raw.get("timestamp_ns")
            vol  = e.volume
            side = e.side.value

            if e_ns is not None:
                if w_start_ns is None:
                    w_start_ns    = e_ns
                    w_first_price = e.price
                    w_ts          = e.timestamp
                elif e_ns - w_start_ns >= _WINDOW_NS:
                    _flush(e_ns - 1)
                    w_start_ns    = e_ns
                    w_first_price = e.price
                    w_ts          = e.timestamp
            else:
                if w_ts is not None:
                    _flush(0)
                w_start_ns    = 0
                w_first_price = e.price
                w_ts          = e.timestamp

            if side == "buy":
                total_bid += vol
                cumdelta  += vol
            else:
                total_ask += vol
                cumdelta  -= vol
            deltamin     = min(deltamin, cumdelta)
            deltamax     = max(deltamax, cumdelta)
            w_last_price = e.price

        if w_ts is not None:
            last_ns = tape[-1].raw.get("timestamp_ns") or 0
            _flush(last_ns)

        return clusters

    # ── ingest_batch ───────────────────────────────────────────────────────────────────────────

    def ingest_batch(
        self,
        tape_rows: List[Dict],
        dom_rows:  List[Dict],
        symbol:    str,
        top_n:     int = 5,
    ) -> List[LiquidityCluster]:
        """
        Processa um batch de eventos T&S + DOM.

        top_n: número de níveis do Book a considerar no snapshot DOM.
               Default=5. Use top_n=10 para capturar Icebergs em níveis
               mais profundos (6-10) sem custo computacional proibitivo.
        """
        tape = parse_tape_rows(tape_rows, symbol)
        dom  = parse_dom_rows(dom_rows, symbol)

        if tape_rows and not tape:
            logging.warning(
                "[ingest_batch] %d tape_rows sem parse (symbol=%s) — payload malformado.",
                len(tape_rows), symbol,
            )
            return []
        if not tape:
            return []
        if dom_rows and not dom:
            logging.warning(
                "[ingest_batch] %d dom_rows sem parse (symbol=%s) — DOM offline.",
                len(dom_rows), symbol,
            )

        batch_id = str(time.time_ns())

        dom_index = self._build_dom_index(dom_rows, self.cfg.tick_size, top_n=top_n)

        clusters = self._build_clusters_from_windows(tape, dom_index, symbol, batch_id)

        self.repo.begin()
        try:
            self.repo.insert_tape_events(tape)
            self.repo.insert_dom_levels(dom)
            self.repo.insert_clusters(clusters)
            self.repo.commit()
        except Exception:
            self.repo.rollback()
            raise

        snap = self.matrix.snapshot()
        try:
            self.matrix.build_from_events(tape, dom, clusters=clusters)

            original_signatures: dict[int, str] = {
                id(c): c.behavior_signature.value for c in clusters
            }
            current_batch_id = clusters[0].batch_id if clusters else None

            self._batch_counter = getattr(self, "_batch_counter", 0) + 1
            if self._batch_counter % 10 == 0:
                hotspots = self.matrix.hotspots(self.cfg.min_occurrences)
                for h in hotspots:
                    level_clusters = self.matrix.active_levels.get(h["price"], [])
                    refined = self.engine.post_classify(h["price"], level_clusters)
                    if current_batch_id:
                        for c in level_clusters:
                            if c.batch_id == current_batch_id:
                                c.behavior_signature = refined

            upgraded = [
                c for c in clusters
                if c.behavior_signature.value != original_signatures.get(id(c))
            ]
            if upgraded:
                self.repo.begin()
                try:
                    self.repo.conn.executemany(
                        """UPDATE liquidity_clusters
                           SET behavior_signature = ?
                           WHERE symbol = ? AND timestamp = ? AND price = ? AND batch_id = ?""",
                        [
                            (c.behavior_signature.value, c.symbol,
                             c.timestamp, c.price, c.batch_id)
                            for c in upgraded
                        ],
                    )
                    self.repo.commit()
                except Exception:
                    self.repo.rollback()
                    raise
        except Exception:
            self.matrix.restore(snap)
            raise

        if self.narrator is not None:
            self.narrator.invalidate_cache()

        return clusters
