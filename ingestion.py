from __future__ import annotations
"""
ingestion.py
------------
Orquestra o pipeline completo de ingestao:
  1. Parse T&S e DOM
  2. Persiste no DuckDB
  3. Alimenta a LiquidityMatrix
  4. Classifica assinatura comportamental
  5. Devolve clusters gerados

Dual-path:
  PATH A (backtest) — tape_rows contém 'timestamp_ns'
                      → SQL window aggregation + classify_batch()
  PATH B (produção) — tape_rows sem 'timestamp_ns' (MQL5)
                      → loop Python original (inalterado)
"""
import bisect
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

from config import Config
from models import BehaviorSignature, LiquidityCluster
from parser_tsdom import parse_tape_rows, parse_dom_rows
from adaptive_pattern_engine import AdaptivePatternEngine
from liquidity_matrix import LiquidityMatrix
from repository_duckdb import DuckDBRepository




class IngestionService:
    def __init__(self, repo: DuckDBRepository, matrix: LiquidityMatrix,
                 engine: AdaptivePatternEngine, cfg: Config, narrator=None):
        self.repo     = repo
        self.matrix   = matrix
        self.engine   = engine
        self.cfg      = cfg
        self.narrator = narrator
        self._batch_counter = 0

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

    # =========================================================================
    # PATH A — SQL window aggregation (backtest)
    # =========================================================================

    def _build_clusters_sql(
        self,
        symbol: str,
        batch_id: str,
    ) -> List[LiquidityCluster]:
        """
        Substitui _build_clusters_from_windows() no path de backtest.

        Os dados já estão em tape_events e dom_levels (inseridos via
        bulk_insert_arrow antes desta chamada). A query usa 3 CTEs:

          tape_ordered   — delta por evento (+vol buy / -vol sell)
          running_calcs  — SUM(delta) OVER window (estado cumulativo puro)
          windowed       — GROUP BY bucket, MIN/MAX sobre running_delta
                           (evita o paradoxo GROUP BY + window do DuckDB)
          dom_joined     — ASOF LEFT JOIN em timestamp_ns

        Retorna LiquidityCluster[] prontos para classify_batch().
        """
        tick_size  = self.cfg.tick_size
        tick_range = tick_size * 6  # janela de preço para ASOF JOIN DOM (±6 ticks)

        sql = """
        WITH tape_ordered AS (
            SELECT
                timestamp_ns,
                timestamp,
                price,
                volume,
                side,
                CASE WHEN side = 'buy' THEN volume ELSE -volume END AS delta,
                (timestamp_ns // $bucket_ns) AS bucket_id
            FROM tape_events
            WHERE symbol = $symbol AND batch_id = $batch_id
        ),
        running_calcs AS (
            SELECT
                bucket_id,
                timestamp_ns,
                timestamp,
                price,
                volume,
                side,
                SUM(delta) OVER (
                    PARTITION BY bucket_id
                    ORDER BY timestamp_ns
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_delta
            FROM tape_ordered
        ),
        windowed AS (
            SELECT
                bucket_id,
                MIN(timestamp)                                     AS w_timestamp,
                MIN(timestamp_ns)                                  AS w_start_ns,
                MAX(timestamp_ns)                                  AS w_end_ns,
                FIRST(price ORDER BY timestamp_ns)                 AS first_price,
                LAST(price  ORDER BY timestamp_ns)                 AS last_price,
                SUM(CASE WHEN side = 'buy'  THEN volume ELSE 0 END) AS total_bid,
                SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) AS total_ask,
                LAST(running_delta ORDER BY timestamp_ns)           AS cumdelta,
                MIN(running_delta)                                  AS deltamin,
                MAX(running_delta)                                  AS deltamax
            FROM running_calcs
            GROUP BY bucket_id
        ),
        dom_joined AS (
            SELECT
                w.*,
                COALESCE(d.bid_volume, 0) AS dom_bid,
                COALESCE(d.ask_volume, 0) AS dom_ask
            FROM windowed w
            ASOF LEFT JOIN (
                SELECT * FROM dom_levels 
                WHERE symbol = $symbol AND batch_id = $batch_id
            ) d
                ON  d.price        = w.last_price
                AND d.timestamp_ns <= w.w_end_ns
        )
        SELECT
            w_timestamp,
            w_start_ns,
            first_price,
            last_price,
            total_bid,
            total_ask,
            cumdelta,
            deltamin,
            deltamax,
            dom_bid,
            dom_ask
        FROM dom_joined
        ORDER BY w_start_ns
        """

        rows = self.repo.conn.execute(sql, {
            "symbol":    symbol,
            "batch_id":  batch_id,
            "bucket_ns": self.cfg.window_ns,
        }).fetchall()

        if not rows:
            return []

        clusters: List[LiquidityCluster] = []
        for row in rows:
            (
                w_ts, w_start_ns, first_price, last_price,
                total_bid, total_ask, cumdelta, deltamin, deltamax,
                dom_bid, dom_ask
            ) = row

            # w_ts pode chegar como string (DuckDB → Python) ou datetime
            if isinstance(w_ts, str):
                w_ts = datetime.fromisoformat(w_ts)

            dp = round((last_price - first_price) / self.cfg.tick_size)

            c = LiquidityCluster(
                symbol    = symbol,
                timestamp = w_ts,
                price     = last_price,
                session   = self.cfg.session_for(w_ts.hour),
                total_bid = int(total_bid),
                total_ask = int(total_ask),
                cumdelta  = int(cumdelta),
                deltamin  = int(deltamin),
                deltamax  = int(deltamax),
                delta_price_ticks = int(dp),
                batch_id  = batch_id,
                raw_payload = {
                    "window_ns":    w_start_ns,
                    "timestamp_ns": w_start_ns,
                    "dom_bid":      int(dom_bid),
                    "dom_ask":      int(dom_ask),
                },
            )
            clusters.append(c)

        # Fase 3 — classify_batch() vetorizado
        if _PANDAS_AVAILABLE and clusters:
            self._classify_clusters_batch(clusters)
        else:
            # fallback escalar (pandas não instalado)
            for c in clusters:
                sig, conf = self.engine.classify(c)
                c.behavior_signature = sig
                c.confidence = conf

        if clusters:
            self.last_closed_price = clusters[-1].price

        return clusters

    def _classify_clusters_batch(self, clusters: List[LiquidityCluster]) -> None:
        """
        Fase 3 — classify_batch() vetorizado.

        Agrupa clusters por sessão (evita mistura de thresholds) e
        chama engine.classify_batch() por grupo. Escreve behavior_signature
        e confidence de volta nos objetos in-place.

        delta_price_ticks é coerced para int16 ANTES de passar ao engine
        para garantir SIMD sem coerção float64 implícita.
        """
        from collections import defaultdict
        session_groups: Dict[str, List[int]] = defaultdict(list)
        for i, c in enumerate(clusters):
            session_groups[c.session].append(i)

        for session, indices in session_groups.items():
            group = [clusters[i] for i in indices]
            df = pd.DataFrame({
                "total_bid":         pd.array([c.total_bid         for c in group], dtype="int32"),
                "total_ask":         pd.array([c.total_ask         for c in group], dtype="int32"),
                "cumdelta":          pd.array([c.cumdelta          for c in group], dtype="int32"),
                "deltamin":          pd.array([c.deltamin          for c in group], dtype="int32"),
                "deltamax":          pd.array([c.deltamax          for c in group], dtype="int32"),
                # int16 evita coerção float64 no .abs() <= 2 (per review)
                "delta_price_ticks": pd.array([c.delta_price_ticks for c in group], dtype="int16"),
                "dom_bid":           pd.array([c.raw_payload.get("dom_bid", 0) for c in group], dtype="int32"),
                "dom_ask":           pd.array([c.raw_payload.get("dom_ask", 0) for c in group], dtype="int32"),
            })
            sigs, confs = self.engine.classify_batch(df, session)
            for i, (sig_val, conf) in enumerate(zip(sigs, confs)):
                clusters[indices[i]].behavior_signature = BehaviorSignature(sig_val)
                clusters[indices[i]].confidence = conf

    # =========================================================================
    # PATH A — DOM index (mantido para fallback e testes)
    # =========================================================================

    @staticmethod
    def _build_dom_index(
        dom_rows: List[Dict],
        tick_size: float,
        top_n: int = 5,
    ) -> Dict[int, List[Tuple[int, int, int]]]:
        if not dom_rows or tick_size <= 0:
            return {}
        acc: Dict[Tuple[int, int], Tuple[int, int]] = {}
        for row in dom_rows:
            ts_ns = row.get("timestamp_ns")
            if ts_ns is None:
                continue
            if row.get("level_index", top_n) >= top_n:
                continue
            pk  = round(row.get("price", 0.0) / tick_size)
            key = (pk, ts_ns)
            b, a = acc.get(key, (0, 0))
            acc[key] = (b + row.get("bid_volume", 0),
                        a + row.get("ask_volume", 0))
        grouped: Dict[int, List[Tuple[int, int, int]]] = defaultdict(list)
        for (pk, ts_ns), (b, a) in acc.items():
            grouped[pk].append((ts_ns, b, a))
        index: Dict[int, List[Tuple[int, int, int]]] = {}
        for pk, entries in grouped.items():
            entries.sort()
            index[pk] = entries
        return index

    def _dom_at(
        self,
        dom_index: Dict[int, List[Tuple[int, int, int]]],
        price: float,
        end_ns: int,
    ) -> Tuple[int, int]:
        pk      = round(price / self.cfg.tick_size)
        entries = dom_index.get(pk)
        if not entries:
            return (0, 0)

        lo, hi = 0, len(entries)
        while lo < hi:
            mid = (lo + hi) // 2
            if entries[mid][0] <= end_ns:
                lo = mid + 1
            else:
                hi = mid
        pos = lo - 1
        if pos < 0:
            return (0, 0)
        _, b, a = entries[pos]
        return (b, a)

    # -- Micro-agregacao em janelas de 250ms ---------------------------------------

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
          - total_bid / total_ask acumulam ambos os lados  ->  imb != vol
          - delta_price_ticks = (last_price - first_price) / tick_size
          - cumdelta / deltamin / deltamax via CVD incremental
          - dom_bid / dom_ask injetados via _dom_at() no fechamento

        Fallback para producao MQL5 sem timestamp_ns:
          Cada TapeEvent gera sua propria janela (sem regressao).
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
            dom_bid, dom_ask = self._dom_at(dom_index, w_last_price, end_ns)

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
                raw_payload = {
                    "window_ns":    w_start_ns,
                    "timestamp_ns": w_start_ns,
                    "dom_bid":      dom_bid,
                    "dom_ask":      dom_ask,
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

    # -- ingest_batch --------------------------------------------------------------


    def ingest_batch(
        self,
        tape_rows: List[Dict],
        dom_rows:  List[Dict],
        symbol:    str,
        batch_id:  Optional[str] = None,
        top_n:     int = 5,
        is_sql_path: bool = False,
    ) -> List[LiquidityCluster]:
        if not batch_id:
            batch_id = str(time.time_ns())

        is_sql_path = is_sql_path or bool(tape_rows and "timestamp_ns" in tape_rows[0])

        if is_sql_path:
            clusters = self._build_clusters_sql(symbol, batch_id)
            tape = []
            dom  = []
            # Na fase SQL os dados ja estao inseridos no DuckDB via Arrow bulk_insert.
            # E pular o parse iterativo de milhoes de eventos salva >2h de tempo de execucao.
        else:
            tape = parse_tape_rows(tape_rows, symbol)
            dom  = parse_dom_rows(dom_rows, symbol)

            if tape_rows and not tape:
                logging.warning("[ingest_batch] %d tape_rows sem parse", len(tape_rows))
                return []
            if not tape:
                return []

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
                        "UPDATE liquidity_clusters SET behavior_signature = ? WHERE symbol = ? AND timestamp = ? AND price = ? AND batch_id = ?",
                        [
                            (c.behavior_signature.value, c.symbol, c.timestamp, c.price, c.batch_id)
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
