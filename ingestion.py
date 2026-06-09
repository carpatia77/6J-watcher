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

_WINDOW_NS = 250_000_000   # 250ms em nanosegundos
_BUCKET_NS  = 250_000_000  # alias semântico para a query SQL


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
                (timestamp_ns / $bucket_ns) AS bucket_id
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
            ASOF LEFT JOIN dom_levels d
                ON  d.symbol       = $symbol
                AND d.timestamp_ns <= w.w_end_ns
                AND d.price BETWEEN w.last_price - $tick_range
                                AND w.last_price + $tick_range
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
            "bucket_ns": _BUCKET_NS,
            "tick_range": tick_range,
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
