import os
import sys
import unittest
import pyarrow as pa
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from repository_duckdb import DuckDBRepository
from liquidity_matrix import LiquidityMatrix
from adaptive_pattern_engine import AdaptivePatternEngine
from ingestion import IngestionService
from lab_vector import _vectorize, WINDOW_NS

class TestVectorSQL(unittest.TestCase):
    def setUp(self):
        self.db_path = ":memory:"
        self.repo = DuckDBRepository(self.db_path)
        self.cfg = Config()
        self.cfg.symbol = "TEST"
        self.matrix = LiquidityMatrix(self.cfg.symbol, self.cfg.tick_size)
        self.engine = AdaptivePatternEngine(cfg=self.cfg)
        self.service = IngestionService(self.repo, self.matrix, self.engine, self.cfg)

    def test_sql_vs_pandas_cumdelta(self):
        # 1. Dados de Validacao Visual
        raw_tape_small = [
            {"timestamp_ns": 100_000_000, "timestamp": "2026-06-09T10:00:00.100000", "price": 10.0, "volume": 5,  "side": "buy"},
            {"timestamp_ns": 200_000_000, "timestamp": "2026-06-09T10:00:00.200000", "price": 10.5, "volume": 10, "side": "buy"},
            {"timestamp_ns": 350_000_000, "timestamp": "2026-06-09T10:00:00.350000", "price": 11.0, "volume": 3,  "side": "sell"},
            {"timestamp_ns": 400_000_000, "timestamp": "2026-06-09T10:00:00.400000", "price": 10.0, "volume": 7,  "side": "sell"},
            {"timestamp_ns": 450_000_000, "timestamp": "2026-06-09T10:00:00.450000", "price": 9.5,  "volume": 2,  "side": "buy"},
        ]

        # 2. Roda Pandas Vetorizado (lab_vector)
        df_clusters = _vectorize(raw_tape_small, WINDOW_NS)

        # 3. Roda SQL Vetorizado (ingestion)
        # Prepara Arrow batches para bulk_insert_arrow
        tape_rb = pa.record_batch({
            "timestamp_ns": pa.array([e["timestamp_ns"] for e in raw_tape_small], type=pa.int64()),
            "timestamp":    pa.array([e["timestamp"] for e in raw_tape_small],    type=pa.string()),
            "price":        pa.array([e["price"] for e in raw_tape_small],        type=pa.float64()),
            "volume":       pa.array([e["volume"] for e in raw_tape_small],       type=pa.int32()),
            "side":         pa.array([e["side"] for e in raw_tape_small],         type=pa.string()),
        })
        dom_rb = pa.record_batch({
            "timestamp_ns": pa.array([], type=pa.int64()),
            "timestamp":    pa.array([], type=pa.string()),
            "price":        pa.array([], type=pa.float64()),
            "level_index":  pa.array([], type=pa.int32()),
            "bid_volume":   pa.array([], type=pa.int32()),
            "ask_volume":   pa.array([], type=pa.int32()),
        })

        batch_id = "test_batch_01"
        self.repo.bulk_insert_arrow(self.cfg.symbol, batch_id, tape_rb, dom_rb)

        sql_clusters = self.service._build_clusters_sql(self.cfg.symbol, batch_id)

        # 4. Compara Resultados
        self.assertEqual(len(sql_clusters), len(df_clusters), "Diferenca no numero de clusters gerados")

        for idx, sql_c in enumerate(sql_clusters):
            # df_clusters iterrows usa o indice df que comeca em 0, mas pandas as vezes preserva chaves.
            # window_id no DataFrame eh o indice. df_clusters.iloc[idx]
            pd_c = df_clusters.iloc[idx]
            
            print(f"\\n--- Window {idx} ---")
            print(f"SQL    : bid_vol={sql_c.total_bid}, ask_vol={sql_c.total_ask}, delta_min={sql_c.deltamin}, delta_max={sql_c.deltamax}, cumdelta={sql_c.cumdelta}, delta_price={sql_c.delta_price_ticks}")
            print(f"Pandas : bid_vol={pd_c['bid_vol_sum']}, ask_vol={pd_c['ask_vol_sum']}, delta_min={pd_c['cumdelta_min']}, delta_max={pd_c['cumdelta_max']}, cumdelta={pd_c['cumdelta_last']}, delta_price={pd_c['delta_price']}")
            
            self.assertEqual(sql_c.total_bid, pd_c['bid_vol_sum'], f"Window {idx}: total_bid diverge")
            self.assertEqual(sql_c.total_ask, pd_c['ask_vol_sum'], f"Window {idx}: total_ask diverge")
            self.assertEqual(sql_c.deltamin, pd_c['cumdelta_min'], f"Window {idx}: deltamin diverge")
            self.assertEqual(sql_c.deltamax, pd_c['cumdelta_max'], f"Window {idx}: deltamax diverge")
            self.assertEqual(sql_c.cumdelta, pd_c['cumdelta_last'], f"Window {idx}: cumdelta diverge")
            self.assertEqual(sql_c.delta_price_ticks * self.cfg.tick_size, pd_c['delta_price'], f"Window {idx}: delta_price diverge")

if __name__ == '__main__':
    unittest.main()
