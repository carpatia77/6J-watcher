"""
backtest_runner.py
------------------
Executa backtest do pipeline 6J Watcher com dados históricos do Databento.
"""
from __future__ import annotations
import sys
import os

# Garante que os módulos da raiz do projeto são encontrados
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from config import Config
from repository_duckdb import DuckDBRepository
from liquidity_matrix import LiquidityMatrix
from adaptive_pattern_engine import AdaptivePatternEngine
from ingestion import IngestionService
from signature_profiler import SignatureProfiler
from narrator import Narrator

from backtest.databento_loader import DatabentoLoader
from backtest.adapter import DatabentoAdapter

logger = logging.getLogger(__name__)


class BacktestRunner:
    """
    Executa backtest completo:
    1. Baixa dados do Databento (MBP-10, CME Globex)
    2. Alimenta o pipeline via IngestionService.ingest_batch()
    3. Roda SignatureProfiler ao final para calibrar thresholds
    4. Gera relatório narrativo via Narrator
    """

    def __init__(
        self,
        api_key: str,
        db_path: str = "./data/backtest.db",
        profile_path: str = "./data/backtest_profile.json",
        batch_size_seconds: int = 300,
        skip_dom: bool = False,
    ):
        self.api_key = api_key
        self.db_path = db_path
        self.profile_path = profile_path
        self.skip_dom = skip_dom

        # Loader e adapter Databento
        self.loader = DatabentoLoader(api_key)
        self.adapter = DatabentoAdapter(self.loader, batch_size_seconds=batch_size_seconds)

        # Pipeline de produção — mesmo código do main.py, DB isolado
        self.cfg = Config()
        self.cfg.db_path = db_path

        self.repo   = DuckDBRepository(db_path)
        self.matrix = LiquidityMatrix(self.cfg.symbol, self.cfg.tick_size)
        self.engine = AdaptivePatternEngine(
            profile_path=profile_path,
            cfg=self.cfg,
        )
        self.service = IngestionService(
            repo=self.repo,
            matrix=self.matrix,
            engine=self.engine,
            cfg=self.cfg,
            narrator=Narrator(engine=self.engine, cfg=self.cfg)
        )
        self.narrator = self.service.narrator

        # Métricas acumuladas ao longo do run
        self.metrics: Dict = {
            "total_batches": 0,
            "total_tape_events": 0,
            "total_dom_levels": 0,
            "total_clusters": 0,
            "signature_counts": {},
            "hotspots": [],
            "processing_time_seconds": 0.0,
            "report": "",
        }

        import atexit
        atexit.register(self._cleanup)

    def _cleanup(self):
        """Garante fechamento limpo mesmo em crash — libera file lock no Windows."""
        try:
            self.repo.conn.execute("CHECKPOINT")
            self.repo.conn.close()
            logger.info("[BacktestRunner] Conexão DuckDB fechada via atexit.")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def run(
        self,
        start: date,
        end: date,
        symbol: str = "6J",
        skip_download: bool = False,
    ) -> Dict:
        """
        Executa backtest completo.

        Args:
            start:          Data inicial (inclusive)
            end:            Data final (inclusive)
            symbol:         Símbolo base (ex: "6J"); o loader usa "6J.n.0" internamente
            skip_download:  Se True, usa cache .dbn.zst existente (evita cobranças)

        Returns:
            Dict com métricas consolidadas do backtest
        """
        logger.info("=== Backtest iniciado: %s → %s ===", start, end)
        wall_start = time.time()

        file_path = self._resolve_file(start, end, symbol, skip_download)

        # Stream de batches → pipeline
        for tape_rows, _ in self.adapter.stream_batches(file_path, skip_dom=self.skip_dom):
            clusters = self.service.ingest_batch(tape_rows, [], symbol)

            self.metrics["total_batches"]     += 1
            self.metrics["total_tape_events"] += len(tape_rows)
            self.metrics["total_dom_levels"]  += 0
            self.metrics["total_clusters"]    += len(clusters)

            for c in clusters:
                sig = c.behavior_signature.value
                self.metrics["signature_counts"][sig] = (
                    self.metrics["signature_counts"].get(sig, 0) + 1
                )

            if self.metrics["total_batches"] % 100 == 0:
                elapsed = time.time() - wall_start
                rate = self.metrics["total_batches"] / elapsed
                logger.info(
                    "Batch %d | %.1f batches/s | clusters: %d",
                    self.metrics["total_batches"],
                    rate,
                    self.metrics["total_clusters"],
                )

        # Calibra thresholds com dados persistidos
        logger.info("Calibrando SignatureProfiler...")
        profiler = SignatureProfiler(self.db_path, cfg=self.cfg, conn=self.repo.conn)
        lookback_days = (end - start).days + 5
        profile  = profiler.build_profile(symbol, lookback_days=lookback_days, horizon_minutes=30, since=str(start))
        profiler.save_profile(profile, self.profile_path)

        # Hotspots e relatório narrativo
        hotspots     = self.matrix.hotspots(min_occurrences=self.cfg.min_occurrences)
        sig_dist     = self.repo.signature_distribution(symbol)
        sess_analysis = self.repo.session_analysis(symbol)
        report       = self.narrator.daily_report(symbol, hotspots, sig_dist, sess_analysis)

        self.metrics["processing_time_seconds"] = time.time() - wall_start
        self.metrics["hotspots"] = hotspots[:20]
        self.metrics["report"]   = report

        logger.info(
            "=== Backtest concluído em %.1fs | %d batches | %d clusters ===",
            self.metrics["processing_time_seconds"],
            self.metrics["total_batches"],
            self.metrics["total_clusters"],
        )
        return self.metrics

    def save_report(self, output_path: str = "./data/backtest_report.md") -> None:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(self.metrics.get("report", "Sem relatório disponível."))
        logger.info("Relatório salvo em %s", output_path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_file(self, start: date, end: date, symbol: str, skip_download: bool) -> Path:
        databento_symbol = f"{symbol}.n.0"  # continuous future
        if skip_download:
            pattern = f"{databento_symbol}_{start}_{end}_mbp-10.dbn.zst"
            matches = list(self.loader.cache_dir.glob(pattern))
            if not matches:
                raise FileNotFoundError(
                    f"Cache não encontrado: {self.loader.cache_dir / pattern}\n"
                    "Rode sem skip_download=True primeiro."
                )
            return matches[0]
        return self.loader.download(start, end, symbol=databento_symbol)
