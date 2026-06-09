"""
backtest_runner.py
------------------
Executa backtest do pipeline 6J Watcher com dados históricos do Databento.
"""
from __future__ import annotations
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

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


def _estimate_total_batches(file_path: Path, batch_size_seconds: int) -> int:
    try:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        batches_per_mb = 8.5 * (60 / batch_size_seconds)
        return max(100, int(size_mb * batches_per_mb))
    except Exception:
        return 1000


def _parse_market_ts(timestamp_str: str) -> Optional[datetime]:
    """Parseia timestamp de tape_row para datetime UTC. Retorna None em caso de falha."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(timestamp_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


class BacktestRunner:
    def __init__(
        self,
        api_key: str,
        db_path: str = "./data/backtest.db",
        profile_path: str = "./data/backtest_profile.json",
        batch_size_seconds: int = 300,
        skip_dom: bool = False,
        skip_profiler: bool = False,
    ):
        self.api_key = api_key
        self.db_path = db_path
        self.profile_path = profile_path
        self.skip_dom = skip_dom
        self.skip_profiler = skip_profiler
        self.batch_size_seconds = batch_size_seconds

        self.loader  = DatabentoLoader(api_key)
        self.adapter = DatabentoAdapter(self.loader, batch_size_seconds=batch_size_seconds)

        self.cfg = Config()
        self.cfg.db_path = db_path

        self.repo   = DuckDBRepository(db_path)
        self.matrix = LiquidityMatrix(self.cfg.symbol, self.cfg.tick_size)
        self.engine = AdaptivePatternEngine(profile_path=profile_path, cfg=self.cfg)
        self.service = IngestionService(
            repo=self.repo, matrix=self.matrix, engine=self.engine, cfg=self.cfg,
            narrator=Narrator(engine=self.engine, cfg=self.cfg)
        )
        self.narrator = self.service.narrator

        self.metrics: Dict = {
            "total_batches": 0, "total_tape_events": 0, "total_dom_levels": 0,
            "total_clusters": 0, "signature_counts": {}, "hotspots": [],
            "processing_time_seconds": 0.0, "report": "",
        }

        # Timestamp de mercado do último batch processado — usado por prune e CHECKPOINT
        self._last_market_ts: Optional[datetime] = None

    def run(self, start: date, end: date, symbol: str = "6J", skip_download: bool = False) -> Dict:
        self.metrics = {
            "total_batches": 0, "total_tape_events": 0, "total_dom_levels": 0,
            "total_clusters": 0, "signature_counts": {}, "hotspots": [],
            "processing_time_seconds": 0.0, "report": "",
        }
        logger.info("=== Backtest iniciado: %s -> %s ===", start, end)
        wall_start = time.time()

        file_path = self._resolve_file(start, end, symbol, skip_download)
        estimated_batches = _estimate_total_batches(file_path, self.batch_size_seconds)
        label = f"{start.strftime('%b/%Y')}"

        if TQDM_AVAILABLE:
            pbar = tqdm(
                total=estimated_batches, desc=f"  {label}", unit="batch",
                dynamic_ncols=True,
                ascii=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] clusters:{postfix}",
                file=sys.stderr,
            )
        else:
            pbar = None
            logger.warning("tqdm nao instalado. Execute: pip install tqdm")

        for tape_rows, dom_rows in self.adapter.stream_batches(file_path, skip_dom=self.skip_dom):
            # dom_rows agora entra no pipeline quando skip_dom=False
            clusters = self.service.ingest_batch(tape_rows, dom_rows, symbol)

            self.metrics["total_batches"]     += 1
            self.metrics["total_tape_events"] += len(tape_rows)
            self.metrics["total_dom_levels"]  += len(dom_rows)
            self.metrics["total_clusters"]    += len(clusters)

            for c in clusters:
                sig = c.behavior_signature.value
                self.metrics["signature_counts"][sig] = self.metrics["signature_counts"].get(sig, 0) + 1

            # Atualiza timestamp de mercado a partir do último tape event do batch
            if tape_rows:
                ts = _parse_market_ts(tape_rows[-1].get("timestamp", ""))
                if ts:
                    self._last_market_ts = ts

            if pbar:
                pbar.set_postfix_str(str(self.metrics["total_clusters"]))
                pbar.update(1)
            elif self.metrics["total_batches"] % 100 == 0:
                elapsed = time.time() - wall_start
                logger.info("Batch %d | %.1f batches/s | clusters: %d",
                    self.metrics["total_batches"], self.metrics["total_batches"] / elapsed,
                    self.metrics["total_clusters"])

            # CHECKPOINT + prune a cada 500 batches
            # prune usa tempo de mercado do batch — evita amnesia total no backtest
            if self.metrics["total_batches"] % 500 == 0:
                self.repo.conn.execute("CHECKPOINT")
                if self._last_market_ts:
                    self.matrix.prune_stale_data(hours=4, reference_time=self._last_market_ts)
                    logger.info("[prune] Matriz podada @ %s", self._last_market_ts.isoformat())

        if pbar:
            pbar.n = pbar.total
            pbar.refresh()
            pbar.close()

        elapsed_s = time.time() - wall_start
        logger.info("=== Mes %s stream OK em %.1fs | %d batches | %d clusters ===",
            label, elapsed_s, self.metrics["total_batches"], self.metrics["total_clusters"])

        self.repo.commit()

        if self.skip_profiler:
            logger.info("[Profiler] skip_profiler=True — pulando calibragem MFE/MAE.")
        else:
            logger.info("[Profiler] Iniciando calibragem (pode levar alguns minutos com muitos dados)...")
            t_prof = time.time()
            try:
                profiler = SignatureProfiler(self.db_path, cfg=self.cfg, conn=self.repo.conn)
                lookback_days = (end - start).days + 5
                profile = profiler.build_profile(
                    symbol, lookback_days=lookback_days,
                    horizon_minutes=30, since=str(start)
                )
                profiler.save_profile(profile, self.profile_path)
                logger.info("[Profiler] Calibragem concluida em %.1fs", time.time() - t_prof)
            except Exception:
                logger.exception("[Profiler] Falha na calibragem — continuando sem atualizar profile:")

        hotspots      = self.matrix.hotspots(min_occurrences=self.cfg.min_occurrences)
        sig_dist      = self.repo.signature_distribution(symbol)
        sess_analysis = self.repo.session_analysis(symbol)
        report        = self.narrator.daily_report(symbol, hotspots, sig_dist, sess_analysis)

        self.metrics["processing_time_seconds"] = time.time() - wall_start
        self.metrics["hotspots"] = hotspots[:20]
        self.metrics["report"]   = report
        return self.metrics

    def save_report(self, output_path: str = "./data/backtest_report.md") -> None:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(self.metrics.get("report", "Sem relatorio disponivel."))
        logger.info("Relatorio salvo em %s", output_path)

    def _resolve_file(self, start: date, end: date, symbol: str, skip_download: bool) -> Path:
        databento_symbol = f"{symbol}.n.0"
        if skip_download:
            pattern = f"{databento_symbol}_{start}_{end}_mbp-10.dbn.zst"
            matches = list(self.loader.cache_dir.glob(pattern))
            if not matches:
                raise FileNotFoundError(
                    f"Cache nao encontrado: {self.loader.cache_dir / pattern}")
            return matches[0]
        return self.loader.download(start, end, symbol=databento_symbol)
