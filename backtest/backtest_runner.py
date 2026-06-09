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


# ────────────────────────────────────────────────────────────────────────
# BacktestPhaseProfiler
# ────────────────────────────────────────────────────────────────────────

class BacktestPhaseProfiler:
    """
    Mede o tempo de cada fase do hot-path por batch e acumula estatísticas
    para guiar a decisão de vetorização.

    4 fases instrumentadas:
      stream   — DatabentoAdapter.stream_batches() (I/O + parse .dbn.zst)
      ingest   — IngestionService.ingest_batch() Python loop
                 (_build_dom_index + _build_clusters_from_windows)
      classify — engine.classify() dentro do ingest (separado via hook)
      persist  — repo.insert_* + DuckDB commit

    Nota: classify está embutido dentro do ingest na implementação atual.
    O profiler estima o custo relativo via amostragem a cada 50 batches.
    """

    PHASES = ("stream", "ingest", "persist", "profiler", "other")

    def __init__(self):
        self._t: Dict[str, float] = {p: 0.0 for p in self.PHASES}
        self._counts: Dict[str, int] = {p: 0 for p in self.PHASES}
        self._n_batches = 0
        self._n_events  = 0
        self._n_dom     = 0
        self._n_clusters = 0
        self._wall_start: Optional[float] = None

    def start_run(self):
        self._wall_start = time.perf_counter()

    def record(self, phase: str, elapsed: float, count: int = 1):
        if phase in self._t:
            self._t[phase]      += elapsed
            self._counts[phase] += count

    def tick_batch(self, n_tape: int, n_dom: int, n_clusters: int):
        self._n_batches  += 1
        self._n_events   += n_tape
        self._n_dom      += n_dom
        self._n_clusters += n_clusters

    def total_wall(self) -> float:
        if self._wall_start is None:
            return 0.0
        return time.perf_counter() - self._wall_start

    def phase_totals(self) -> Dict[str, float]:
        """Retorna segundos por fase e 'other' (overhead não instrumentado)."""
        instrumented = sum(self._t[p] for p in self.PHASES if p != "other")
        wall = self.total_wall()
        result = {p: self._t[p] for p in self.PHASES}
        result["other"] = max(0.0, wall - instrumented)
        return result

    def summary(self, chunk_label: str, total_chunks: int) -> str:
        """
        Gera o bloco de texto que vai para o log após cada chunk.
        Inclui breakdown de fases, throughput, e projeção para 8 meses.
        """
        wall   = self.total_wall()
        phases = self.phase_totals()
        total_p = sum(phases.values()) or 1.0

        lines = []
        lines.append("")
        lines.append("=" * 62)
        lines.append(f"  PHASE PROFILER — {chunk_label}")
        lines.append("=" * 62)
        lines.append(f"  Batches  : {self._n_batches:>8,}")
        lines.append(f"  Tape evts: {self._n_events:>8,}")
        lines.append(f"  DOM rows : {self._n_dom:>8,}")
        lines.append(f"  Clusters : {self._n_clusters:>8,}")
        lines.append(f"  Wall time: {wall:>8.1f}s  ({wall/3600:.2f}h)")
        lines.append("")
        lines.append("  Fase              Segundos     %      Nota")
        lines.append("  " + "-" * 58)

        phase_notes = {
            "stream":   "I/O + parse .dbn.zst",
            "ingest":   "Python loop (HOT-PATH)",
            "persist":  "DuckDB insert + commit",
            "profiler": "SignatureProfiler SQL",
            "other":    "overhead/tqdm/logging",
        }
        hot_path_pct = 0.0
        for p in self.PHASES:
            s = phases[p]
            pct = s / total_p * 100
            note = phase_notes.get(p, "")
            marker = " <<< HOT-PATH" if p == "ingest" else ""
            lines.append(f"  {p:<16}  {s:>8.1f}s  {pct:>5.1f}%   {note}{marker}")
            if p == "ingest":
                hot_path_pct = pct

        # Throughput
        if wall > 0:
            tp_events   = self._n_events   / wall
            tp_clusters = self._n_clusters / wall
            lines.append("")
            lines.append(f"  Throughput: {tp_events:>8,.0f} tape-events/s")
            lines.append(f"             {tp_clusters:>8,.0f} clusters/s")

        # Projeção para os chunks restantes
        remaining = total_chunks - 1   # chunks restantes após este
        proj_h = (wall / 3600) * remaining
        lines.append("")
        lines.append(f"  Projeção restante ({remaining} chunks): ~{proj_h:.1f}h")
        lines.append(f"  Projeção total    ({total_chunks} chunks): ~{wall/3600 * total_chunks:.1f}h")

        # Recomendação de vetorização (só após chunk 1)
        lines.append("")
        if hot_path_pct >= 60.0:
            lines.append("  [VECTORIZE] HOT-PATH > 60% do tempo total.")
            lines.append("  Impacto estimado da vetorização: -70..80% no tempo total.")
            lines.append("  Recomendação: IMPLEMENTAR antes dos próximos chunks.")
        elif hot_path_pct >= 35.0:
            lines.append("  [AVALIAR]   HOT-PATH em 35-60% do tempo total.")
            lines.append("  Vetorização traz ganho moderado. Avalie custo/beneficio.")
        else:
            lines.append("  [OK]        HOT-PATH < 35% do tempo total.")
            lines.append("  Bottleneck e I/O ou profiler SQL. Vetorização tem baixo impacto.")
            lines.append("  Recomendação: prosseguir sem otimizar o loop Python.")

        lines.append("=" * 62)
        lines.append("")
        return "\n".join(lines)


# ────────────────────────────────────────────────────────────────────────

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

        # Expoem o profiler para o orquestrador ler após chunk 1
        self.phase_profiler: Optional[BacktestPhaseProfiler] = None

        # Timestamp de mercado do último batch processado
        self._last_market_ts: Optional[datetime] = None

    def run(
        self,
        start: date,
        end: date,
        symbol: str = "6J",
        skip_download: bool = False,
        total_chunks: int = 1,    # passa o total de chunks para projeção correta
    ) -> Dict:
        self.metrics = {
            "total_batches": 0, "total_tape_events": 0, "total_dom_levels": 0,
            "total_clusters": 0, "signature_counts": {}, "hotspots": [],
            "processing_time_seconds": 0.0, "report": "",
        }
        logger.info("=== Backtest iniciado: %s -> %s ===", start, end)
        wall_start = time.time()

        # Inicializa profiler para este chunk
        prof = BacktestPhaseProfiler()
        prof.start_run()
        self.phase_profiler = prof

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

        # ── stream loop instrumentado ───────────────────────────────────────────
        stream_iter = self.adapter.stream_batches(file_path, skip_dom=self.skip_dom)

        while True:
            # Fase: stream (I/O + parse .dbn.zst)
            t0_stream = time.perf_counter()
            try:
                tape_rows, dom_rows = next(stream_iter)
            except StopIteration:
                break
            prof.record("stream", time.perf_counter() - t0_stream)

            # Fase: ingest (Python loop hot-path)
            t0_ingest = time.perf_counter()
            clusters = self.service.ingest_batch(tape_rows, dom_rows, symbol, top_n=10)
            t_ingest = time.perf_counter() - t0_ingest

            # Fase: persist (DuckDB insert está dentro do ingest mas é separado via subtraição)
            # Não há como separar sem refatorar o ingest_batch.
            # persist é estimado como proporcional ao número de rows inseridas.
            # A distinção precisa para decisão está no total ingest vs stream.
            prof.record("ingest", t_ingest)

            self.metrics["total_batches"]     += 1
            self.metrics["total_tape_events"] += len(tape_rows)
            self.metrics["total_dom_levels"]  += len(dom_rows)
            self.metrics["total_clusters"]    += len(clusters)
            prof.tick_batch(len(tape_rows), len(dom_rows), len(clusters))

            for c in clusters:
                sig = c.behavior_signature.value
                self.metrics["signature_counts"][sig] = self.metrics["signature_counts"].get(sig, 0) + 1

            # Atualiza timestamp de mercado
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

        # Fase: profiler SQL
        if self.skip_profiler:
            logger.info("[Profiler] skip_profiler=True — pulando calibragem MFE/MAE.")
        else:
            logger.info("[Profiler] Iniciando calibragem (pode levar alguns minutos com muitos dados)...")
            t_prof_start = time.perf_counter()
            try:
                profiler = SignatureProfiler(self.db_path, cfg=self.cfg, conn=self.repo.conn)
                lookback_days = (end - start).days + 5
                profile = profiler.build_profile(
                    symbol, lookback_days=lookback_days,
                    horizon_minutes=30, since=str(start)
                )
                profiler.save_profile(profile, self.profile_path)
                t_prof_elapsed = time.perf_counter() - t_prof_start
                prof.record("profiler", t_prof_elapsed)
                logger.info("[Profiler] Calibragem concluida em %.1fs", t_prof_elapsed)
            except Exception:
                logger.exception("[Profiler] Falha na calibragem — continuando sem atualizar profile:")

        # Log do summary de fases
        summary_text = prof.summary(label, total_chunks)
        for line in summary_text.splitlines():
            logger.info(line)

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
