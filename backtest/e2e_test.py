"""
e2e_test.py
-----------
Testes end-to-end do pipeline 6J Watcher.

Execução:
    pytest backtest/e2e_test.py -v

Testes com dados reais (requer DATABENTO_API_KEY no ambiente):
    pytest backtest/e2e_test.py -v -m live
"""
from __future__ import annotations
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import Dict
from unittest.mock import patch, MagicMock

import pytest

from backtest.book_reconstructor import BookReconstructor, BookSnapshot
from backtest.adapter import DatabentoAdapter
from backtest.backtest_runner import BacktestRunner
from config import Config
from ingestion import IngestionService
from liquidity_matrix import LiquidityMatrix
from adaptive_pattern_engine import AdaptivePatternEngine
from repository_duckdb import DuckDBRepository


# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------

FIXED = 1_000_000_000  # Databento fixed-point


def _make_level(bid_px: float, bid_sz: int, ask_px: float, ask_sz: int):
    """Cria mock de BidAskPair (record.levels[i]) compatível com a API Databento."""
    class _Level:
        pass
    lv = _Level()
    lv.bid_px = int(bid_px * FIXED)
    lv.bid_sz = bid_sz
    lv.ask_px = int(ask_px * FIXED)
    lv.ask_sz = ask_sz
    return lv


def _make_trade_record(
    price: float = 0.006760,
    size: int = 50,
    side: str = "B",       # 'B'=bid aggressor=compra, 'A'=ask aggressor=venda
    ts_ns: int = 1_700_000_000_000_000_000,
    action: str = "T",
    num_levels: int = 10,
) -> object:
    """Mock completo de record MBP-10 (trade + book snapshot)."""
    class _Record:
        pass
    r = _Record()
    r.ts_event = ts_ns
    r.price    = int(price * FIXED)
    r.size     = size
    r.side     = side
    r.action   = action
    r.levels   = [
        _make_level(
            bid_px=price - (i + 1) * 0.00005,
            bid_sz=100 + i * 10,
            ask_px=price + (i + 1) * 0.00005,
            ask_sz=100 + i * 10,
        )
        for i in range(num_levels)
    ]
    return r


def _make_synthetic_batch(
    n_tape: int = 10,
    n_dom: int = 10,
    price: float = 0.006760,
    minute_offset: int = 0,
) -> tuple:
    """Gera um par (tape_rows, dom_rows) sintético pronto para ingest_batch()."""
    tape = [
        {
            "timestamp": datetime(2026, 6, 4, 14, minute_offset, i % 60).strftime("%Y-%m-%dT%H:%M:%S"),
            "price":  price + i * 0.00001,
            "volume": 20 + i,
            "side":   "buy" if i % 2 == 0 else "sell",
            "timestamp_ns": 1_717_502_400_000_000_000 + minute_offset * 60_000_000_000 + i * 1_000_000_000,
        }
        for i in range(n_tape)
    ]
    dom = [
        {
            "timestamp": datetime(2026, 6, 4, 14, minute_offset, 0).strftime("%Y-%m-%dT%H:%M:%S"),
            "price":        price,
            "level_index":  j,
            "bid_volume":   100,
            "ask_volume":   100,
            "timestamp_ns": 1_717_502_400_000_000_000 + minute_offset * 60_000_000_000,
        }
        for j in range(n_dom)
    ]
    return tape, dom


# ---------------------------------------------------------------------------
# TestBookReconstructor
# ---------------------------------------------------------------------------

class TestBookReconstructor:
    """Testes unitários do reconstrutor de book."""

    def test_extract_trade_buy(self):
        """Record com action='T' e side='B' → tape event de compra."""
        rc = BookReconstructor()
        record = _make_trade_record(price=0.006760, size=50, side="B", action="T")
        event = rc.extract_tape_event(record)

        assert event is not None
        assert event["side"] == "buy"
        assert event["volume"] == 50
        assert abs(event["price"] - 0.006760) < 1e-8

    def test_extract_trade_sell(self):
        """Record com action='T' e side='A' → tape event de venda."""
        rc = BookReconstructor()
        record = _make_trade_record(price=0.006760, size=30, side="A", action="T")
        event = rc.extract_tape_event(record)

        assert event is not None
        assert event["side"] == "sell"
        assert event["volume"] == 30

    def test_non_trade_record_returns_none(self):
        """Record com action='A' (add order) não gera tape event."""
        rc = BookReconstructor()
        record = _make_trade_record(action="A", size=100)
        event = rc.extract_tape_event(record)
        assert event is None

    def test_zero_size_returns_none(self):
        """Trade com size=0 é descartado."""
        rc = BookReconstructor()
        record = _make_trade_record(action="T", size=0)
        event = rc.extract_tape_event(record)
        assert event is None

    def test_process_record_returns_snapshot(self):
        """process_record() retorna BookSnapshot com 10 bid + 10 ask levels."""
        rc = BookReconstructor(depth=10)
        record = _make_trade_record(price=0.006760, num_levels=10)
        snap = rc.process_record(record)

        assert snap is not None
        assert abs(snap.last_price - 0.006760) < 1e-8
        assert len(snap.bid_levels) == 10
        assert len(snap.ask_levels) == 10

    def test_fixed_point_conversion(self):
        """Preços são corretamente convertidos de fixed-point (int * 1e-9) para float."""
        rc = BookReconstructor(depth=1)
        price = 0.006750
        record = _make_trade_record(price=price, num_levels=1)
        snap = rc.process_record(record)

        # Tolerância: 1 pip do 6J = 0.00001
        assert abs(snap.bid_levels[0]["price"] - (price - 0.00005)) < 1e-8
        assert abs(snap.ask_levels[0]["price"] - (price + 0.00005)) < 1e-8

    def test_snapshot_dom_rows_format(self):
        """to_dom_rows() emite formato exato esperado pelo parse_dom_rows()."""
        rc = BookReconstructor(depth=2)
        record = _make_trade_record(price=0.006760, num_levels=2)
        snap = rc.process_record(record)
        rows = snap.to_dom_rows()

        # 2 bid rows + 2 ask rows = 4 total
        assert len(rows) == 4

        bid_rows = [r for r in rows if r["bid_volume"] > 0]
        ask_rows = [r for r in rows if r["ask_volume"] > 0]
        assert len(bid_rows) == 2
        assert len(ask_rows) == 2

        # Campos obrigatórios para parse_dom_rows()
        for row in rows:
            assert "timestamp" in row
            assert "price" in row
            assert "level_index" in row
            assert "bid_volume" in row
            assert "ask_volume" in row
            assert row["price"] > 0

    def test_timestamp_is_utc(self):
        """Timestamp gerado deve ser UTC (não hora local)."""
        rc = BookReconstructor()
        # 2023-11-14T22:00:00 UTC
        ts_ns = int(datetime(2023, 11, 14, 22, 0, 0).timestamp() * 1e9)
        record = _make_trade_record(ts_ns=ts_ns)
        snap = rc.process_record(record)

        # utcfromtimestamp deve retornar hora 22, não ajustada para fuso local
        assert snap.timestamp.hour == 22


# ---------------------------------------------------------------------------
# TestAdapterFormat
# ---------------------------------------------------------------------------

class TestAdapterFormat:
    """Valida que o output do adapter é compatível com parse_tape_rows/parse_dom_rows."""

    def test_tape_row_fields(self):
        """tape_rows geradas pelo reconstructor têm todos os campos do parser."""
        rc = BookReconstructor()
        record = _make_trade_record(price=0.006760, size=50, side="B", action="T")
        event = rc.extract_tape_event(record)

        # parse_tape_rows() requer: timestamp, price, volume, side
        assert "timestamp" in event
        assert "price" in event
        assert "volume" in event
        assert "side" in event
        assert event["side"] in ("buy", "sell")
        assert event["price"] > 0
        assert event["volume"] > 0

    def test_tape_timestamp_format(self):
        """timestamp deve ser aceito pelo _to_datetime() do parser_tsdom."""
        rc = BookReconstructor()
        record = _make_trade_record()
        event = rc.extract_tape_event(record)

        # Parser aceita: "YYYY-MM-DDTHH:MM:SS" ou "YYYY-MM-DD HH:MM:SS"
        ts = event["timestamp"]
        assert "T" in ts or " " in ts
        # Deve parsear sem exceção
        from parser_tsdom import _to_datetime
        dt = _to_datetime(ts)
        assert isinstance(dt, datetime)


# ---------------------------------------------------------------------------
# TestE2EPipeline — sem Databento, dados sintéticos
# ---------------------------------------------------------------------------

class TestE2EPipeline:
    """Testes end-to-end usando pipeline real com dados sintéticos."""

    @pytest.fixture
    def tmp_pipeline(self, tmp_path):
        """Instância completa do pipeline com DB temporário."""
        db_path = str(tmp_path / "test.db")
        cfg = Config()
        cfg.db_path = db_path

        repo    = DuckDBRepository(db_path)
        matrix  = LiquidityMatrix(cfg.symbol, cfg.tick_size)
        engine  = AdaptivePatternEngine(cfg=cfg)
        service = IngestionService(repo=repo, matrix=matrix, engine=engine, cfg=cfg)
        return service, repo, matrix, cfg

    def _make_tape_rows(self, n: int, price: float = 0.006760, side: str = "buy") -> list:
        return [
            {
                "timestamp": datetime(2026, 6, 4, 14, 0, i % 60).strftime("%Y-%m-%dT%H:%M:%S"),
                "price": price,
                "volume": 50,
                "side": side,
            }
            for i in range(n)
        ]

    def _make_dom_rows(self, n: int, price: float = 0.006760,
                       bid_vol: int = 100, ask_vol: int = 100) -> list:
        return [
            {
                "timestamp": datetime(2026, 6, 4, 14, 0, i % 60).strftime("%Y-%m-%dT%H:%M:%S"),
                "price": price,
                "level_index": 0,
                "bid_volume": bid_vol,
                "ask_volume": ask_vol,
            }
            for i in range(n)
        ]

    def test_ingest_returns_clusters(self, tmp_pipeline):
        """ingest_batch() retorna clusters para tape não-vazia."""
        service, repo, _, _ = tmp_pipeline
        tape = self._make_tape_rows(10)
        dom  = self._make_dom_rows(10)
        clusters = service.ingest_batch(tape, dom, "6J")
        assert len(clusters) == 10

    def test_ingest_empty_tape_returns_empty(self, tmp_pipeline):
        """ingest_batch() com tape vazia retorna [] sem exceção."""
        service, _, _, _ = tmp_pipeline
        clusters = service.ingest_batch([], [], "6J")
        assert clusters == []

    def test_clusters_have_valid_signatures(self, tmp_pipeline):
        """Todos os clusters têm BehaviorSignature válida (não None)."""
        from models import BehaviorSignature
        service, _, _, _ = tmp_pipeline
        tape = self._make_tape_rows(20)
        dom  = self._make_dom_rows(20)
        clusters = service.ingest_batch(tape, dom, "6J")
        for c in clusters:
            assert isinstance(c.behavior_signature, BehaviorSignature)

    def test_absorption_scenario(self, tmp_pipeline):
        """
        50 compras de volume 100 no mesmo preço, com parede de ask enorme →
        deve detectar ABSORPTION_PASSIVE em pelo menos alguns clusters
        (requer thresholds do fallback profile).
        """
        from models import BehaviorSignature
        service, _, _, cfg = tmp_pipeline

        # Força thresholds baixos para o fallback profile detectar absorção
        service.engine.profile["thresholds"]["NEW_YORK"] = {
            "vol_percentiles": {"75": 40, "90": 80},
            "imb_percentiles": {"75": 40, "90": 80},
        }

        # 50 compras de 100 contratos no mesmo preço = volume=100, side=buy
        tape = [
            {
                "timestamp": datetime(2026, 6, 4, 14, 0, i % 60).strftime("%Y-%m-%dT%H:%M:%S"),
                "price": 0.006760,
                "volume": 100,
                "side": "buy",
            }
            for i in range(50)
        ]
        dom = self._make_dom_rows(1, bid_vol=50, ask_vol=5000)

        clusters = service.ingest_batch(tape, dom, "6J")
        assert len(clusters) == 50

        absorption_count = sum(
            1 for c in clusters
            if c.behavior_signature == BehaviorSignature.ABSORPTION_PASSIVE
        )
        # Com vol=100, imbalance=100 e delta=0 ticks, deve detectar absorção
        assert absorption_count > 0, (
            f"Nenhuma absorção detectada. Distribuição: "
            f"{set(c.behavior_signature.value for c in clusters)}"
        )

    def test_data_persisted_to_duckdb(self, tmp_pipeline):
        """
        Após ingest_batch(), tape_events, dom_levels e clusters devem estar no DuckDB.

        GAP 2 (fechado): dom_levels não estava sendo verificado — qualquer regressão
        em repo.insert_dom_levels() passava silenciosamente por este teste.
        """
        service, repo, _, _ = tmp_pipeline
        tape = self._make_tape_rows(5)
        dom  = self._make_dom_rows(5)
        service.ingest_batch(tape, dom, "6J")

        tape_count = repo.conn.execute(
            "SELECT COUNT(*) FROM tape_events WHERE symbol = '6J'"
        ).fetchone()[0]
        cluster_count = repo.conn.execute(
            "SELECT COUNT(*) FROM liquidity_clusters WHERE symbol = '6J'"
        ).fetchone()[0]
        # GAP 2: verifica que dom_levels também foram persistidos
        dom_count = repo.conn.execute(
            "SELECT COUNT(*) FROM dom_levels WHERE symbol = '6J'"
        ).fetchone()[0]

        assert tape_count    == 5, f"tape_events: esperado 5, got {tape_count}"
        assert cluster_count == 5, f"liquidity_clusters: esperado 5, got {cluster_count}"
        assert dom_count     == 5, f"dom_levels: esperado 5, got {dom_count}"  # GAP 2

    def test_signature_distribution_populated(self, tmp_pipeline):
        """repo.signature_distribution() retorna dados após ingest."""
        service, repo, _, _ = tmp_pipeline
        tape = self._make_tape_rows(30)
        dom  = self._make_dom_rows(30)
        service.ingest_batch(tape, dom, "6J")

        dist = repo.signature_distribution("6J")
        assert len(dist) > 0


# ---------------------------------------------------------------------------
# TestBacktestRunner — sem Databento
# ---------------------------------------------------------------------------

class TestBacktestRunner:
    """Testes do BacktestRunner injetando batches diretamente (sem download)."""

    @pytest.fixture
    def runner(self, tmp_path):
        return BacktestRunner(
            api_key="fake_key_for_unit_test",
            db_path=str(tmp_path / "bt.db"),
            profile_path=str(tmp_path / "profile.json"),
        )

    def test_runner_initializes(self, runner):
        """BacktestRunner inicializa todos os componentes sem erro."""
        assert runner.repo    is not None
        assert runner.matrix  is not None
        assert runner.engine  is not None
        assert runner.service is not None
        assert runner.loader  is not None
        assert runner.adapter is not None

    def test_direct_ingest_updates_metrics(self, runner):
        """Injetar batch diretamente no service atualiza métricas corretamente."""
        tape = [
            {
                "timestamp": datetime(2026, 6, 4, 14, i, 0).strftime("%Y-%m-%dT%H:%M:%S"),
                "price": 0.006760 + i * 0.00001,
                "volume": 20 + i,
                "side": "buy" if i % 2 == 0 else "sell",
            }
            for i in range(20)
        ]
        dom = [
            {
                "timestamp": datetime(2026, 6, 4, 14, 0, 0).strftime("%Y-%m-%dT%H:%M:%S"),
                "price": 0.006760,
                "level_index": j,
                "bid_volume": 100,
                "ask_volume": 100,
            }
            for j in range(10)
        ]

        clusters = runner.service.ingest_batch(tape, dom, "6J")
        assert len(clusters) == 20

    def test_runner_stream_loop_mock(self, tmp_path):
        """
        GAP 1 (fechado): o loop stream do BacktestRunner.run() nunca foi testado
        sem um arquivo .dbn.zst real. Qualquer regressão no loop
        (stream → ingest → metrics) era invisível para o CI.

        Este teste mocka adapter.stream_batches() com um generator de 3 batches
        sintéticos e um arquivo dummy, exercendo o loop completo de run()
        incluindo acumulação de métricas, phase_profiler e skip_profiler=True.

        Garante:
          - total_batches == 3
          - total_tape_events == soma dos tapes dos 3 batches
          - total_dom_levels  == soma dos dom dos 3 batches
          - total_clusters    > 0  (classificação real, não mock)
          - phase_profiler    não é None após o run
          - nenhuma exceção é levantada
        """
        # Batches sintéticos: 3 batches com volumes crescentes
        batch_specs = [
            (8,  10, 0.006760, 0),  # (n_tape, n_dom, price, minute_offset)
            (12, 10, 0.006765, 1),
            (6,  10, 0.006755, 2),
        ]
        synthetic_batches = [
            _make_synthetic_batch(n_tape, n_dom, price, mo)
            for n_tape, n_dom, price, mo in batch_specs
        ]
        expected_tape   = sum(s[0] for s in batch_specs)   # 26
        expected_dom    = sum(s[1] for s in batch_specs)   # 30

        # Arquivo dummy — runner._resolve_file() é bypassed via mock
        dummy_file = tmp_path / "dummy.dbn.zst"
        dummy_file.write_bytes(b"\x00" * 64)  # conteúdo irrelevante

        runner = BacktestRunner(
            api_key="fake_key",
            db_path=str(tmp_path / "gap1.db"),
            profile_path=str(tmp_path / "gap1_profile.json"),
            skip_profiler=True,
        )

        def _fake_stream(file_path, skip_dom=False):
            """Generator que emite os 3 batches sintéticos e para."""
            yield from synthetic_batches

        with patch.object(runner.adapter, "stream_batches", side_effect=_fake_stream), \
             patch.object(runner, "_resolve_file", return_value=dummy_file):
            metrics = runner.run(
                start=date(2026, 6, 4),
                end=date(2026, 6, 4),
                symbol="6J",
                skip_download=True,
                total_chunks=1,
            )

        assert metrics["total_batches"]     == 3, \
            f"Esperado 3 batches, got {metrics['total_batches']}"
        assert metrics["total_tape_events"] == expected_tape, \
            f"Esperado {expected_tape} tape events, got {metrics['total_tape_events']}"
        assert metrics["total_dom_levels"]  == expected_dom, \
            f"Esperado {expected_dom} dom levels, got {metrics['total_dom_levels']}"
        assert metrics["total_clusters"]    >  0, \
            "Nenhum cluster gerado — ingest_batch() falhou silenciosamente"
        assert runner.phase_profiler is not None, \
            "phase_profiler não foi inicializado pelo run()"
        assert len(metrics["signature_counts"]) > 0, \
            "signature_counts vazio — classificação não ocorreu"


# ---------------------------------------------------------------------------
# Testes Live (marcados com @pytest.mark.live — requerem DATABENTO_API_KEY)
# ---------------------------------------------------------------------------

@pytest.mark.live
def test_live_download_and_ingest(tmp_path):
    """
    Baixa 1 dia de dados reais do 6J e roda pipeline completo.
    Requer: DATABENTO_API_KEY no ambiente.

    Execução:
        pytest backtest/e2e_test.py::test_live_download_and_ingest -v -m live
    """
    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        pytest.skip("DATABENTO_API_KEY não encontrada no ambiente")

    runner = BacktestRunner(
        api_key=api_key,
        db_path=str(tmp_path / "live_test.db"),
        profile_path=str(tmp_path / "live_profile.json"),
        batch_size_seconds=300,  # janelas de 5 min para acelerar
    )

    metrics = runner.run(
        start=date(2026, 5, 1),
        end=date(2026, 5, 2),   # 1 dia para minimizar custo
        symbol="6J",
    )

    assert metrics["total_batches"] > 0
    assert metrics["total_clusters"] > 0
    assert metrics["total_tape_events"] > 0
    assert len(metrics["signature_counts"]) > 0

    print(f"\n=== Live Test Results ===")
    print(f"Batches:   {metrics['total_batches']}")
    print(f"Clusters:  {metrics['total_clusters']}")
    print(f"Tape evts: {metrics['total_tape_events']}")
    print(f"Assinaturas: {metrics['signature_counts']}")
    print(f"Tempo: {metrics['processing_time_seconds']:.1f}s")


if __name__ == "__main__":
    import pytest as _pytest
    _pytest.main([__file__, "-v", "-m", "not live"])
