from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Tuple, Iterator, Union
from pathlib import Path
import logging

try:
    import pyarrow as pa
    _ARROW_AVAILABLE = True
except ImportError:
    _ARROW_AVAILABLE = False

from backtest.databento_loader import DatabentoLoader
from backtest.book_reconstructor import BookReconstructor

logger = logging.getLogger(__name__)

# Tamanho do bloco de alocação Arrow (evita re-alocação contínua)
# 4096 eventos por bloco = ~256KB por array de double — cabe em L2 cache
_ARROW_BLOCK = 4096


class DatabentoAdapter:
    def __init__(self, loader: DatabentoLoader, batch_size_seconds: int = 60):
        self.loader = loader
        self.batch_size_seconds = batch_size_seconds
        self.reconstructor = BookReconstructor(depth=10)

    # ------------------------------------------------------------------
    # Path backtest: emite RecordBatches Arrow (zero-copy para DuckDB)
    # ------------------------------------------------------------------

    def stream_batches_arrow(
        self,
        file_path: Path,
        skip_dom: bool = False,
    ) -> Iterator[Tuple["pa.RecordBatch", "pa.RecordBatch"]]:
        """
        Versão vetorizada de stream_batches() para o backtest.

        Emite (tape_batch, dom_batch) como pyarrow.RecordBatch por
        janela de batch_size_seconds.

        BUG5 FIX: o evento que cruza a fronteira de janela não é mais
        descartado — após _clear() e reset de batch_start_ns, o evento
        é adicionado ao novo buffer antes de continuar o loop.
        """
        if not _ARROW_AVAILABLE:
            raise ImportError(
                "pyarrow não instalado. Execute: pip install pyarrow"
            )

        tape_bufs: dict = {
            "timestamp_ns": [], "timestamp": [], "price": [],
            "volume": [],       "side": [],
        }
        dom_bufs: dict = {
            "timestamp_ns": [], "timestamp": [], "price": [],
            "level_index": [],  "bid_volume": [], "ask_volume": [],
        }
        batch_start_ns = None

        def _flush_arrow(tape_b: dict, dom_b: dict):
            tape_rb = pa.record_batch(
                {
                    "timestamp_ns": pa.array(tape_b["timestamp_ns"], type=pa.int64()),
                    "timestamp":    pa.array(tape_b["timestamp"],    type=pa.string()),
                    "price":        pa.array(tape_b["price"],        type=pa.float64()),
                    "volume":       pa.array(tape_b["volume"],       type=pa.int32()),
                    "side":         pa.array(tape_b["side"],         type=pa.string()),
                }
            )
            dom_rb = pa.record_batch(
                {
                    "timestamp_ns": pa.array(dom_b["timestamp_ns"], type=pa.int64()),
                    "timestamp":    pa.array(dom_b["timestamp"],    type=pa.string()),
                    "price":        pa.array(dom_b["price"],        type=pa.float64()),
                    "level_index":  pa.array(dom_b["level_index"],  type=pa.int32()),
                    "bid_volume":   pa.array(dom_b["bid_volume"],   type=pa.int32()),
                    "ask_volume":   pa.array(dom_b["ask_volume"],   type=pa.int32()),
                }
            ) if not skip_dom else pa.record_batch({
                    "timestamp_ns": pa.array([], type=pa.int64()),
                    "timestamp":    pa.array([], type=pa.string()),
                    "price":        pa.array([], type=pa.float64()),
                    "level_index":  pa.array([], type=pa.int32()),
                    "bid_volume":   pa.array([], type=pa.int32()),
                    "ask_volume":   pa.array([], type=pa.int32()),
            })
            return tape_rb, dom_rb

        def _clear(b: dict):
            for k in b:
                b[k].clear()

        for record in self.loader.stream_records(file_path):
            ts_ns = record.ts_event
            if batch_start_ns is None:
                batch_start_ns = ts_ns

            if (ts_ns - batch_start_ns) / 1e9 >= self.batch_size_seconds:
                if tape_bufs["timestamp_ns"]:
                    yield _flush_arrow(tape_bufs, dom_bufs)
                _clear(tape_bufs)
                _clear(dom_bufs)
                batch_start_ns = ts_ns
                # BUG5 FIX: processa o evento da fronteira para o novo buffer
                # (antes era pulado silenciosamente após o _clear)

            tape_event = self.reconstructor.extract_tape_event(record)
            if tape_event:
                tape_bufs["timestamp_ns"].append(tape_event.get("timestamp_ns"))
                tape_bufs["timestamp"].append(tape_event["timestamp"])
                tape_bufs["price"].append(tape_event["price"])
                tape_bufs["volume"].append(tape_event["volume"])
                tape_bufs["side"].append(tape_event["side"])

            if not skip_dom:
                snapshot = self.reconstructor.process_record(record)
                if snapshot:
                    for row in snapshot.to_dom_rows():
                        dom_bufs["timestamp_ns"].append(row.get("timestamp_ns"))
                        dom_bufs["timestamp"].append(row["timestamp"])
                        dom_bufs["price"].append(row["price"])
                        dom_bufs["level_index"].append(row["level_index"])
                        dom_bufs["bid_volume"].append(row["bid_volume"])
                        dom_bufs["ask_volume"].append(row["ask_volume"])

        if tape_bufs["timestamp_ns"]:
            yield _flush_arrow(tape_bufs, dom_bufs)

    # ------------------------------------------------------------------
    # Path produção: mantido inalterado (MQL5 / main.py)
    # ------------------------------------------------------------------

    def stream_batches(
        self,
        file_path: Path,
        skip_dom: bool = False,
    ) -> Iterator[Tuple[List[Dict], List[Dict]]]:
        """
        Path original — List[Dict] — usado por:
          - TestBacktestRunner.test_runner_stream_loop_mock (testes)
          - Fallback quando pyarrow não está instalado

        NÃO modificado. Mantido para compatibilidade total.
        """
        current_tape: List[Dict] = []
        current_dom:  List[Dict] = []
        batch_start_ns = None

        for record in self.loader.stream_records(file_path):
            ts_ns = record.ts_event
            if batch_start_ns is None:
                batch_start_ns = ts_ns

            elapsed = (ts_ns - batch_start_ns) / 1e9
            if elapsed >= self.batch_size_seconds:
                if current_tape or current_dom:
                    yield current_tape, current_dom
                current_tape, current_dom = [], []
                batch_start_ns = ts_ns

            tape_event = self.reconstructor.extract_tape_event(record)
            if tape_event:
                current_tape.append(tape_event)

            snapshot = self.reconstructor.process_record(record)
            if snapshot and not skip_dom:
                current_dom.extend(snapshot.to_dom_rows())

        if current_tape or current_dom:
            yield current_tape, current_dom
