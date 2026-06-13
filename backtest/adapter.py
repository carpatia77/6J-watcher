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

        Emite (tape_batch, dom_batch, cancel_batch) como pyarrow.RecordBatch por
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
            "timestamp_ns": [], "price": [],
            "volume": [],       "side": [],
        }
        dom_bufs: dict = {
            "timestamp_ns": [], "price": [],
            "level_index": [],  "bid_volume": [], "ask_volume": [],
        }
        cancel_bufs: dict = {
            "timestamp_ns": [], "price_level": [], "side": [], 
            "size": [], "snapshots_present": []
        }
        batch_start_ns = None
        
        # Estado mantido entre snapshots na mesma sessão
        # (price, side) -> (snapshot_count, last_level_index)
        _wall_tracker: dict[tuple[float, str], tuple[int, int]] = {}

        def _flush_arrow(tape_b: dict, dom_b: dict, cancel_b: dict) -> Tuple[pa.RecordBatch, pa.RecordBatch, pa.RecordBatch]:
            tape_ns = pa.array(tape_b["timestamp_ns"], type=pa.int64())
            tape_rb = pa.record_batch(
                {
                    "timestamp_ns": tape_ns,
                    "timestamp":    tape_ns.cast(pa.timestamp('ns', tz='UTC')).cast(pa.string()),
                    "price":        pa.array(tape_b["price"],        type=pa.float64()),
                    "volume":       pa.array(tape_b["volume"],       type=pa.int32()),
                    "side":         pa.array(tape_b["side"],         type=pa.string()),
                }
            )
            
            dom_ns = pa.array(dom_b["timestamp_ns"], type=pa.int64())
            dom_rb = pa.record_batch(
                {
                    "timestamp_ns": dom_ns,
                    "timestamp":    dom_ns.cast(pa.timestamp('ns', tz='UTC')).cast(pa.string()),
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
            
            cancel_ns = pa.array(cancel_b["timestamp_ns"], type=pa.int64())
            cancel_rb = pa.record_batch(
                {
                    "timestamp_ns":      cancel_ns,
                    "timestamp":         cancel_ns.cast(pa.timestamp('ns', tz='UTC')).cast(pa.string()),
                    "price_level":       pa.array(cancel_b["price_level"], type=pa.int32()),
                    "side":              pa.array(cancel_b["side"], type=pa.string()),
                    "size":              pa.array(cancel_b["size"], type=pa.int32()),
                    "snapshots_present": pa.array(cancel_b["snapshots_present"], type=pa.int32()),
                }
            )
            return tape_rb, dom_rb, cancel_rb

        def _clear(b: dict):
            for k in b:
                b[k].clear()

        for record in self.loader.stream_records(file_path):
            ts_ns = record.ts_event
            if batch_start_ns is None:
                batch_start_ns = ts_ns

            if (ts_ns - batch_start_ns) / 1e9 >= self.batch_size_seconds:
                if tape_bufs["timestamp_ns"] or dom_bufs["timestamp_ns"] or cancel_bufs["timestamp_ns"]:
                    yield _flush_arrow(tape_bufs, dom_bufs, cancel_bufs)
                _clear(tape_bufs)
                _clear(dom_bufs)
                _clear(cancel_bufs)
                batch_start_ns = ts_ns
                # BUG5 FIX: processa o evento da fronteira para o novo buffer
                # (antes era pulado silenciosamente após o _clear)

            action = getattr(record, "action", None)
            action_val = getattr(action, "value", action)
            if action_val in ('T', '84', 84):  # ACTION_TRADE
                size = getattr(record, "size", 0)
                if size > 0:
                    side_raw = str(getattr(record, "side", "N"))
                    side_char = side_raw.split(".")[-1].strip().upper()[0]
                    if side_char == "B":
                        side = "buy"
                    elif side_char == "A":
                        side = "sell"
                    else:
                        side = None
                        
                    if side:
                        tape_bufs["timestamp_ns"].append(ts_ns)
                        tape_bufs["price"].append(record.price / 1_000_000_000)
                        tape_bufs["volume"].append(size)
                        tape_bufs["side"].append(side)

            # Captura de Cancelamentos/Modificações (Fase 2)
            if action_val in ('C', '67', 67, 'M', '77', 77):
                size = getattr(record, "size", 0)
                if size > 0:
                    side_raw = str(getattr(record, "side", "N"))
                    side_char = side_raw.split(".")[-1].strip().upper()[0]
                    side = "buy" if side_char == "B" else ("sell" if side_char == "A" else None)
                    if side:
                        p = record.price / 1_000_000_000
                        # Resgatar contador de persistência e nível
                        snaps, lvl = _wall_tracker.get((p, side), (0, -1))
                        if snaps >= 3:
                            cancel_bufs["timestamp_ns"].append(ts_ns)
                            cancel_bufs["price_level"].append(lvl)
                            cancel_bufs["side"].append(side)
                            cancel_bufs["size"].append(size)
                            cancel_bufs["snapshots_present"].append(snaps)
                        
                        # Resetar contador após cancelamento/modificação
                        _wall_tracker[(p, side)] = (0, lvl)

            if not skip_dom:
                if hasattr(record, "levels") and record.levels:
                    depth = self.reconstructor.depth
                    for i, lv in enumerate(record.levels[:depth]):
                        # Bid level
                        dom_bufs["timestamp_ns"].append(ts_ns)
                        dom_bufs["price"].append(lv.bid_px / 1_000_000_000)
                        dom_bufs["level_index"].append(i)
                        dom_bufs["bid_volume"].append(lv.bid_sz)
                        dom_bufs["ask_volume"].append(0)
                        # Ask level
                        dom_bufs["timestamp_ns"].append(ts_ns)
                        dom_bufs["price"].append(lv.ask_px / 1_000_000_000)
                        dom_bufs["level_index"].append(i)
                        dom_bufs["bid_volume"].append(0)
                        dom_bufs["ask_volume"].append(lv.ask_sz)
                        
                        # Atualizar _wall_tracker
                        if lv.bid_sz > 0:
                            bp = lv.bid_px / 1_000_000_000
                            cnt, _ = _wall_tracker.get((bp, "buy"), (0, i))
                            _wall_tracker[(bp, "buy")] = (cnt + 1, i)
                        if lv.ask_sz > 0:
                            ap = lv.ask_px / 1_000_000_000
                            cnt, _ = _wall_tracker.get((ap, "sell"), (0, i))
                            _wall_tracker[(ap, "sell")] = (cnt + 1, i)
                            
                # Opcional: evitar memory leak no dict limpando periodicamente
                if len(_wall_tracker) > 20000:
                    _wall_tracker.clear()

        if tape_bufs["timestamp_ns"] or dom_bufs["timestamp_ns"] or cancel_bufs["timestamp_ns"]:
            yield _flush_arrow(tape_bufs, dom_bufs, cancel_bufs)

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
