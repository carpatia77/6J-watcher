from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Tuple, Iterator
from pathlib import Path
import logging

from backtest.databento_loader import DatabentoLoader
from backtest.book_reconstructor import BookReconstructor

logger = logging.getLogger(__name__)


class DatabentoAdapter:
    def __init__(self, loader: DatabentoLoader, batch_size_seconds: int = 60):
        self.loader = loader
        self.batch_size_seconds = batch_size_seconds
        self.reconstructor = BookReconstructor(depth=10)

    def stream_batches(self, file_path: Path, skip_dom: bool = False) -> Iterator[Tuple[List[Dict], List[Dict]]]:
        current_tape: List[Dict] = []
        current_dom: List[Dict] = []
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
