from __future__ import annotations
import databento as db
from pathlib import Path
from datetime import date
from typing import Iterator
import logging

logger = logging.getLogger(__name__)


class DatabentoLoader:
    SYMBOL_6J = "6J.n.0"

    def __init__(self, api_key: str, cache_dir: str = "./data/databento"):
        self.client = db.Historical(api_key)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def download(self, start: date, end: date,
                 symbol: str = SYMBOL_6J, schema: str = "mbp-10",
                 force: bool = False) -> Path:
        cache_file = self.cache_dir / f"{symbol}_{start}_{end}_{schema}.dbn.zst"
        if cache_file.exists() and not force:
            logger.info(f"Usando cache: {cache_file}")
            return cache_file
        logger.info(f"Baixando {symbol} de {start} a {end} ({schema})...")
        # ✅ API correta: get_range com path= para salvar em disco
        self.client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols=[symbol],
            schema=schema,
            start=str(start),
            end=str(end),
            stype_in="continuous",
            path=str(cache_file),
        )
        logger.info(f"Download concluído: {cache_file}")
        return cache_file

    def stream_records(self, file_path: Path) -> Iterator:
        store = db.DBNStore.from_file(str(file_path))
        yield from store

    def get_metadata(self, file_path: Path) -> dict:
        store = db.DBNStore.from_file(str(file_path))
        return {"schema": store.schema, "dataset": store.dataset,
                "start": store.start, "end": store.end}
