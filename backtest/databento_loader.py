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
        self.client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols=[symbol],
            schema=schema,
            start=str(start),
            end=str(end),
            stype_in="continuous",
            path=str(cache_file),
        )
        logger.info(f"Download concluido: {cache_file}")
        return cache_file

    def stream_records(self, file_path: Path) -> Iterator:
        """
        Streaming compativel com qualquer versao do databento-python.
        Usa context manager se disponivel (>= 0.30), senão itera direto.
        RAM: DBNStore faz lazy loading — nao carrega o arquivo inteiro.
        """
        store = db.DBNStore.from_file(str(file_path))
        # Suporte a context manager depende da versao instalada
        if hasattr(store, '__exit__'):
            with store:
                yield from store
        else:
            yield from store

    def get_metadata(self, file_path: Path) -> dict:
        store = db.DBNStore.from_file(str(file_path))
        return {"schema": store.schema, "dataset": store.dataset,
                "start": store.start, "end": store.end}
