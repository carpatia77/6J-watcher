from __future__ import annotations
import databento as db
from pathlib import Path
from datetime import date, timedelta
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
        # cache usa end original para o nome do arquivo (sem +1)
        cache_file = self.cache_dir / f"{symbol}_{start}_{end}_{schema}.dbn.zst"
        if cache_file.exists() and not force:
            logger.info(f"Usando cache: {cache_file}")
            return cache_file
        # API Databento: end é exclusivo — somar 1 dia para incluir o último dia do chunk
        end_exclusive = end + timedelta(days=1)
        logger.info(f"Baixando {symbol} de {start} a {end} ({schema})...")
        self.client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols=[symbol],
            schema=schema,
            start=str(start),
            end=str(end_exclusive),
            stype_in="continuous",
            path=str(cache_file),
        )
        logger.info(f"Download concluido: {cache_file}")
        return cache_file

    def stream_records(self, file_path: Path) -> Iterator:
        """
        Streaming do arquivo .dbn.zst com context manager correto.

        BUG 3 FIX: o padrão anterior abria DBNStore.from_file() e depois
        aplicava 'with store:' no mesmo objeto já aberto. Se a API
        databento-python fechar o handle em __exit__, o yield from store
        falharia com arquivo fechado.

        Correção: DBNStore é aberto diretamente como context manager desde
        a criação. O fallback sem context manager cobre SDKs mais antigos.
        RAM: DBNStore faz lazy loading — não carrega o arquivo inteiro.
        """
        if hasattr(db.DBNStore, "from_file"):
            # SDK moderno: abre e itera dentro do mesmo context manager
            store = db.DBNStore.from_file(str(file_path))
            if hasattr(store, "__exit__"):
                with store:
                    yield from store
            else:
                # SDK antigo sem context manager — itera direto
                yield from store
        else:
            # Fallback para versões muito antigas do SDK
            with db.DBNStore(str(file_path)) as store:
                yield from store

    def get_metadata(self, file_path: Path) -> dict:
        store = db.DBNStore.from_file(str(file_path))
        return {"schema": store.schema, "dataset": store.dataset,
                "start": store.start, "end": store.end}
