from __future__ import annotations
from typing import List, Dict
from models import LiquidityCluster
from parser_tsdom import parse_tape_rows, parse_dom_rows
from pattern_engine import PatternEngine
from liquidity_matrix import LiquidityMatrix
from repository_duckdb import DuckDBRepository

class IngestionService:
    def __init__(self, repo: DuckDBRepository, matrix: LiquidityMatrix, engine: PatternEngine):
        self.repo = repo
        self.matrix = matrix
        self.engine = engine

    def ingest_batch(self, tape_rows: List[Dict], dom_rows: List[Dict], symbol: str):
        tape = parse_tape_rows(tape_rows, symbol)
        dom = parse_dom_rows(dom_rows, symbol)
        self.repo.insert_tape_events(tape)
        self.repo.insert_dom_levels(dom)
        self.matrix.build_from_events(tape, dom, classify=self.engine.classify)
        clusters = []
        for e in tape:
            c = LiquidityCluster(symbol=symbol, timestamp=e.timestamp, price=e.price, total_ask=e.volume if e.side.value == 'sell' else 0, total_bid=e.volume if e.side.value == 'buy' else 0, raw_payload=e.raw)
            c.behavior_signature = self.engine.classify(c)
            clusters.append(c)
        self.repo.insert_clusters(clusters)
        return clusters
