from __future__ import annotations
"""
ingestion.py
------------
Orquestra o pipeline completo de ingestão:
  1. Parse T&S e DOM
  2. Persiste no DuckDB
  3. Alimenta a LiquidityMatrix
  4. Classifica assinatura comportamental
  5. Devolve clusters gerados
"""
from typing import Dict, List
from config import Config
from models import LiquidityCluster
from parser_tsdom import parse_tape_rows, parse_dom_rows
from pattern_engine import PatternEngine
from liquidity_matrix import LiquidityMatrix
from repository_duckdb import DuckDBRepository


class IngestionService:
    def __init__(self, repo: DuckDBRepository, matrix: LiquidityMatrix, engine: PatternEngine, cfg: Config):
        self.repo   = repo
        self.matrix = matrix
        self.engine = engine
        self.cfg    = cfg

    def ingest_batch(self, tape_rows: List[Dict], dom_rows: List[Dict], symbol: str) -> List[LiquidityCluster]:
        tape   = parse_tape_rows(tape_rows, symbol)
        dom    = parse_dom_rows(dom_rows, symbol)

        # Persist raw events
        self.repo.insert_tape_events(tape)
        self.repo.insert_dom_levels(dom)

        # Feed matrix with both streams
        self.matrix.build_from_events(tape, dom, classify=self.engine.classify)

        # Build clusters from tape and classify
        clusters: List[LiquidityCluster] = []
        for e in tape:
            session = self.cfg.session_for(e.timestamp.hour)
            c = LiquidityCluster(
                symbol    = symbol,
                timestamp = e.timestamp,
                price     = e.price,
                session   = session,
                total_bid = e.volume if e.side.value == "buy"  else 0,
                total_ask = e.volume if e.side.value == "sell" else 0,
                cumdelta  = e.volume if e.side.value == "buy"  else -e.volume,
                raw_payload = e.raw,
            )
            c.behavior_signature = self.engine.classify(c)
            clusters.append(c)

        self.repo.insert_clusters(clusters)

        # Post-classify recurring levels
        hotspots = self.matrix.hotspots(self.cfg.min_occurrences)
        for h in hotspots:
            level_clusters = self.matrix.active_levels.get(h["price"], [])
            refined = self.engine.post_classify(h["price"], level_clusters)
            for c in level_clusters:
                c.behavior_signature = refined

        return clusters
