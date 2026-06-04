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
import logging
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

        # Validate parse results — warn and bail out early on malformed payloads
        if tape_rows and not tape:
            logging.warning(
                "[ingest_batch] %d tape_rows recebidas mas nenhuma parseada com sucesso "
                "(symbol=%s) — payload pode estar malformado.",
                len(tape_rows), symbol,
            )
            return []
        if not tape:
            return []  # nada a processar — payload vazio legítimo

        # Build clusters from tape — single source of truth
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

        # Persist first — only committed data should drive analysis
        self.repo.begin()
        try:
            self.repo.insert_tape_events(tape)
            self.repo.insert_dom_levels(dom)
            self.repo.insert_clusters(clusters)
            self.repo.commit()
        except Exception:
            self.repo.rollback()
            raise

        # Feed matrix with DOM, tape, and pre-built clusters (data is safe in DB)
        # Snapshot first so we can rollback the matrix if anything fails
        snap = self.matrix.snapshot()
        try:
            self.matrix.build_from_events(tape, dom, clusters=clusters)

            # Post-classify recurring levels (now based on persisted data)
            # Use identity set to restrict refinement to NEW clusters only —
            # clusters from previous batches are already persisted with their own signature
            current_batch_ids = {id(c) for c in clusters}
            hotspots = self.matrix.hotspots(self.cfg.min_occurrences)
            for h in hotspots:
                level_clusters = self.matrix.active_levels.get(h["price"], [])
                refined = self.engine.post_classify(h["price"], level_clusters)
                for c in level_clusters:
                    if id(c) in current_batch_ids:
                        c.behavior_signature = refined
        except Exception:
            self.matrix.restore(snap)
            raise

        return clusters
