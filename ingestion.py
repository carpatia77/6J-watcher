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
import time
from typing import Dict, List
from config import Config
from models import LiquidityCluster
from parser_tsdom import parse_tape_rows, parse_dom_rows
from adaptive_pattern_engine import AdaptivePatternEngine
from liquidity_matrix import LiquidityMatrix
from repository_duckdb import DuckDBRepository


class IngestionService:
    def __init__(self, repo: DuckDBRepository, matrix: LiquidityMatrix, engine: AdaptivePatternEngine, cfg: Config, narrator=None):
        self.repo   = repo
        self.matrix = matrix
        self.engine = engine
        self.cfg    = cfg
        self.narrator = narrator

        # Inicializa cursor de preço do DuckDB para evitar delta=0 no cold start
        row = repo.conn.execute(
            "SELECT price FROM tape_events WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            [cfg.symbol]
        ).fetchone()
        self.last_closed_price = row[0] if row else None
        if self.last_closed_price is not None:
            logging.info(f"[IngestionService] Cold start: last_closed_price={self.last_closed_price} (do DuckDB)")

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

        if dom_rows and not dom:
            logging.warning(
                "[ingest_batch] %d dom_rows recebidas mas nenhuma parseada com sucesso "
                "(symbol=%s) — DOM sensor pode estar offline.",
                len(dom_rows), symbol,
            )

        # Agregação de tape events por (price, side) com janela de continuidade
        # Se mudar de preço ou lado, fecha o cluster atual e abre outro.
        clusters: List[LiquidityCluster] = []
        batch_id = str(time.time_ns())
        
        if tape:
            current_cluster_data = None
            
            for e in tape:
                side = e.side.value
                price = e.price
                vol = e.volume
                cumdelta = vol if side == "buy" else -vol
                
                # Quebra de contexto: mudou o preço ou a direção da agressão
                if current_cluster_data is None or current_cluster_data["price"] != price or current_cluster_data["side"] != side:
                    # Fecha o cluster anterior, se existir
                    if current_cluster_data is not None:
                        # O delta_price_ticks é medido em relação ao último cluster fechado
                        dp = round((current_cluster_data["price"] - self.last_closed_price) / self.cfg.tick_size) if self.last_closed_price is not None else 0
                        
                        c = LiquidityCluster(
                            symbol=symbol,
                            timestamp=current_cluster_data["timestamp"],
                            price=current_cluster_data["price"],
                            session=self.cfg.session_for(current_cluster_data["timestamp"].hour),
                            total_bid=current_cluster_data["total_bid"],
                            total_ask=current_cluster_data["total_ask"],
                            cumdelta=current_cluster_data["cumdelta"],
                            deltamin=current_cluster_data["deltamin"],
                            deltamax=current_cluster_data["deltamax"],
                            delta_price_ticks=dp,
                            batch_id=batch_id,
                            raw_payload={"events_aggregated": current_cluster_data["count"]},
                        )
                        sig, conf = self.engine.classify(c)
                        c.behavior_signature = sig
                        c.confidence = conf
                        clusters.append(c)
                        self.last_closed_price = current_cluster_data["price"]
                    
                    # Inicia um novo cluster
                    current_cluster_data = {
                        "price": price,
                        "side": side,
                        "timestamp": e.timestamp,
                        "total_bid": vol if side == "buy" else 0,
                        "total_ask": vol if side == "sell" else 0,
                        "cumdelta": cumdelta,
                        "deltamin": min(0, cumdelta),
                        "deltamax": max(0, cumdelta),
                        "count": 1
                    }
                else:
                    # Acumula no mesmo cluster
                    current_cluster_data["total_bid"] += (vol if side == "buy" else 0)
                    current_cluster_data["total_ask"] += (vol if side == "sell" else 0)
                    current_cluster_data["cumdelta"] += cumdelta
                    current_cluster_data["deltamin"] = min(current_cluster_data["deltamin"], current_cluster_data["cumdelta"])
                    current_cluster_data["deltamax"] = max(current_cluster_data["deltamax"], current_cluster_data["cumdelta"])
                    current_cluster_data["count"] += 1
                    
            # Adiciona o último cluster pendente
            if current_cluster_data is not None:
                dp = round((current_cluster_data["price"] - self.last_closed_price) / self.cfg.tick_size) if self.last_closed_price is not None else 0
                c = LiquidityCluster(
                    symbol=symbol,
                    timestamp=current_cluster_data["timestamp"],
                    price=current_cluster_data["price"],
                    session=self.cfg.session_for(current_cluster_data["timestamp"].hour),
                    total_bid=current_cluster_data["total_bid"],
                    total_ask=current_cluster_data["total_ask"],
                    cumdelta=current_cluster_data["cumdelta"],
                    deltamin=current_cluster_data["deltamin"],
                    deltamax=current_cluster_data["deltamax"],
                    delta_price_ticks=dp,
                    batch_id=batch_id,
                    raw_payload={"events_aggregated": current_cluster_data["count"]},
                )
                sig, conf = self.engine.classify(c)
                c.behavior_signature = sig
                c.confidence = conf
                clusters.append(c)
                self.last_closed_price = current_cluster_data["price"]

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

            # Captura as assinaturas originais para detectar elevações (ex: DEFENSE_LINE)
            original_signatures: dict[int, str] = {id(c): c.behavior_signature.value for c in clusters}

            # Post-classify recurring levels (now based on persisted data)
            current_batch_id = clusters[0].batch_id if clusters else None
            
            self._batch_counter = getattr(self, '_batch_counter', 0) + 1
            if self._batch_counter % 10 == 0:
                hotspots = self.matrix.hotspots(self.cfg.min_occurrences)
                for h in hotspots:
                    level_clusters = self.matrix.active_levels.get(h["price"], [])
                    refined = self.engine.post_classify(h["price"], level_clusters)
                    if current_batch_id:
                        for c in level_clusters:
                            if c.batch_id == current_batch_id:
                                c.behavior_signature = refined
                            
            # Persiste qualquer cluster que teve a assinatura elevada no DuckDB
            upgraded = [
                c for c in clusters
                if c.behavior_signature.value != original_signatures.get(id(c))
            ]
            if upgraded:
                self.repo.begin()
                try:
                    self.repo.conn.executemany(
                        """UPDATE liquidity_clusters
                           SET behavior_signature = ?
                           WHERE symbol = ? AND timestamp = ? AND price = ? AND batch_id = ?""",
                        [(c.behavior_signature.value, c.symbol, c.timestamp, c.price, c.batch_id) for c in upgraded]
                    )
                    self.repo.commit()
                except Exception:
                    self.repo.rollback()
                    raise
        except Exception:
            self.matrix.restore(snap)
            raise

        # Invalida cache do narrator para refletir novos dados
        if self.narrator is not None:
            self.narrator.invalidate_cache()

        return clusters
