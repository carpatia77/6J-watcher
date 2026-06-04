from __future__ import annotations
"""
repository_duckdb.py
--------------------
Camada de persistência histórica. Usa DuckDB como banco
colunar local. Todas as tabelas seguem o schema de schemas.sql.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
from models import DOMLevel, KeyLevel, LiquidityCluster, TapeEvent


def _j(obj: Any) -> str:
    return json.dumps(obj, default=str)


class DuckDBRepository:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def begin(self):
        self.conn.execute("BEGIN TRANSACTION")

    def commit(self):
        self.conn.execute("COMMIT")

    def rollback(self):
        self.conn.execute("ROLLBACK")

    def _init_schema(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tape_events (
            symbol    VARCHAR,
            timestamp TIMESTAMP,
            price     DOUBLE,
            volume    INTEGER,
            side      VARCHAR,
            raw       TEXT
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS dom_levels (
            symbol      VARCHAR,
            timestamp   TIMESTAMP,
            price       DOUBLE,
            level_index INTEGER,
            bid_volume  INTEGER,
            ask_volume  INTEGER,
            raw         TEXT
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidity_clusters (
            symbol             VARCHAR,
            timestamp          TIMESTAMP,
            price              DOUBLE,
            session            VARCHAR,
            behavior_signature VARCHAR,
            total_ask          INTEGER,
            total_bid          INTEGER,
            cumdelta           INTEGER,
            deltamin           INTEGER,
            deltamax           INTEGER,
            delta_price_ticks  INTEGER,
            confidence         DOUBLE,
            outcome            VARCHAR,
            raw_payload        TEXT
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS key_levels (
            symbol             VARCHAR,
            price              DOUBLE,
            occurrences        INTEGER,
            first_seen         TIMESTAMP,
            last_seen          TIMESTAMP,
            dominant_signature VARCHAR,
            days_active        INTEGER,
            reliability_score  DOUBLE,
            PRIMARY KEY (symbol, price)
        )""")

    # ── Inserts ──────────────────────────────────────────────────────────────

    def insert_tape_events(self, events: List[TapeEvent]):
        rows = [[e.symbol, e.timestamp, e.price, e.volume, e.side.value, _j(e.raw)] for e in events]
        if rows:
            self.conn.executemany("INSERT INTO tape_events VALUES (?,?,?,?,?,?)", rows)

    def insert_dom_levels(self, levels: List[DOMLevel]):
        rows = [[l.symbol, l.timestamp, l.price, l.level_index, l.bid_volume, l.ask_volume, _j(l.raw)] for l in levels]
        if rows:
            self.conn.executemany("INSERT INTO dom_levels VALUES (?,?,?,?,?,?,?)", rows)

    def insert_clusters(self, clusters: List[LiquidityCluster]):
        rows = [[
            c.symbol, c.timestamp, c.price, c.session,
            c.behavior_signature.value, c.total_ask, c.total_bid,
            c.cumdelta, c.deltamin, c.deltamax, c.delta_price_ticks,
            c.confidence, c.outcome, _j(c.raw_payload)
        ] for c in clusters]
        if rows:
            self.conn.executemany("INSERT INTO liquidity_clusters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

    def upsert_key_level(self, level: KeyLevel):
        self.conn.execute(
            """INSERT INTO key_levels 
               VALUES (?,?,?,?,?,?,?,?) 
               ON CONFLICT (symbol, price) DO UPDATE SET 
               occurrences=EXCLUDED.occurrences, 
               first_seen=EXCLUDED.first_seen, 
               last_seen=EXCLUDED.last_seen, 
               dominant_signature=EXCLUDED.dominant_signature, 
               days_active=EXCLUDED.days_active, 
               reliability_score=EXCLUDED.reliability_score""",
            [level.symbol, level.price, level.occurrences, level.first_seen,
             level.last_seen, level.dominant_signature, level.days_active, level.reliability_score])

    # ── Queries ──────────────────────────────────────────────────────────────

    def signature_distribution(self, symbol: str) -> List:
        return self.conn.execute(
            "SELECT behavior_signature, COUNT(*) cnt FROM liquidity_clusters WHERE symbol=? GROUP BY 1 ORDER BY cnt DESC",
            [symbol]).fetchall()

    def recurring_levels(self, symbol: str, min_occurrences: int = 3) -> List:
        return self.conn.execute(
            """SELECT price, COUNT(*) occ,
                      MIN(timestamp) first_seen, MAX(timestamp) last_seen,
                      MODE(behavior_signature) dominant
               FROM liquidity_clusters WHERE symbol=?
               GROUP BY price HAVING COUNT(*)>=?
               ORDER BY occ DESC""",
            [symbol, min_occurrences]).fetchall()

    def session_analysis(self, symbol: str) -> Dict:
        rows = self.conn.execute(
            "SELECT session, behavior_signature, COUNT(*) cnt FROM liquidity_clusters WHERE symbol=? GROUP BY 1,2",
            [symbol]).fetchall()
        result: Dict = {}
        for session, sig, cnt in rows:
            result.setdefault(session, {})[sig] = cnt
        return result

    def recent_tape(self, symbol: str, minutes: int = 60) -> List:
        return self.conn.execute(
            "SELECT * FROM tape_events WHERE symbol=? AND timestamp > NOW() - INTERVAL ? MINUTE ORDER BY timestamp DESC",
            [symbol, minutes]).fetchall()

    def recent_dom(self, symbol: str, minutes: int = 60) -> List:
        return self.conn.execute(
            "SELECT * FROM dom_levels WHERE symbol=? AND timestamp > NOW() - INTERVAL ? MINUTE ORDER BY timestamp DESC",
            [symbol, minutes]).fetchall()
