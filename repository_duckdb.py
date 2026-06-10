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

try:
    import pyarrow as pa
    _ARROW_AVAILABLE = True
except ImportError:
    _ARROW_AVAILABLE = False


def _j(obj: Any) -> str:
    return json.dumps(obj, default=str)


class DuckDBRepository:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self.conn.execute("PRAGMA threads=4")
        self.conn.execute("PRAGMA memory_limit='4GB'")
        self._init_schema()

    def begin(self):
        self.conn.execute("BEGIN TRANSACTION")

    def commit(self):
        """Commit da transação. Silencioso se não houver transação ativa."""
        try:
            self.conn.execute("COMMIT")
        except Exception:
            pass

    def rollback(self):
        """Rollback da transação. Silencioso se não houver transação ativa."""
        try:
            self.conn.execute("ROLLBACK")
        except Exception:
            pass

    def close(self):
        """Fecha conexão e libera file lock (crítico no Windows)."""
        try:
            self.conn.execute("CHECKPOINT")
            self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _init_schema(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tape_events (
            symbol       VARCHAR,
            batch_id     VARCHAR,
            timestamp    TIMESTAMP,
            timestamp_ns BIGINT,
            price        DOUBLE,
            volume       INTEGER,
            side         VARCHAR,
            raw          TEXT
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS dom_levels (
            symbol       VARCHAR,
            batch_id     VARCHAR,
            timestamp    TIMESTAMP,
            timestamp_ns BIGINT,
            price        DOUBLE,
            level_index  INTEGER,
            bid_volume   INTEGER,
            ask_volume   INTEGER,
            raw          TEXT
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidity_clusters (
            symbol             VARCHAR,
            timestamp          TIMESTAMP,
            timestamp_ns       BIGINT,
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
            batch_id           VARCHAR,
            raw_payload        TEXT
        )""")

        # Guards para bancos existentes sem as novas colunas
        for tbl, col, typedef in [
            ("tape_events",        "timestamp_ns", "BIGINT"),
            ("tape_events",        "batch_id",     "VARCHAR"),
            ("dom_levels",         "timestamp_ns", "BIGINT"),
            ("dom_levels",         "batch_id",     "VARCHAR"),
            ("liquidity_clusters", "timestamp_ns", "BIGINT"),
            ("liquidity_clusters", "batch_id",     "VARCHAR"),
        ]:
            try:
                self.conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {typedef}")
            except Exception:
                pass

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
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_reports (
            symbol      VARCHAR,
            date        DATE,
            report_text TEXT,
            PRIMARY KEY (symbol, date)
        )""")

        # Índices TIMESTAMP (produção)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_lc_price         ON liquidity_clusters(price)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_lc_timestamp     ON liquidity_clusters(timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_lc_symbol_ts     ON liquidity_clusters(symbol, timestamp)")
        self.conn.execute("DROP INDEX IF EXISTS idx_te_symbol_ts")

        # Índices BIGINT (backtest)
        # Remover indices antigos que matam o bulk_insert:
        self.conn.execute("DROP INDEX IF EXISTS idx_te_symbol_ts_ns")
        self.conn.execute("DROP INDEX IF EXISTS idx_lc_symbol_ts_ns")
        # Remover indices antigos que matam o bulk_insert:
        self.conn.execute("DROP INDEX IF EXISTS idx_dom_symbol_ts_ns")
        self.conn.execute("DROP INDEX IF EXISTS idx_dom_price")

    # ── Bulk Insert Arrow (backtest path) ─────────────────────────────────────────────

    def bulk_insert_arrow(
        self,
        symbol: str,
        batch_id: str,
        tape_rb: "pa.RecordBatch",
        dom_rb:  "pa.RecordBatch",
    ) -> None:
        """
        Insere tape_events e dom_levels via zero-copy Arrow → DuckDB.

        O RecordBatch é registrado como view temporária e inserido com
        uma única instrução SQL por tabela — sem loop Python, sem
        executemany, sem serialização de dicts.

        Campos `symbol`, `raw` e `batch_id` são injetados via SQL
        (não existem no RecordBatch do adapter).
        """
        if not _ARROW_AVAILABLE:
            raise ImportError("pyarrow não instalado. Execute: pip install pyarrow")

        # tape_events
        if tape_rb.num_rows > 0:
            self.conn.register("_tape_rb", tape_rb)
            self.conn.execute("""
                INSERT INTO tape_events
                    (symbol, batch_id, timestamp, timestamp_ns, price, volume, side, raw)
                SELECT
                    ? AS symbol,
                    ? AS batch_id,
                    timestamp::TIMESTAMP,
                    timestamp_ns,
                    price,
                    volume,
                    side,
                    '{}'  AS raw
                FROM _tape_rb
            """, [symbol, batch_id])
            self.conn.unregister("_tape_rb")

        # dom_levels
        if dom_rb.num_rows > 0:
            self.conn.register("_dom_rb", dom_rb)
            self.conn.execute("""
                INSERT INTO dom_levels
                    (symbol, batch_id, timestamp, timestamp_ns, price, level_index, bid_volume, ask_volume, raw)
                SELECT
                    ? AS symbol,
                    ? AS batch_id,
                    timestamp::TIMESTAMP,
                    timestamp_ns,
                    price,
                    level_index,
                    bid_volume,
                    ask_volume,
                    '{}'  AS raw
                FROM _dom_rb
            """, [symbol, batch_id])
            self.conn.unregister("_dom_rb")

    # ── Inserts clássicos (produção / MQL5 path) ──────────────────────────────────────

    def upsert_daily_report(self, symbol: str, date_str: str, report_text: str):
        self.conn.execute(
            """INSERT INTO daily_reports (symbol, date, report_text) VALUES (?, ?, ?)
               ON CONFLICT (symbol, date) DO UPDATE SET report_text = EXCLUDED.report_text""",
            [symbol, date_str, report_text]
        )

    def insert_tape_events(self, events: List[TapeEvent]):
        rows = [
            [
                e.symbol,
                None, # batch_id
                e.timestamp,
                e.raw.get("timestamp_ns"),
                e.price,
                e.volume,
                e.side.value,
                _j(e.raw),
            ]
            for e in events
        ]
        if rows:
            self.conn.executemany(
                "INSERT INTO tape_events (symbol, batch_id, timestamp, timestamp_ns, price, volume, side, raw) VALUES (?,?,?,?,?,?,?,?)", rows
            )

    def insert_dom_levels(self, levels: List[DOMLevel]):
        rows = [
            [
                l.symbol,
                None, # batch_id
                l.timestamp,
                l.raw.get("timestamp_ns"),
                l.price,
                l.level_index,
                l.bid_volume,
                l.ask_volume,
                _j(l.raw),
            ]
            for l in levels
        ]
        if rows:
            self.conn.executemany(
                "INSERT INTO dom_levels (symbol, batch_id, timestamp, timestamp_ns, price, level_index, bid_volume, ask_volume, raw) VALUES (?,?,?,?,?,?,?,?,?)", rows
            )

    def insert_clusters(self, clusters: List[LiquidityCluster]):
        rows = [[
            c.symbol,
            c.timestamp,
            c.raw_payload.get("timestamp_ns"),
            c.price,
            c.session,
            c.behavior_signature.value,
            c.total_ask,
            c.total_bid,
            c.cumdelta,
            c.deltamin,
            c.deltamax,
            c.delta_price_ticks,
            c.confidence,
            c.outcome,
            c.batch_id,
            _j(c.raw_payload),
        ] for c in clusters]
        if rows:
            self.conn.executemany(
                "INSERT INTO liquidity_clusters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )

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

    # ── Queries ───────────────────────────────────────────────────────────────────────

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
            f"SELECT * FROM tape_events WHERE symbol=? AND timestamp > CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL {int(minutes)} MINUTE ORDER BY timestamp DESC",
            [symbol]).fetchall()

    def recent_dom(self, symbol: str, minutes: int = 60) -> List:
        return self.conn.execute(
            f"SELECT * FROM dom_levels WHERE symbol=? AND timestamp > CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL {int(minutes)} MINUTE ORDER BY timestamp DESC",
            [symbol]).fetchall()
