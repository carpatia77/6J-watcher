"""
backtest_loader.py
------------------
Lê diretamente do backtest_8months.db via DuckDB read_only.
Não passa pelo backend HTTP — o DB pode estar sendo escrito
pelo orquestrador em paralelo (read_only garante zero bloqueio).
"""
from __future__ import annotations
import sys
import json
from pathlib import Path

# Raiz do projeto no path para importar repository_duckdb
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from typing import Dict, List

# Caminho padrão — pode ser sobrescrito via st.sidebar no futuro
DEFAULT_DB = str(Path(__file__).parent.parent.parent / "data" / "backtest_8months.db")


def _conn(db_path: str = DEFAULT_DB) -> duckdb.DuckDBPyConnection:
    """Conexão read_only: não bloqueia gravações do orquestrador."""
    return duckdb.connect(db_path, read_only=True)


def load_summary(db_path: str = DEFAULT_DB) -> Dict:
    """Métricas gerais: total clusters, eventos, hotspots únicos."""
    try:
        con = _conn(db_path)
        row = con.execute("""
            SELECT
                COUNT(*)                                       AS total_clusters,
                COUNT(DISTINCT price)                          AS unique_levels,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp))   AS days_processed,
                MIN(timestamp)                                 AS first_event,
                MAX(timestamp)                                 AS last_event
            FROM liquidity_clusters
            WHERE symbol = '6J'
        """).fetchone()
        tape_count = con.execute(
            "SELECT COUNT(*) FROM tape_events WHERE symbol = '6J'"
        ).fetchone()[0]
        con.close()
        return {
            "total_clusters":  row[0],
            "unique_levels":   row[1],
            "days_processed":  row[2],
            "first_event":     str(row[3]) if row[3] else "—",
            "last_event":      str(row[4]) if row[4] else "—",
            "total_tape_events": tape_count,
        }
    except Exception as e:
        return {"error": str(e)}


def load_signature_distribution(db_path: str = DEFAULT_DB) -> List[Dict]:
    """Distribuição de assinaturas: contagem por tipo."""
    try:
        con = _conn(db_path)
        rows = con.execute("""
            SELECT behavior_signature, COUNT(*) AS count
            FROM liquidity_clusters
            WHERE symbol = '6J'
            GROUP BY behavior_signature
            ORDER BY count DESC
        """).fetchall()
        con.close()
        return [{"signature": r[0], "count": r[1]} for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def load_session_breakdown(db_path: str = DEFAULT_DB) -> List[Dict]:
    """Volume e distribuição de assinaturas por sessão (ASIAN/LONDON/NEW_YORK)."""
    try:
        con = _conn(db_path)
        rows = con.execute("""
            SELECT
                session,
                COUNT(*)                          AS total_clusters,
                SUM(total_bid + total_ask)        AS total_volume,
                SUM(CASE WHEN behavior_signature = 'absorption_passive'   THEN 1 ELSE 0 END) AS absorptions,
                SUM(CASE WHEN behavior_signature = 'defense_line'         THEN 1 ELSE 0 END) AS defense_lines,
                SUM(CASE WHEN behavior_signature = 'breakout_genuine'     THEN 1 ELSE 0 END) AS breakouts,
                SUM(CASE WHEN behavior_signature = 'spoofing_wall'        THEN 1 ELSE 0 END) AS spoofing_walls,
                SUM(CASE WHEN behavior_signature = 'iceberg_accumulation' THEN 1 ELSE 0 END) AS icebergs_acc,
                SUM(CASE WHEN behavior_signature = 'iceberg_distribution' THEN 1 ELSE 0 END) AS icebergs_dist
            FROM liquidity_clusters
            WHERE symbol = '6J'
            GROUP BY session
            ORDER BY total_clusters DESC
        """).fetchall()
        con.close()
        cols = ["session", "total_clusters", "total_volume",
                "absorptions", "defense_lines", "breakouts",
                "spoofing_walls", "icebergs_acc", "icebergs_dist"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def load_hotspots_historical(
    db_path: str = DEFAULT_DB,
    min_occurrences: int = 5,
    limit: int = 20,
) -> List[Dict]:
    """Top hotspots históricos: níveis de preço com maior recorrência."""
    try:
        con = _conn(db_path)
        rows = con.execute(f"""
            SELECT
                ROUND(price / 0.00005) * 0.00005                        AS price_norm,
                COUNT(*)                                                AS occurrences,
                MODE(behavior_signature)                                AS dominant_signature,
                SUM(total_bid)                                          AS total_bid,
                SUM(total_ask)                                          AS total_ask,
                MIN(timestamp)                                          AS first_seen,
                MAX(timestamp)                                          AS last_seen,
                COUNT(DISTINCT DATE_TRUNC('day', timestamp))            AS active_days
            FROM liquidity_clusters
            WHERE symbol = '6J'
            GROUP BY price_norm
            HAVING COUNT(*) >= {min_occurrences}
            ORDER BY occurrences DESC
            LIMIT {limit}
        """).fetchall()
        con.close()
        cols = ["price", "occurrences", "dominant_signature",
                "total_bid", "total_ask", "first_seen", "last_seen", "active_days"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def load_monthly_progress(db_path: str = DEFAULT_DB) -> List[Dict]:
    """Progresso do backtest: clusters processados por mês."""
    try:
        con = _conn(db_path)
        rows = con.execute("""
            SELECT
                DATE_TRUNC('month', timestamp) AS month,
                COUNT(*)                       AS clusters,
                COUNT(DISTINCT price)          AS unique_levels
            FROM liquidity_clusters
            WHERE symbol = '6J'
            GROUP BY month
            ORDER BY month ASC
        """).fetchall()
        con.close()
        return [
            {"month": str(r[0])[:7], "clusters": r[1], "unique_levels": r[2]}
            for r in rows
        ]
    except Exception as e:
        return [{"error": str(e)}]

def load_calibrated_profile(
    profile_path: str = str(Path(__file__).parent.parent.parent / "data" / "profile_calibrated.json"),
) -> Dict:
    """
    Lê o profile_calibrated.json (Win Rate, PF, MFE/MAE por assinatura×sessão).
    Usado pelo Painel B sem sub-queries na UI.
    """
    try:
        with open(profile_path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": f"Profile não encontrado: {profile_path}. Execute recalibrate_clusters.py primeiro."}
    except Exception as e:
        return {"error": str(e)}


def load_hourly_heatmap(db_path: str = DEFAULT_DB) -> List[Dict]:
    """
    Heatmap hora UTC × assinatura — Painel D.
    Filtra unknown para não poluir o visual.
    """
    try:
        con = _conn(db_path)
        rows = con.execute("""
            SELECT
                EXTRACT(HOUR FROM timestamp)::INTEGER AS hour_utc,
                behavior_signature,
                COUNT(*) AS count
            FROM liquidity_clusters
            WHERE symbol = '6J'
              AND behavior_signature != 'unknown'
            GROUP BY hour_utc, behavior_signature
            ORDER BY hour_utc, count DESC
        """).fetchall()
        con.close()
        return [{"hour_utc": r[0], "signature": r[1], "count": r[2]} for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def load_cumdelta_by_level(
    db_path: str = DEFAULT_DB,
    top_n: int = 30,
) -> List[Dict]:
    """
    Pressão acumulada (cumdelta) por nível de preço normalizado — Painel C.
    Nível com cumdelta positivo alto = acumulação de compra; negativo = distribuição.
    """
    try:
        con = _conn(db_path)
        rows = con.execute(f"""
            SELECT
                ROUND(price / 0.00005) * 0.00005         AS price_norm,
                SUM(cumdelta)                            AS cumdelta_total,
                SUM(total_bid)                           AS total_bid,
                SUM(total_ask)                           AS total_ask,
                COUNT(*)                                 AS occurrences
            FROM liquidity_clusters
            WHERE symbol = '6J'
            GROUP BY price_norm
            ORDER BY ABS(SUM(cumdelta)) DESC
            LIMIT {top_n}
        """).fetchall()
        con.close()
        cols = ["price", "cumdelta_total", "total_bid", "total_ask", "occurrences"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]
