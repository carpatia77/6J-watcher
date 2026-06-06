"""relatorio_mes1.py — consulta resultados do mes 1 direto do DuckDB."""
import duckdb
import sys

DB_PATH = "./data/backtest_8months.db"

try:
    conn = duckdb.connect(DB_PATH, read_only=True)
except Exception as e:
    print(f"Erro ao abrir banco: {e}")
    print(f"Verifique se o arquivo existe em: {DB_PATH}")
    sys.exit(1)

print("=" * 60)
print("  RESULTADO PARCIAL - MES 1 (Out/2025)")
print("=" * 60)

# Total
total = conn.execute("SELECT COUNT(*) FROM liquidity_clusters WHERE symbol='6J'").fetchone()[0]
print(f"\nTotal clusters: {total:,}")

# Periodo
row = conn.execute("""
    SELECT MIN(timestamp), MAX(timestamp),
           COUNT(DISTINCT CAST(timestamp AS DATE)) as dias
    FROM liquidity_clusters WHERE symbol='6J'
""").fetchone()
print(f"Periodo:        {row[0]} -> {row[1]}")
print(f"Dias com dados: {row[2]}")

# Assinaturas
print("\n--- Distribuicao de Assinaturas ---")
rows = conn.execute("""
    SELECT behavior_signature,
           COUNT(*) as cnt,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
    FROM liquidity_clusters WHERE symbol='6J'
    GROUP BY behavior_signature
    ORDER BY cnt DESC
""").fetchall()
for sig, cnt, pct in rows:
    bar = "#" * int(pct / 2)
    print(f"  {sig:<32} {cnt:>7,}  {pct:>5.1f}%  {bar}")

# Sessoes
print("\n--- Clusters por Sessao ---")
rows = conn.execute("""
    SELECT session, COUNT(*) as cnt,
           ROUND(AVG(confidence), 3) as avg_conf,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
    FROM liquidity_clusters WHERE symbol='6J'
    GROUP BY session
    ORDER BY cnt DESC
""").fetchall()
for sess, cnt, conf, pct in rows:
    print(f"  {sess:<12} {cnt:>7,} clusters  {pct:>5.1f}%  conf_media={conf}")

# Hotspots
print("\n--- Top 15 Hotspots de Preco (>= 5 ocorrencias) ---")
rows = conn.execute("""
    SELECT price,
           COUNT(*) as occ,
           MODE(behavior_signature) as dominant_sig,
           ROUND(AVG(confidence), 3) as avg_conf,
           ROUND(AVG(CAST(total_bid AS DOUBLE)), 0) as avg_bid,
           ROUND(AVG(CAST(total_ask AS DOUBLE)), 0) as avg_ask
    FROM liquidity_clusters WHERE symbol='6J'
    GROUP BY price
    HAVING COUNT(*) >= 5
    ORDER BY occ DESC
    LIMIT 15
""").fetchall()
for price, occ, sig, conf, avg_bid, avg_ask in rows:
    print(f"  {price:.6f}  {occ:>4}x  {sig:<32} conf={conf}  bid={int(avg_bid or 0)}  ask={int(avg_ask or 0)}")

# Delta medio por sessao
print("\n--- CumDelta Medio por Sessao ---")
rows = conn.execute("""
    SELECT session,
           ROUND(AVG(cumdelta), 1) as avg_delta,
           ROUND(AVG(ABS(cumdelta)), 1) as avg_abs_delta,
           MIN(cumdelta) as min_delta,
           MAX(cumdelta) as max_delta
    FROM liquidity_clusters WHERE symbol='6J'
    GROUP BY session
    ORDER BY avg_abs_delta DESC
""").fetchall()
for sess, avg_d, avg_abs, mn, mx in rows:
    print(f"  {sess:<12} avg={avg_d:>8.1f}  abs_avg={avg_abs:>7.1f}  range=[{mn}, {mx}]")

print("\n" + "=" * 60)
conn.close()
print("Consulta concluida.")
