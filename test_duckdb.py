import duckdb

conn = duckdb.connect(':memory:')
conn.execute("CREATE TABLE test (symbol VARCHAR, val INTEGER)")
conn.execute("INSERT INTO test VALUES ('AAPL', 10), ('MSFT', 20)")

try:
    print("Testing conn.sql with params...")
    rel = conn.sql("SELECT * FROM test WHERE symbol = ?", params=['AAPL'])
    print("conn.sql(..., params=[]) returned:", type(rel))
    res = rel.query("alias", "SELECT val * 2 AS v FROM alias")
    print(res.fetchdf())
    print("SUCCESS: conn.sql() supports params and returns a relation.")
except Exception as e:
    print("conn.sql with params failed:", e)

try:
    print("Testing conn.execute ...")
    rel2 = conn.execute("SELECT * FROM test WHERE symbol = ?", ['AAPL'])
    print("conn.execute returned:", type(rel2))
    # Let's see if rel2 has a .query() method that takes 2 args
    try:
        res2 = rel2.query("alias", "SELECT val * 2 AS v FROM alias")
        print(res2.fetchdf())
        print("SUCCESS: conn.execute() returns an object with .query('alias', ...)")
    except Exception as e:
        print("rel2.query('alias', ...) failed:", e)
except Exception as e:
    print("conn.execute failed:", e)
