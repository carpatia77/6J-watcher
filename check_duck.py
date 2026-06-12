import duckdb
try:
    con = duckdb.connect('/home/aidea/data_backtest/backtest_8months.db', read_only=True)
    print('TAPE EVENTS:', con.execute('SELECT count(*) FROM tape_events').fetchone()[0])
    print('DOM LEVELS:', con.execute('SELECT count(*) FROM dom_levels').fetchone()[0])
    print('CLUSTERS:', con.execute('SELECT count(*) FROM liquidity_clusters').fetchone()[0])
except Exception as e:
    print('Error:', e)
