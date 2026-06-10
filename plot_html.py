import duckdb
import json

DB_PATH = 'data/backtest_8months.db'
con = duckdb.connect(DB_PATH, read_only=True)

query = """
    SELECT 
        timestamp_ns, 
        total_bid, 
        total_ask, 
        cumdelta, 
        deltamin, 
        deltamax
    FROM liquidity_clusters 
    ORDER BY timestamp_ns
    LIMIT 150 OFFSET 1000
"""

rows = con.execute(query).fetchall()

labels = []
cumdelta = []
deltamin = []
deltamax = []
bid = []
ask = []

for r in rows:
    labels.append(str(r[0]))
    bid.append(r[1])
    ask.append(-r[2])
    cumdelta.append(r[3])
    deltamin.append(r[4])
    deltamax.append(r[5])

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Cluster Validation</title>
    <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
</head>
<body>
    <div id="plot1" style="width:100%;height:500px;"></div>
    <div id="plot2" style="width:100%;height:300px;"></div>
    <script>
        var labels = {json.dumps(labels)};
        
        var trace_cumdelta = {{
            x: labels, y: {json.dumps(cumdelta)},
            name: 'CVD', type: 'scatter', line: {{color: 'blue'}}
        }};
        var trace_max = {{
            x: labels, y: {json.dumps(deltamax)},
            name: 'DeltaMax', type: 'scatter', line: {{color: 'lightblue'}}, fill: 'tonexty'
        }};
        var trace_min = {{
            x: labels, y: {json.dumps(deltamin)},
            name: 'DeltaMin', type: 'scatter', line: {{color: 'lightblue'}}, fill: 'tonexty'
        }};

        Plotly.newPlot('plot1', [trace_min, trace_cumdelta, trace_max], {{title: 'Cumulative Volume Delta'}});

        var trace_bid = {{
            x: labels, y: {json.dumps(bid)},
            name: 'Bid Volume', type: 'bar', marker: {{color: 'green'}}
        }};
        var trace_ask = {{
            x: labels, y: {json.dumps(ask)},
            name: 'Ask Volume', type: 'bar', marker: {{color: 'red'}}
        }};

        Plotly.newPlot('plot2', [trace_bid, trace_ask], {{title: 'Volume', barmode: 'relative'}});
    </script>
</body>
</html>
"""

with open("validation_plot.html", "w") as f:
    f.write(html_content)

print("[+] validation_plot.html gerado com sucesso!")
