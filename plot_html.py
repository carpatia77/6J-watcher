import duckdb
import json
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

DB_PATH = '/home/aidea/data_backtest/backtest_8months.db'
con = duckdb.connect(DB_PATH, read_only=True)

query = """
    SELECT 
        timestamp_ns, 
        total_bid, 
        total_ask, 
        cumdelta, 
        deltamin, 
        deltamax,
        behavior_signature
    FROM liquidity_clusters 
    ORDER BY timestamp_ns
    LIMIT 200 OFFSET 2000
"""

rows = con.execute(query).fetchall()

labels = []
cumdelta = []
deltamin = []
deltamax = []
bid = []
ask = []
signatures = []

tz = ZoneInfo('America/Chicago')

for r in rows:
    ts_ns = r[0]
    dt_utc = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    dt_chi = dt_utc.astimezone(tz)
    
    labels.append(dt_chi.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    bid.append(r[1])
    ask.append(-r[2])  # negative for stacked bar visualization
    cumdelta.append(r[3])
    deltamin.append(r[4])
    deltamax.append(r[5])
    signatures.append(r[6] if r[6] else "UNKNOWN")

# Compute 90th percentile of CVD (absolute)
abs_cvd = sorted([abs(c) for c in cumdelta])
p90 = abs_cvd[int(len(abs_cvd) * 0.90)] if abs_cvd else 0

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Cluster Validation (Dark Mode)</title>
    <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
    <style>
        body {{ background-color: #0d1117; color: white; font-family: sans-serif; }}
    </style>
</head>
<body>
    <div id="plot1" style="width:100%;height:600px;"></div>
    <div id="plot2" style="width:100%;height:300px;"></div>
    <script>
        var labels = {json.dumps(labels)};
        var cumdelta = {json.dumps(cumdelta)};
        var deltamin = {json.dumps(deltamin)};
        var deltamax = {json.dumps(deltamax)};
        var signatures = {json.dumps(signatures)};
        var p90 = {p90};
        
        // Color mapping for behavior signatures
        var sig_colors = signatures.map(sig => {{
            var s = String(sig).toUpperCase();
            if (s.includes("ABSORPTION")) return "orange";
            if (s.includes("VACUUM")) return "red";
            if (s.includes("BUY")) return "lime";
            if (s.includes("SELL")) return "fuchsia";
            return "gray";
        }});

        // Traces for CVD and Bands
        var trace_cvd_baseline1 = {{x: labels, y: cumdelta, type: 'scatter', mode: 'lines', line: {{width: 0}}, showlegend: false, hoverinfo: 'skip'}};
        var trace_max = {{
            x: labels, y: deltamax,
            name: 'DeltaMax', type: 'scatter', mode: 'lines',
            line: {{width: 0}}, fill: 'tonexty', fillcolor: 'rgba(0, 200, 100, 0.25)'
        }};
        
        var trace_cvd_baseline2 = {{x: labels, y: cumdelta, type: 'scatter', mode: 'lines', line: {{width: 0}}, showlegend: false, hoverinfo: 'skip'}};
        var trace_min = {{
            x: labels, y: deltamin,
            name: 'DeltaMin', type: 'scatter', mode: 'lines',
            line: {{width: 0}}, fill: 'tonexty', fillcolor: 'rgba(220, 50, 50, 0.25)'
        }};

        var trace_cvd = {{
            x: labels, y: cumdelta,
            name: 'CVD Line', type: 'scatter', 
            mode: 'lines+markers',
            line: {{color: '#FF6B35', width: 2}},
            marker: {{color: sig_colors, size: 6}},
            text: signatures
        }};

        // Dummy traces for the Legend
        var leg_abs = {{x: [null], y: [null], name: '🟢 Absorção', type: 'scatter', mode: 'markers', marker: {{color: 'orange', size: 8}}}};
        var leg_vac = {{x: [null], y: [null], name: '🔴 Vacuum', type: 'scatter', mode: 'markers', marker: {{color: 'red', size: 8}}}};
        var leg_mbuy = {{x: [null], y: [null], name: '🚀 Momentum Compra', type: 'scatter', mode: 'markers', marker: {{color: 'lime', size: 8}}}};
        var leg_msell = {{x: [null], y: [null], name: '☄️ Momentum Venda', type: 'scatter', mode: 'markers', marker: {{color: 'fuchsia', size: 8}}}};
        var leg_unk = {{x: [null], y: [null], name: '⚪ Não classificado', type: 'scatter', mode: 'markers', marker: {{color: 'gray', size: 8}}}};

        var layout1 = {{
            title: 'Cumulative Volume Delta (CVD) vs DeltaMin/Max',
            plot_bgcolor: '#0d1117',
            paper_bgcolor: '#0d1117',
            font: {{color: 'white'}},
            xaxis: {{gridcolor: 'rgba(255,255,255,0.05)', showgrid: true}},
            yaxis: {{gridcolor: 'rgba(255,255,255,0.05)', showgrid: true}},
            shapes: [
                {{type: 'line', y0: p90, y1: p90, x0: labels[0], x1: labels[labels.length-1], line: {{color: 'lime', dash: 'dash', width: 1}}}},
                {{type: 'line', y0: -p90, y1: -p90, x0: labels[0], x1: labels[labels.length-1], line: {{color: 'red', dash: 'dash', width: 1}}}}
            ]
        }};

        Plotly.newPlot('plot1', [trace_cvd_baseline1, trace_max, trace_cvd_baseline2, trace_min, trace_cvd, leg_abs, leg_vac, leg_mbuy, leg_msell, leg_unk], layout1);

        // Traces for Volume
        var trace_bid = {{
            x: labels, y: {json.dumps(bid)},
            name: 'Bid Volume', type: 'bar', marker: {{color: '#00C864'}}
        }};
        var trace_ask = {{
            x: labels, y: {json.dumps(ask)},
            name: 'Ask Volume', type: 'bar', marker: {{color: '#DC3232'}}
        }};

        var layout2 = {{
            title: 'Cluster Volume (Bid vs Ask)',
            barmode: 'relative',
            plot_bgcolor: '#0d1117',
            paper_bgcolor: '#0d1117',
            font: {{color: 'white'}},
            xaxis: {{gridcolor: 'rgba(255,255,255,0.05)', showgrid: true}},
            yaxis: {{gridcolor: 'rgba(255,255,255,0.05)', showgrid: true}}
        }};

        Plotly.newPlot('plot2', [trace_bid, trace_ask], layout2);
    </script>
</body>
</html>
"""

with open("validation_plot.html", "w") as f:
    f.write(html_content)

print("[+] validation_plot.html dark mode gerado com sucesso!")
