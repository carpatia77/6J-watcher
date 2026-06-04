from __future__ import annotations
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from config import Config
from repository_duckdb import DuckDBRepository
from liquidity_matrix import LiquidityMatrix
from pattern_engine import PatternEngine
from narrator import Narrator
from ingestion import IngestionService

cfg = Config()
repo = DuckDBRepository(cfg.db_path)
matrix = LiquidityMatrix(cfg.symbol, cfg.tick_size)
engine = PatternEngine()
narrator = Narrator()
service = IngestionService(repo, matrix, engine)

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/ingest":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        payload = self.rfile.read(length).decode("utf-8")
        data = json.loads(payload)
        tape_rows, dom_rows = data.get("tape", []), data.get("dom", [])
        clusters = service.ingest_batch(tape_rows, dom_rows, data.get("symbol", cfg.symbol))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "clusters": len(clusters), "hotspots": matrix.hotspots(cfg.min_occurrences)}).encode())


def run_server():
    server = HTTPServer(("127.0.0.1", 8765), Handler)
    server.serve_forever()


def main():
    Thread(target=run_server, daemon=True).start()
    print("HTTP ingest server running on 127.0.0.1:8765/ingest")
    print("Waiting for ClusterDelta bridge payloads...")
    import time
    while True:
        time.sleep(2)

if __name__ == "__main__":
    main()
