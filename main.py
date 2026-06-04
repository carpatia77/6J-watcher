from __future__ import annotations
"""
main.py
-------
Entrypoint do 6J Watcher.

- Sobe servidor HTTP em 127.0.0.1:8765
- Recebe payloads do MQL bridge
- Orquestra ingestão completa
- Expõe /report e /hotspots para inspeção em tempo real
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from config import Config, BASE_DIR
from ingestion import IngestionService
from liquidity_matrix import LiquidityMatrix
from narrator import Narrator
from adaptive_pattern_engine import AdaptivePatternEngine
from repository_duckdb import DuckDBRepository

cfg     = Config()
repo    = DuckDBRepository(cfg.db_path)
matrix  = LiquidityMatrix(cfg.symbol, cfg.tick_size)
engine  = AdaptivePatternEngine(profile_path=str(BASE_DIR / "profile.json"))

# LLM Client — graceful degradation se NVIDIA_API_KEY não estiver configurada
llm_client = None
if cfg.nvidia_api_key:
    try:
        from llm_client import NvidiaLLMClient
        llm_client = NvidiaLLMClient(
            api_key=cfg.nvidia_api_key,
            context_model=cfg.llm_context_model,
            reasoning_model=cfg.llm_reasoning_model,
            timeout=cfg.llm_timeout_seconds,
            max_calls_per_hour=cfg.llm_max_calls_hour,
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"LLM client não disponível: {e}")

narrator = Narrator(engine=engine, cfg=cfg, llm_client=llm_client)
service = IngestionService(repo, matrix, engine, cfg, narrator=narrator)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silencia logs HTTP no terminal

    def do_POST(self):
        if self.path == "/ingest":
            length  = int(self.headers.get("Content-Length", 0))
            payload = self.rfile.read(length).decode("utf-8")
            try:
                data     = json.loads(payload)
                clusters = service.ingest_batch(
                    data.get("tape", []),
                    data.get("dom",  []),
                    data.get("symbol", cfg.symbol),
                )
                hotspots = matrix.hotspots(cfg.min_occurrences)
                self._respond(200, {"status": "ok", "clusters": len(clusters), "hotspots": len(hotspots)})
            except Exception as e:
                self._respond(500, {"status": "error", "detail": str(e)})
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/hotspots":
            h = matrix.hotspots(cfg.min_occurrences)
            self._respond(200, {"hotspots": [{**x, "first": str(x["first"]), "last": str(x["last"])} for x in h]})
        elif self.path == "/report":
            hotspots = matrix.hotspots(cfg.min_occurrences)
            sigdist  = repo.signature_distribution(cfg.symbol)
            session  = repo.session_analysis(cfg.symbol)
            report   = narrator.daily_report(cfg.symbol, hotspots, sigdist, session)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(report.encode("utf-8"))
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code: int, body: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body, default=str).encode("utf-8"))


def run_server():
    server = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    threading.Thread(target=run_server, daemon=True).start()
    print(f"6J Watcher running on http://{cfg.host}:{cfg.port}")
    print(f"  POST /ingest  — recebe payloads do MQL bridge")
    print(f"  GET  /hotspots — retorna hotspots atuais em JSON")
    print(f"  GET  /report   — retorna relatório Markdown")
    print(f"  DB   {cfg.db_path}")
    print()
    while True:
        time.sleep(30)
        matrix.prune_stale_data(hours=4)
        hotspots = matrix.hotspots(cfg.min_occurrences)
        if hotspots:
            print(f"[{__import__('datetime').datetime.utcnow().strftime('%H:%M:%S')}] "
                  f"{len(hotspots)} hotspot(s) ativos — top: {hotspots[0]}")
