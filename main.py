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

    if llm_client is not None:
        import atexit
        import asyncio
        atexit.register(lambda: asyncio.run(llm_client.close()))

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


def background_scheduler():
    import logging
    from datetime import datetime, timezone
    
    last_prune = time.time()
    last_profile = time.time()
    last_daily_report_date = None

    while True:
        time.sleep(30)
        now = time.time()
        
        # 1. Prune stale data a cada 30 segundos
        if now - last_prune >= 30:
            try:
                matrix.prune_stale_data(hours=4)
            except Exception as e:
                logging.getLogger(__name__).error(f"Erro no prune: {e}")
            last_prune = now
            
        # 2. Gerar profile.json a cada 30 minutos (1800s)
        if now - last_profile >= 1800:
            try:
                from signature_profiler import SignatureProfiler
                profiler = SignatureProfiler(cfg.db_path)
                profile_data = profiler.build_profile(cfg.symbol)
                profiler.save_profile(profile_data, str(BASE_DIR / "profile.json"))
                engine.profile = engine._load_profile(str(BASE_DIR / "profile.json"))
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Profile recalibrado automaticamente.")
                last_profile = now
            except Exception as e:
                logging.getLogger(__name__).error(f"[Profiler] Lock detectado, retry em 60s: {e}")
                last_profile = now - 1740  # Tenta de novo em 60s

        # 3. Gerar relatório de fechamento de mercado (Whale Dynamics)
        # Assumindo fechamento CME às 22h UTC (17h EST)
        current_dt = datetime.now(timezone.utc)
        current_date_str = current_dt.strftime("%Y-%m-%d")
        
        if current_dt.hour == 22 and current_dt.minute < 5 and last_daily_report_date != current_date_str:
            try:
                hotspots = matrix.hotspots(cfg.min_occurrences)
                sigdist  = repo.signature_distribution(cfg.symbol)
                session  = repo.session_analysis(cfg.symbol)
                
                report_text = narrator.daily_report(cfg.symbol, hotspots, sigdist, session)
                
                if llm_client is not None:
                    try:
                        import asyncio
                        llm_narrative = asyncio.run(narrator.generate_narrative(report_text))
                        report_text += f"\n\n## Chief Quant Orchestrator (LLM Analysis)\n\n{llm_narrative}"
                    except Exception as e_llm:
                        logging.getLogger(__name__).error(f"Erro no LLM do fechamento: {e_llm}")

                repo.upsert_daily_report(cfg.symbol, current_date_str, report_text)
                print(f"[{current_dt.strftime('%H:%M:%S')}] Relatório Institucional de Fechamento salvo no DB.")
                last_daily_report_date = current_date_str
            except Exception as e:
                logging.getLogger(__name__).error(f"Erro no relatório de fechamento: {e}")


if __name__ == "__main__":
    threading.Thread(target=run_server, daemon=True).start()
    threading.Thread(target=background_scheduler, daemon=True).start()
    
    print(f"6J Watcher running on http://{cfg.host}:{cfg.port}")
    print(f"  POST /ingest  — recebe payloads do MQL bridge")
    print(f"  GET  /hotspots — retorna hotspots atuais em JSON")
    print(f"  GET  /report   — retorna relatório Markdown")
    print(f"  DB   {cfg.db_path}")
    print(f"  Scheduler ativo (Prune: 30s | Profile: 30m | Report: 22h UTC)")
    print()
    
    # Mantém o processo vivo e pode printar status esporádico
    while True:
        time.sleep(60)
        hotspots = matrix.hotspots(cfg.min_occurrences)
        if hotspots:
            from datetime import datetime, timezone
            import json
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"{len(hotspots)} hotspot(s) ativos — top: {json.dumps(hotspots[0], default=str)}")
