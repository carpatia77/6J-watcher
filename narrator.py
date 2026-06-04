from __future__ import annotations
"""
narrator.py
-----------
Chief Quant Orchestrator — Transforma a inteligência acumulada dos módulos
SignatureProfiler + AdaptivePatternEngine + LiquidityMatrix em decisões
acionáveis para o trader.

Camadas:
  1. Smart Alerts    — Filtra sinais por Tier, Win Rate e Sample Size
  2. Confluências    — Detecta padrões compostos de alta probabilidade
  3. LLM Integration — Narrativa institucional via NVIDIA (Llama 3B + DeepSeek V4)
  4. Caching         — Evita reprocessamento em múltiplas chamadas ao /report
"""
import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from adaptive_pattern_engine import AdaptivePatternEngine
from config import Config

logger = logging.getLogger(__name__)


class Narrator:
    """Orquestrador cognitivo da última milha do pipeline 6J Watcher."""

    def __init__(self, engine: AdaptivePatternEngine, cfg: Optional[Config] = None,
                 llm_client=None):
        self.engine = engine
        self.cfg = cfg or Config()
        self.llm_client = llm_client  # Optional — graceful degradation se None
        self._report_cache: Dict[str, str] = {}

    # ──────────────────────────────────────────────
    #  P0: SMART ALERTS
    # ──────────────────────────────────────────────

    def smart_alert(self, price: float, signature: str, occurrences: int,
                    volume: int, session: str) -> Optional[str]:
        """
        Gera alerta apenas se o sinal tiver edge estatístico comprovado.
        Retorna None para sinais de baixa qualidade (Tier 3, amostras insuficientes,
        win rate abaixo do limiar).
        """
        quality = self.engine.get_signal_quality(signature, session)

        if quality["tier"] == 3:
            logger.debug(f"[Narrator] Suprimido alerta Tier 3: {signature}")
            return None

        if quality["sample_size"] < self.cfg.min_alert_sample_size:
            logger.debug(f"[Narrator] Amostra insuficiente para {signature}: "
                         f"{quality['sample_size']} < {self.cfg.min_alert_sample_size}")
            return None

        if quality["historical_win_rate"] < self.cfg.min_alert_win_rate:
            logger.debug(f"[Narrator] Win rate baixo para {signature}: "
                         f"{quality['historical_win_rate']:.1%} < {self.cfg.min_alert_win_rate:.1%}")
            return None

        return (
            f"🔔 HIGH-QUALITY PATTERN\n"
            f"   Price:        {price:.5f}\n"
            f"   Signature:    {signature} (Tier {quality['tier']})\n"
            f"   Occurrences:  {occurrences}\n"
            f"   Volume:       {volume}\n"
            f"   Win Rate:     {quality['historical_win_rate']:.1%}\n"
            f"   Profit Factor:{quality['profit_factor']:.2f}\n"
            f"   Sample Size:  {quality['sample_size']}"
        )

    def alert(self, price: float, signature: str, occurrences: int,
              volume: int, confidence: float) -> str:
        """Legacy alert — mantido para backward compatibility."""
        return (
            f"🔔 PATTERN DETECTED\n"
            f"   Price:       {price:.5f}\n"
            f"   Signature:   {signature}\n"
            f"   Occurrences: {occurrences}\n"
            f"   Volume:      {volume}\n"
            f"   Confidence:  {confidence:.2f}"
        )

    # ──────────────────────────────────────────────
    #  P0: DETECÇÃO DE CONFLUÊNCIAS
    # ──────────────────────────────────────────────

    def detect_confluences(self, hotspots: List[Dict]) -> List[Dict]:
        """
        Detecta padrões compostos de alta probabilidade cruzando hotspots
        por proximidade de preço.

        Confluências detectadas:
          - BREAKOUT_AT_DEFENSE: Breakout Genuine em nível com Defense Line
          - ACCUMULATION_ABSORPTION: Iceberg Accumulation + Absorption Passive
        """
        if len(hotspots) < 2:
            return []

        tick_tol = self.cfg.confluence_tick_tolerance * self.cfg.tick_size
        confluences: List[Dict] = []

        for i, h1 in enumerate(hotspots):
            for h2 in hotspots[i + 1:]:
                price_dist = abs(h1.get("price", 0) - h2.get("price", 0))
                if price_dist > tick_tol:
                    continue

                sig1 = h1.get("dominant_signature", "")
                sig2 = h2.get("dominant_signature", "")
                pair = frozenset([sig1, sig2])

                # BREAKOUT em DEFENSE_LINE
                if pair == frozenset(["breakout_genuine", "defense_line"]):
                    confluences.append({
                        "type": "BREAKOUT_AT_DEFENSE",
                        "price": h1["price"],
                        "components": [sig1, sig2],
                        "interpretation": (
                            "Rompimento válido em nível defendido — "
                            "alta probabilidade de continuação direcional"
                        ),
                    })

                # ACCUMULATION + ABSORPTION = reversão iminente
                if pair == frozenset(["iceberg_accumulation", "absorption_passive"]):
                    confluences.append({
                        "type": "ACCUMULATION_ABSORPTION",
                        "price": h1["price"],
                        "components": [sig1, sig2],
                        "interpretation": (
                            "Acumulação institucional com absorção passiva — "
                            "reversão iminente no nível"
                        ),
                    })

                # DISTRIBUTION + ABSORPTION = teto institucional
                if pair == frozenset(["iceberg_distribution", "absorption_passive"]):
                    confluences.append({
                        "type": "DISTRIBUTION_ABSORPTION",
                        "price": h1["price"],
                        "components": [sig1, sig2],
                        "interpretation": (
                            "Distribuição institucional com absorção passiva — "
                            "teto de preço ativo, evitar compras"
                        ),
                    })

        if confluences:
            logger.info(f"[Narrator] {len(confluences)} confluência(s) detectada(s)")

        return confluences

    # ──────────────────────────────────────────────
    #  RELATÓRIO DIÁRIO (com Confluências + Cache)
    # ──────────────────────────────────────────────

    def daily_report(
        self,
        symbol: str,
        hotspots: List[Dict],
        signature_distribution: List,
        session_analysis: Dict,
        notable_events: Optional[List[Dict]] = None,
    ) -> str:
        """Gera relatório Markdown com hotspots, confluências, distribuição e sessão."""
        notable_events = notable_events or []

        # --- P2: Cache ---
        cache_key = hashlib.md5(
            f"{symbol}:{len(hotspots)}:{str(signature_distribution)}"
            f":{str(session_analysis)}".encode()
        ).hexdigest()

        if cache_key in self._report_cache:
            logger.debug("[Narrator] Cache hit para daily_report")
            return self._report_cache[cache_key]

        report = self._generate_report(
            symbol, hotspots, signature_distribution, session_analysis, notable_events
        )
        self._report_cache[cache_key] = report
        return report

    def invalidate_cache(self):
        """Limpa cache quando novos dados chegam via ingest_batch."""
        self._report_cache.clear()

    def _generate_report(
        self,
        symbol: str,
        hotspots: List[Dict],
        signature_distribution: List,
        session_analysis: Dict,
        notable_events: List[Dict],
    ) -> str:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        lines = [
            f"# 6J Watcher — Intelligence Report",
            f"**Symbol:** {symbol}  |  **Date:** {today} UTC",
            "",
            "---",
            "",
            "## Executive Summary",
            "This report summarizes institutional aggression and passive intention",
            "captured from ClusterDelta footprint and DOM data.",
            "",
            "---",
            "",
        ]

        # --- Confluências (P0) ---
        confluences = self.detect_confluences(hotspots)
        if confluences:
            lines.append("## ⚡ Confluências de Alta Probabilidade")
            lines.append("")
            lines.append("| Tipo | Preço | Componentes | Interpretação |")
            lines.append("|------|-------|-------------|---------------|")
            for cf in confluences:
                comps = " + ".join(cf["components"])
                price_str = f"{cf['price']:.5f}" if isinstance(cf["price"], (int, float)) else "?"
                lines.append(
                    f"| {cf['type']} | {price_str} | {comps} | {cf['interpretation']} |"
                )
            lines += ["", "---", ""]

        # --- Hotspots ---
        lines.append("## Institutional Hotspots")
        if hotspots:
            lines.append("| Price | Occurrences | Dominant Signature | Avg Confidence |")
            lines.append("|-------|-------------|-------------------|----------------|")
            for h in hotspots[:15]:
                lines.append(
                    f"| {h['price']:.5f} | {h['occurrences']} | "
                    f"{h.get('dominant_signature', '?')} | {h.get('avg_confidence', 0):.2f} |"
                )
        else:
            lines.append("No hotspots detected yet.")

        lines += ["", "---", "", "## Signature Distribution"]
        if signature_distribution:
            lines.append("| Signature | Count |")
            lines.append("|-----------|-------|")
            for row in signature_distribution:
                lines.append(f"| {row[0]} | {row[1]} |")
        else:
            lines.append("No data.")

        lines += ["", "---", "", "## Session Analysis"]
        if session_analysis:
            for session, sigs in session_analysis.items():
                lines.append(f"### {session.upper()}")
                for sig, cnt in sigs.items():
                    lines.append(f"- {sig}: {cnt}")
        else:
            lines.append("No session data yet.")

        lines += ["", "---", "", "## Notable Events"]
        if notable_events:
            for e in notable_events[:10]:
                lines.append(f"- {e}")
        else:
            lines.append("No notable events.")

        return "\n".join(lines)

    # ──────────────────────────────────────────────
    #  LEVEL SUMMARY
    # ──────────────────────────────────────────────

    def level_summary(self, price_matrix: Dict) -> str:
        p = price_matrix
        price = p.get("price")
        price_str = f"{price:.5f}" if isinstance(price, (int, float)) else "?"
        lines = [
            f"## Level {price_str}",
            f"- Clusters:   {p.get('cluster_count', 0)}",
            f"- Tape events:{p.get('tape_count', 0)}",
            f"- DOM levels: {p.get('dom_count', 0)}",
            f"- DOM bid:    {p.get('dom_total_bid', 0)}",
            f"- DOM ask:    {p.get('dom_total_ask', 0)}",
            f"- Tape vol:   {p.get('tape_total_volume', 0)}",
            f"- Signatures: {p.get('cluster_signature_distribution', {})}",
            f"- First seen: {p.get('first_seen', 'n/a')}",
            f"- Last seen:  {p.get('last_seen', 'n/a')}",
        ]
        return "\n".join(lines)

    # ──────────────────────────────────────────────
    #  P1: LLM INTEGRATION (NVIDIA)
    # ──────────────────────────────────────────────

    async def generate_narrative(self, symbol: str, hotspots: List[Dict]) -> str:
        """
        Gera narrativa institucional usando o pipeline LLM de 2 estágios:
          1. Llama 3B (context): estrutura dados brutos em contexto
          2. DeepSeek V4 (reasoning): raciocina sobre o contexto
        Fallback: template local se LLM não estiver disponível.
        """
        if not self.llm_client:
            logger.debug("[Narrator] LLM client não configurado, usando fallback")
            return self._fallback_narrative(symbol, hotspots)

        # Filtra apenas Tier 1 e Tier 2 para o LLM
        high_quality = []
        for h in hotspots:
            sig = h.get("dominant_signature", "unknown")
            session = h.get("session", "NEW_YORK")
            quality = self.engine.get_signal_quality(sig, session)
            if quality["tier"] <= 2:
                high_quality.append({**h, "_quality": quality})

        if not high_quality:
            return self._fallback_narrative(symbol, hotspots)

        prompt = self._build_narrative_prompt(symbol, high_quality)

        try:
            import asyncio
            response = await asyncio.wait_for(
                self.llm_client.reason(prompt, ""),
                timeout=self.cfg.llm_timeout_seconds,
            )
            logger.info(f"[Narrator] Narrativa LLM gerada ({len(response)} chars)")
            return response
        except Exception as e:
            logger.warning(f"[Narrator] LLM falhou ({e}), usando fallback")
            return self._fallback_narrative(symbol, hotspots)

    def _build_narrative_prompt(self, symbol: str, hotspots: List[Dict]) -> str:
        # Remove campos não-serializáveis para o prompt
        clean = []
        for h in hotspots:
            clean.append({k: v for k, v in h.items() if k != "_quality"})

        return (
            f"Você é um analista institucional de Order Flow para {symbol}.\n\n"
            f"HOTSPOTS DETECTADOS:\n{json.dumps(clean[:10], indent=2, default=str)}\n\n"
            f"GERE UMA NARRATIVA:\n"
            f"1. Resumo executivo (2 frases)\n"
            f"2. Níveis críticos de defesa/acumulação\n"
            f"3. Setup de maior probabilidade para próxima sessão\n"
            f"4. Riscos a monitorar\n\n"
            f"FORMATO: Markdown, tom analítico, máximo 300 palavras."
        )

    def _fallback_narrative(self, symbol: str, hotspots: List[Dict]) -> str:
        """Narrativa template quando LLM não está disponível."""
        return self._generate_report(symbol, hotspots, [], {}, [])
