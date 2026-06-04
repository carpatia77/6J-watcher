from __future__ import annotations
"""
narrator.py
-----------
Transforma os dados da LiquidityMatrix + DuckDB em relatórios
legíveis para o trader.

Outputs:
  - daily_report()   → Markdown com hotspots, distribuição e sessão
  - alert()          → Alerta de padrão recorrente em tempo real
  - level_summary()  → Resumo de um nível específico de preço
"""
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class Narrator:
    def daily_report(
        self,
        symbol: str,
        hotspots: List[Dict],
        signature_distribution: List,
        session_analysis: Dict,
        notable_events: Optional[List[Dict]] = None,
    ) -> str:
        notable_events = notable_events or []
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
            "## Institutional Hotspots",
        ]
        if hotspots:
            lines.append("| Price | Occurrences | Dominant Signature | Avg Confidence |")
            lines.append("|-------|-------------|-------------------|----------------|")
            for h in hotspots[:15]:
                lines.append(
                    f"| {h['price']:.5f} | {h['occurrences']} | "
                    f"{h.get('dominant_signature','?')} | {h.get('avg_confidence',0):.2f} |"
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

    def alert(self, price: float, signature: str, occurrences: int, volume: int, confidence: float) -> str:
        return (
            f"🔔 PATTERN DETECTED\n"
            f"   Price:       {price:.5f}\n"
            f"   Signature:   {signature}\n"
            f"   Occurrences: {occurrences}\n"
            f"   Volume:      {volume}\n"
            f"   Confidence:  {confidence:.2f}"
        )

    def level_summary(self, price_matrix: Dict) -> str:
        p = price_matrix
        price = p.get('price')
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
