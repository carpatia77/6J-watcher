from __future__ import annotations
"""
pattern_engine.py
-----------------
Classifica clusters em BehaviorSignature por heurística.

Regras implementadas:
  ICEBERG_ACCUMULATION  — compra recorrente sem deslocar preço
  ICEBERG_DISTRIBUTION  — venda recorrente sem deslocar preço
  ABSORPTION_PASSIVE    — agressão absorvida; imbalance baixo, confiança alta
  SPOOFING_WALL         — volume passivo > 5x agressão, sem execução
  BREAKOUT_GENUINE      — imbalance alto, volume alto
  DEFENSE_LINE          — detectado via recorrência na LiquidityMatrix
  MAGNET_EFFECT         — detectado via convergência na LiquidityMatrix
  LIQUIDITY_VACUUM      — dom vazio em nível de preço

Atenção: as assinaturas de DEFENSE_LINE e MAGNET_EFFECT dependem de
contexto histórico e são atribuídas no PatternEngine.post_classify(),
que recebe a lista de clusters de um nível.
"""
from collections import Counter
from typing import List
from models import BehaviorSignature, LiquidityCluster


class PatternEngine:
    def classify(self, cluster: LiquidityCluster) -> BehaviorSignature:
        bid = cluster.total_bid
        ask = cluster.total_ask
        total = bid + ask
        if total == 0:
            return BehaviorSignature.UNKNOWN

        imbalance_ratio = abs(ask - bid) / total

        # Iceberg Accumulation: compras dominando, preço não subiu (deduzido por low confidence)
        if bid >= 3 * max(ask, 1) and cluster.confidence <= 0.75:
            return BehaviorSignature.ICEBERG_ACCUMULATION

        # Iceberg Distribution: vendas dominando
        if ask >= 3 * max(bid, 1) and cluster.confidence <= 0.75:
            return BehaviorSignature.ICEBERG_DISTRIBUTION

        # Absorption Passive: agressão existe mas imbalance é baixo
        if imbalance_ratio < 0.15 and total >= 10:
            return BehaviorSignature.ABSORPTION_PASSIVE

        # Breakout Genuine: imbalance alto e volume relevante
        if imbalance_ratio >= 0.5 and total >= 20:
            return BehaviorSignature.BREAKOUT_GENUINE

        return BehaviorSignature.UNKNOWN

    def post_classify(self, price: float, clusters: List[LiquidityCluster]) -> BehaviorSignature:
        """Reclassifica com base em recorrência do nível."""
        if len(clusters) < 3:
            return BehaviorSignature.UNKNOWN
        sigs = Counter(c.behavior_signature for c in clusters)
        dominant = sigs.most_common(1)[0][0]
        # 3+ eventos de absorção ou iceberg no mesmo nível → defense line
        if dominant in (BehaviorSignature.ABSORPTION_PASSIVE,
                        BehaviorSignature.ICEBERG_ACCUMULATION,
                        BehaviorSignature.ICEBERG_DISTRIBUTION):
            if len(clusters) >= 3:
                return BehaviorSignature.DEFENSE_LINE
        return dominant

    def dominant(self, clusters: List[LiquidityCluster]) -> str:
        if not clusters:
            return "unknown"
        return Counter(c.behavior_signature.value for c in clusters).most_common(1)[0][0]
