import json
from collections import Counter
from typing import List, Dict, Optional
from models import BehaviorSignature, LiquidityCluster
from config import Config

class AdaptivePatternEngine:
    """
    Classificador Não-Paramétrico baseado em Percentis Empíricos e Deslocamento de Preço.
    """
    
    TIER_1 = ["breakout_genuine", "defense_line", "absorption_passive"] # Alta Confiança Direcional/Reversão
    TIER_2 = ["iceberg_accumulation", "iceberg_distribution", "magnet_effect"] # Contexto/Acumulação
    TIER_3 = ["spoofing_wall", "liquidity_vacuum"] # Filtros/Ruído (Geralmente descartados no live trading)

    def __init__(self, profile_path: str = "profile.json", tick_size: Optional[float] = None, cfg: Optional[Config] = None):
        self.cfg = cfg or Config()
        self.profile = self._load_profile(profile_path)
        self.tick_size = tick_size or self.cfg.tick_size

    def _load_profile(self, path: str) -> dict:
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return self._fallback_profile()

    def _fallback_profile(self) -> dict:
        # Fallback genérico caso o profiler não tenha rodado
        return {
            "thresholds": {
                "ASIAN":     {"vol_percentiles": {"90": 20, "75": 10}, "imb_percentiles": {"90": 10, "75": 5}},
                "LONDON":    {"vol_percentiles": {"90": 35, "75": 20}, "imb_percentiles": {"90": 20, "75": 10}},
                "NEW_YORK":  {"vol_percentiles": {"90": 50, "75": 30}, "imb_percentiles": {"90": 30, "75": 15}},
                "OFF_HOURS": {"vol_percentiles": {"90": 15, "75": 5},  "imb_percentiles": {"90": 5,  "75": 2}}
            }
        }

    def _get_session(self, hour: int) -> str:
        if 0 <= hour < 8: return "ASIAN"
        if 8 <= hour < 13: return "LONDON"
        if 13 <= hour < 22: return "NEW_YORK"
        return "OFF_HOURS"

    def _get_percentile_rank(self, value: float, percentile_dict: Dict[str, float]) -> int:
        """Retorna o percentil (ex: 90, 95) que o valor ultrapassa."""
        for p in sorted([int(k) for k in percentile_dict.keys()], reverse=True):
            if value >= percentile_dict[str(p)]:
                return p
        return 0

    def classify(self, cluster: LiquidityCluster) -> BehaviorSignature:
        """
        Classificação baseada em Microestrutura Real.
        Usa cluster.delta_price_ticks (calculado pelo IngestionService via Stateful Cursor O(1)).
        """
        session = self._get_session(cluster.timestamp.hour)
        stats = self.profile.get("thresholds", {}).get(session, {})
        
        if not stats:
            return BehaviorSignature.UNKNOWN

        vol_p = self._get_percentile_rank(cluster.total_bid + cluster.total_ask, stats.get("vol_percentiles", {}))
        imb_p = self._get_percentile_rank(abs(cluster.total_bid - cluster.total_ask), stats.get("imb_percentiles", {}))
        delta = cluster.delta_price_ticks
        
        # Agressão de compra = agressores batem no ASK (BID volume domina)
        is_buy_pressure = cluster.total_bid > cluster.total_ask
        
        # --- SEMÂNTICA DE MICROESTRUTURA ---

        # 1. ABSORPTION PASSIVE (Tier 1)
        # Agressão extrema (Imbalance > p90), Volume Alto (> p90), mas o preço NÃO ANDA (delta <= 1 tick).
        # Indica Iceberg Passivo absorvendo toda a agressão no nível.
        if vol_p >= 90 and imb_p >= 90 and abs(delta) <= 1:
            return BehaviorSignature.ABSORPTION_PASSIVE

        # 2. BREAKOUT GENUINE (Tier 1)
        # Volume e Imbalance fortes (> p75), e o preço DESLOCOU (delta > 2 ticks).
        # A agressão consumiu a liquidez e o mercado aceitou o novo preço.
        if vol_p >= 75 and imb_p >= 75 and abs(delta) >= 2:
            return BehaviorSignature.BREAKOUT_GENUINE

        # 3. ICEBERG ACCUMULATION / DISTRIBUTION (Tier 2)
        # Volume alto executando no mesmo nível (delta == 0), sem imbalance extremo.
        # Semântica CORRETA: se os compradores estão agredindo mas o preço não sobe,
        # há uma parede PASSIVA DE VENDA no ASK -> ICEBERG_DISTRIBUTION (vendedor).
        # Se os vendedores estão agredindo mas o preço não cai,
        # há uma parede PASSIVA DE COMPRA no BID -> ICEBERG_ACCUMULATION (comprador).
        if vol_p >= 75 and abs(delta) == 0 and imb_p < 90:
            if is_buy_pressure:
                return BehaviorSignature.ICEBERG_DISTRIBUTION  # Muralha de venda absorvendo compras
            else:
                return BehaviorSignature.ICEBERG_ACCUMULATION  # Muralha de compra absorvendo vendas

        # 4. SPOOFING WALL (Tier 3)
        # Volume alto no nível (> p75), mas imbalance MUITO BAIXO (< p50) e preço parado.
        # Indica liquidez que aparece e desaparece — parede fictícia para enganar algoritmos.
        if vol_p >= 75 and imb_p < 50 and abs(delta) == 0:
            return BehaviorSignature.SPOOFING_WALL

        # 5. LIQUIDITY VACUUM (Tier 3)
        # Volume muito baixo (< p50) mas o preço deslocou significativamente (>= 2 ticks).
        # O mercado se moveu com resistência mínima — book vazio, gap de liquidez.
        if vol_p < 50 and abs(delta) >= 2:
            return BehaviorSignature.LIQUIDITY_VACUUM

        return BehaviorSignature.UNKNOWN

    def post_classify(self, price: float, clusters: List[LiquidityCluster]) -> BehaviorSignature:
        """
        Elevação de Tier baseada em Confluência Histórica (Recorrência).
        Regra de precedência: DEFENSE_LINE > assinatura dominante > UNKNOWN.
        Nota: MAGNET_EFFECT requer rastreamento de convergência de preço ao longo do tempo
        e não pode ser inferido apenas por contagem de toques. Tratado como metadata futura.
        """
        if not clusters:
            return BehaviorSignature.UNKNOWN

        # DEFENSE LINE (Tier 1): 3+ eventos defensivos no exato mesmo nível.
        # Indica que uma instituição está defendendo ativamente este preço.
        defensive_sigs = {
            BehaviorSignature.ABSORPTION_PASSIVE,
            BehaviorSignature.ICEBERG_ACCUMULATION,
            BehaviorSignature.ICEBERG_DISTRIBUTION
        }
        
        defensive_count = sum(1 for c in clusters if c.behavior_signature in defensive_sigs)
        
        if defensive_count >= 3:
            return BehaviorSignature.DEFENSE_LINE

        # Retorna a assinatura dominante preservando informação valiosa.
        # Ex: 3x ICEBERG_ACCUMULATION retorna ICEBERG_ACCUMULATION, não MAGNET_EFFECT.
        sigs = [c.behavior_signature for c in clusters if c.behavior_signature != BehaviorSignature.UNKNOWN]
        if not sigs:
            return BehaviorSignature.UNKNOWN
            
        return Counter(sigs).most_common(1)[0][0]

    def get_signal_quality(self, signature: BehaviorSignature, session: str) -> dict:
        """Retorna a expectativa matemática do sinal baseada no backtest."""
        key = f"{signature.value}_{session}"
        stats = self.profile.get("signatures", {}).get(key, {})
        
        return {
            "tier": 1 if signature.value in self.TIER_1 else (2 if signature.value in self.TIER_2 else 3),
            "historical_win_rate": stats.get("win_rate", 0.0),
            "profit_factor": stats.get("profit_factor", 0.0),
            "sample_size": stats.get("count", 0)
        }
