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
    TIER_2 = ["iceberg_accumulation", "iceberg_distribution"] # Contexto/Acumulação
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
                "ASIAN":     {"vol_percentiles": {"90": 10, "75": 5}, "imb_percentiles": {"90": 5, "75": 2}},
                "LONDON":    {"vol_percentiles": {"90": 11, "75": 5}, "imb_percentiles": {"90": 5, "75": 2}},
                "NEW_YORK":  {"vol_percentiles": {"90": 11, "75": 5}, "imb_percentiles": {"90": 5, "75": 2}},
                "OFF_HOURS": {"vol_percentiles": {"90": 10, "75": 5}, "imb_percentiles": {"90": 5,  "75": 2}}
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

    def classify(self, cluster: LiquidityCluster) -> tuple[BehaviorSignature, float]:
        """
        Classificação baseada em Microestrutura Real.
        Retorna (Assinatura, Confiança).
        """
        session = self._get_session(cluster.timestamp.hour)
        stats = self.profile.get("thresholds", {}).get(session, {})
        
        if not stats:
            return BehaviorSignature.UNKNOWN, 0.0

        vol_p = self._get_percentile_rank(cluster.total_bid + cluster.total_ask, stats.get("vol_percentiles", {}))
        imb_p = self._get_percentile_rank(abs(cluster.total_bid - cluster.total_ask), stats.get("imb_percentiles", {}))
        delta = cluster.delta_price_ticks
        
        is_buy_pressure = cluster.total_bid > cluster.total_ask
        is_stationary = abs(delta) <= 1
        is_trending = abs(delta) >= 2
        
        # 1. ABSORPTION PASSIVE (Tier 1)
        if vol_p >= 90 and imb_p >= 90 and is_stationary:
            conf = (vol_p / 100.0 * 0.4) + (imb_p / 100.0 * 0.4) + 0.2
            return BehaviorSignature.ABSORPTION_PASSIVE, conf

        # 2. BREAKOUT GENUINE (Tier 1)
        if vol_p >= 75 and imb_p >= 75 and is_trending:
            conf = (vol_p / 100.0 * 0.4) + (imb_p / 100.0 * 0.4) + 0.2
            return BehaviorSignature.BREAKOUT_GENUINE, conf

        # 3. ICEBERG ACCUMULATION / DISTRIBUTION (Tier 2)
        if vol_p >= 75 and is_stationary and imb_p < 90:
            conf = (vol_p / 100.0 * 0.5) + ((100 - imb_p) / 100.0 * 0.3) + 0.2
            if is_buy_pressure:
                return BehaviorSignature.ICEBERG_DISTRIBUTION, conf
            else:
                return BehaviorSignature.ICEBERG_ACCUMULATION, conf

        # 4. SPOOFING WALL (Tier 3)
        # O que restou de estacionário com volume, mas desequilíbrio muito pequeno (book fake retirado)
        if vol_p >= 75 and imb_p < 50 and is_stationary:
            conf = (vol_p / 100.0 * 0.5) + ((100 - imb_p) / 100.0 * 0.3) + 0.2
            return BehaviorSignature.SPOOFING_WALL, conf

        # 5. LIQUIDITY VACUUM (Tier 3)
        if vol_p < 50 and is_trending:
            conf = min(1.0, abs(delta) / 5.0 * 0.7 + (1 - vol_p / 100.0) * 0.3)
            return BehaviorSignature.LIQUIDITY_VACUUM, conf

        return BehaviorSignature.UNKNOWN, 0.0

    def post_classify(self, price: float, clusters: List[LiquidityCluster]) -> BehaviorSignature:
        """
        Elevação de Tier baseada em Confluência Histórica (Recorrência).
        Regra de precedência: DEFENSE_LINE > assinatura dominante > UNKNOWN.
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
        # Ex: 3x ICEBERG_ACCUMULATION retorna ICEBERG_ACCUMULATION.
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
