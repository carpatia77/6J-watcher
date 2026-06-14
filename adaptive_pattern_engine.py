import json
from collections import Counter
from typing import List, Dict, Optional, Union
from models import BehaviorSignature, LiquidityCluster
from config import Config


class AdaptivePatternEngine:
    """
    Classificador Não-Paramétrico baseado em Percentis Empíricos e Deslocamento de Preço.
    Calibrado para micro-janelas de 250ms (ingestion.py _WINDOW_NS).
    """

    TIER_1 = ["breakout_genuine", "defense_line", "absorption_passive"]
    TIER_2 = ["iceberg_accumulation", "iceberg_distribution"]
    TIER_3 = ["spoofing_wall", "spoofing_bid_pull", "spoofing_ask_pull", "liquidity_vacuum"]

    def __init__(self, profile_path: str = "profile.json",
                 tick_size: Optional[float] = None,
                 cfg: Optional[Config] = None):
        self.cfg = cfg or Config()
        self.profile = self._load_profile(profile_path)
        self.tick_size = tick_size or self.cfg.tick_size

    def _load_profile(self, path: str) -> dict:
        try:
            with open(path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return self._fallback_profile()

    def _fallback_profile(self) -> dict:
        """
        Fallback calibrado para janelas de 250ms do 6J CME MBP-10.
        Sincronizado com SignatureProfiler._get_fallback_thresholds().

        Valores anteriores (10-11 lotes vol_p90) eram de tick-único e causavam
        colapso de classificação para ABSORPTION_PASSIVE em quase toda janela
        antes do profile.json ser gerado pelo backtest.
        """
        return {
            "signatures": {},
            "thresholds": {
                "ASIAN": {
                    "vol_percentiles": {"50": 20,  "75": 40,  "90": 80,  "95": 120, "99": 200},
                    "imb_percentiles": {"50": 8,   "75": 15,  "90": 30,  "95": 50,  "99": 80},
                },
                "LONDON": {
                    "vol_percentiles": {"50": 50,  "75": 100, "90": 200, "95": 300, "99": 500},
                    "imb_percentiles": {"50": 20,  "75": 40,  "90": 80,  "95": 120, "99": 200},
                },
                "NEW_YORK": {
                    "vol_percentiles": {"50": 70,  "75": 150, "90": 300, "95": 450, "99": 700},
                    "imb_percentiles": {"50": 30,  "75": 60,  "90": 120, "95": 180, "99": 300},
                },
                "OFF_HOURS": {
                    "vol_percentiles": {"50": 15,  "75": 30,  "90": 60,  "95": 90,  "99": 150},
                    "imb_percentiles": {"50": 5,   "75": 10,  "90": 20,  "95": 35,  "99": 60},
                },
            },
        }

    def _get_session(self, hour: int) -> str:
        if 0 <= hour < 8:   return "ASIAN"
        if 8 <= hour < 13:  return "LONDON"
        if 13 <= hour < 22: return "NEW_YORK"
        return "OFF_HOURS"

    def _get_percentile_rank(self, value: float, percentile_dict: Dict[str, float]) -> int:
        """Retorna o percentil (ex: 90, 95) que o valor ultrapassa."""
        for p in sorted([int(k) for k in percentile_dict.keys()], reverse=True):
            if value >= percentile_dict[str(p)]:
                return p
        return 0

    def classify(self, cluster: LiquidityCluster) -> tuple:
        """
        Classificação baseada em Microestrutura Real (micro-janelas 250ms).
        Retorna (BehaviorSignature, confianca: float).

        Regras (ordem de prioridade):
          1. ABSORPTION_PASSIVE  — Vol>=90%, Imb>=90%, |delta|<=1
          2. BREAKOUT_GENUINE    — Vol>=75%, Imb>=75%, |delta|>=2
          3. ICEBERG_*           — Vol>=75%, 50<=Imb<90, |delta|<=1
          4. SPOOFING_WALL       — Vol>=75%, Imb<50,   |delta|<=1
          5. LIQUIDITY_VACUUM    — Vol<50%,  |delta|>=2
        """
        session = self._get_session(cluster.timestamp.hour)
        stats   = self.profile.get("thresholds", {}).get(session, {})
        if not stats:
            return BehaviorSignature.UNKNOWN, 0.0

        vol_total = cluster.total_bid + cluster.total_ask
        imbalance = abs(cluster.total_bid - cluster.total_ask)
        vol_p = self._get_percentile_rank(vol_total, stats.get("vol_percentiles", {}))
        imb_p = self._get_percentile_rank(imbalance, stats.get("imb_percentiles", {}))
        delta = cluster.delta_price_ticks

        is_buy_pressure = cluster.total_bid > cluster.total_ask
        is_stationary   = abs(delta) <= 1
        is_trending     = abs(delta) >= 2

        sig  = BehaviorSignature.UNKNOWN
        conf = 0.0

        # 1. ABSORPTION PASSIVE (Tier 1)
        if vol_p >= 90 and imb_p >= 90 and is_stationary:
            conf = (vol_p / 100.0 * 0.4) + (imb_p / 100.0 * 0.4) + 0.2
            sig  = BehaviorSignature.ABSORPTION_PASSIVE

        # 2. BREAKOUT GENUINE (Tier 1)
        elif vol_p >= 75 and imb_p >= 75 and is_trending:
            conf = (vol_p / 100.0 * 0.4) + (imb_p / 100.0 * 0.4) + 0.2
            sig  = BehaviorSignature.BREAKOUT_GENUINE

        # 3. ICEBERG ACCUMULATION / DISTRIBUTION (Tier 2)
        elif vol_p >= 75 and is_stationary and 50 <= imb_p < 90:
            conf = (vol_p / 100.0 * 0.5) + ((100 - imb_p) / 100.0 * 0.3) + 0.2
            sig  = (BehaviorSignature.ICEBERG_DISTRIBUTION
                    if is_buy_pressure else BehaviorSignature.ICEBERG_ACCUMULATION)

        # 4. SPOOFING WALL (Tier 3)
        elif vol_p >= 75 and imb_p < 50 and is_stationary:
            dom_bid = cluster.raw_payload.get("dom_bid", 0)
            dom_ask = cluster.raw_payload.get("dom_ask", 0)
            dom_contradiction = (
                (is_buy_pressure  and dom_ask > dom_bid * 2) or
                (not is_buy_pressure and dom_bid > dom_ask * 2)
            )
            dom_bonus = 0.1 if (dom_bid > 0 or dom_ask > 0) and dom_contradiction else 0.0
            conf = (vol_p / 100.0 * 0.5) + ((100 - imb_p) / 100.0 * 0.3) + 0.1 + dom_bonus
            sig  = BehaviorSignature.SPOOFING_WALL

        # 4.5 SPOOFING PULLS (Fase 2 - Cancelamentos persistentes)
        cancel_bid = cluster.raw_payload.get("cancel_bid_vol", 0)
        cancel_ask = cluster.raw_payload.get("cancel_ask_vol", 0)
        trade_vol  = max(1, vol_total)

        if cancel_bid >= 38 and (cancel_bid / trade_vol) > 5.0:
            sig = BehaviorSignature.SPOOFING_BID_PULL
            conf = min(1.0, 0.6 + (cancel_bid / trade_vol) / 20.0)
        elif cancel_ask >= 38 and (cancel_ask / trade_vol) > 5.0:
            sig = BehaviorSignature.SPOOFING_ASK_PULL
            conf = min(1.0, 0.6 + (cancel_ask / trade_vol) / 20.0)

        # 5. LIQUIDITY VACUUM (Tier 3)
        elif vol_p < 50 and is_trending:
            conf = abs(delta) / 5.0 * 0.7 + (1 - vol_p / 100.0) * 0.3
            sig  = BehaviorSignature.LIQUIDITY_VACUUM

        if sig == BehaviorSignature.UNKNOWN:
            return BehaviorSignature.UNKNOWN, 0.0

        multiplier = self._depth_multiplier(sig.value, cluster.depth_band)
        conf = min(1.0, conf * multiplier)

        return sig, conf

    def _depth_multiplier(self, sig: str, band: str) -> float:
        """
        Lê o multiplicador de profundidade do profile.json.
        Retorna 1.0 (neutro) se a assinatura não possuir perfil para a banda.
        """
        mults = self.profile.get("depth_multipliers", {}).get(sig, {})
        return mults.get(band, 1.0)



    def post_classify(self, price: float, clusters: List[LiquidityCluster]) -> BehaviorSignature:
        """
        Elevação de Tier baseada em Confluência Histórica (Recorrência).
        DEFENSE_LINE quando 3+ eventos defensivos no mesmo nível.
        """
        if not clusters:
            return BehaviorSignature.UNKNOWN

        defensive_sigs = {
            BehaviorSignature.ABSORPTION_PASSIVE,
            BehaviorSignature.ICEBERG_ACCUMULATION,
            BehaviorSignature.ICEBERG_DISTRIBUTION,
        }
        defensive_count = sum(
            1 for c in clusters if c.behavior_signature in defensive_sigs
        )
        if defensive_count >= 3:
            return BehaviorSignature.DEFENSE_LINE

        sigs = [
            c.behavior_signature
            for c in clusters
            if c.behavior_signature != BehaviorSignature.UNKNOWN
        ]
        if not sigs:
            return BehaviorSignature.UNKNOWN
        return Counter(sigs).most_common(1)[0][0]

    def get_signal_quality(
        self,
        signature: Union["BehaviorSignature", str],
        session: str,
        regime: str = "RANGING"
    ) -> dict:
        """
        Retorna a expectativa matemática do sinal baseada no backtest.
        Aceita BehaviorSignature ou str (ex: chamadas vindas de hotspots()).
        """
        sig_key = signature if isinstance(signature, str) else signature.value
        key     = f"{sig_key}_{regime}_{session}"
        stats   = self.profile.get("signatures", {}).get(key, {})

        if not stats:
            # Fallback para profile legado sem regime
            key = f"{sig_key}_{session}"
            stats = self.profile.get("signatures", {}).get(key, {})

        tier = (
            1 if sig_key in self.TIER_1 else
            2 if sig_key in self.TIER_2 else
            3
        )
        return {
            "tier":                tier,
            "historical_win_rate": stats.get("win_rate",      0.0),
            "profit_factor":       stats.get("profit_factor", 0.0),
            "sample_size":         stats.get("count",         0),
        }
