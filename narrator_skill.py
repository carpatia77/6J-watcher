"""
narrator_skill.py
-----------------
SkillContextBuilder — monta o system prompt dinâmico para o LLM do Narrator.

Injeta no context window:
  1. Playbook empírico (Win Rates, PF, MFE por assinatura x sessão)
  2. Estado do dia (WDS, Trend/Noise, DOW pattern)
  3. Confluências ativas e hotspots Tier 1/2
  4. Regras operacionais derivadas das 5 hipóteses

Chain-of-Thought estruturado em 4 passos:
  STEP 1 → Identificar regime (1/2/3)
  STEP 2 → Avaliar setups acionáveis com EV = WR × MFE - (1-WR) × MAE
  STEP 3 → Identificar sinais a ignorar (H3/H4)
  STEP 4 → Definir o que monitorar nos próximos 30min
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Tier classification ────────────────────────────────────────────────────
TIER1 = {"absorption_passive", "breakout_genuine", "defense_line"}
TIER2 = {"iceberg_accumulation", "iceberg_distribution", "spoofing_wall"}
NOISE = {"liquidity_vacuum", "unknown"}

# ── Regime heuristics ──────────────────────────────────────────────────────
REGIME3_SESSIONS  = {"ASIAN"}          # 6J lidera spot
REGIME2_SESSIONS  = {"NEW_YORK"}       # spot lidera 6J (macro events)
REGIME3_MIN_HOUR  = 0                  # UTC 00h-03h = máximo Regime 3
REGIME3_MAX_HOUR  = 3

# ── EV mínimo para considerar setup acionável ──────────────────────────────
MIN_EV_TICKS = 1.0   # EV esperado mínimo em ticks para recomendar entrada


class SkillContextBuilder:
    """
    Constrói o system prompt completo para o LLM do Narrator.
    Lê profile_calibrated.json e variance_report.json em disco,
    combina com estado live (hotspots, confluências, sessão atual).
    """

    def __init__(
        self,
        profile_path: str,
        variance_path: str,
        cfg=None,
    ):
        self.profile_path  = Path(profile_path)
        self.variance_path = Path(variance_path)
        self.cfg = cfg

        self._profile  = self._load_json(self.profile_path,  "profile")
        self._variance = self._load_json(self.variance_path, "variance")

    # ── Public API ─────────────────────────────────────────────────────────

    def build(
        self,
        symbol: str,
        hotspots: List[Dict],
        session: str,
        confluences: List[Dict],
        hour_utc: Optional[int] = None,
    ) -> str:
        """
        Retorna o system prompt completo para injeção no LLM.
        """
        if hour_utc is None:
            hour_utc = datetime.now(timezone.utc).hour

        regime       = self._infer_regime(session, hour_utc, hotspots)
        day_context  = self._build_day_context()
        rules        = self._build_rules_block(session)
        setups       = self._build_setups_block(hotspots, session)
        conf_block   = self._build_confluences_block(confluences)
        hotspot_block= self._build_hotspots_block(hotspots)
        meta         = self._build_metadata_block(symbol)

        return _SKILL_TEMPLATE.format(
            symbol        = symbol,
            meta          = meta,
            regime        = regime,
            day_context   = day_context,
            rules         = rules,
            setups        = setups,
            conf_block    = conf_block,
            hotspot_block = hotspot_block,
            session       = session,
            hour_utc      = hour_utc,
        )

    # ── Private builders ───────────────────────────────────────────────────

    def _infer_regime(self, session: str, hour_utc: int, hotspots: List[Dict]) -> str:
        """
        Infere o regime atual baseado em sessão, hora UTC e composição de sinais.
        Regime 3 (6J lidera): ASIAN 00h-03h UTC com Tier 1 presente
        Regime 2 (spot lidera): NY com breakout (sinal tardio)
        Regime 1 (sincronia): demais casos
        """
        tier1_present = any(
            h.get("dominant_signature", "") in TIER1 for h in hotspots
        )
        breakout_in_ny = (
            session == "NEW_YORK" and
            any(h.get("dominant_signature") == "breakout_genuine" for h in hotspots)
        )

        if session in REGIME3_SESSIONS and REGIME3_MIN_HOUR <= hour_utc < REGIME3_MAX_HOUR and tier1_present:
            return (
                "**REGIME_3 — 6J LIDERA O SPOT** ✅\n"
                "   Janela de máximo edge: 00h-03h UTC ASIAN com Tier 1 confirmado.\n"
                "   Sinal do 6J precede o USDJPY spot em 30s-5min.\n"
                "   → Sinais Tier 1 têm validade máxima agora."
            )
        elif session in REGIME3_SESSIONS and tier1_present:
            return (
                "**REGIME_3 PARCIAL — ASIAN com absorção ativa** ⚠️\n"
                "   Fora da janela 00h-03h mas Tier 1 presente na sessão ASIAN.\n"
                "   Edge reduzido mas válido — confirmar volume acima vol_p75."
            )
        elif breakout_in_ny:
            return (
                "**REGIME_2 — SPOT LIDERA O 6J** ⚠️\n"
                "   Breakout detectado durante NY: sinal provavelmente tardio (H3).\n"
                "   O USDJPY spot já se moveu — entrada no 6J pode ser no topo/fundo.\n"
                "   → NÃO recomendar breakout como setup primário agora."
            )
        else:
            return (
                "**REGIME_1 — SINCRONIA** ℹ️\n"
                "   6J e spot em movimento sincronizado.\n"
                "   Sinais válidos mas sem vantagem de liderança."
            )

    def _build_day_context(self) -> str:
        """Extrai contexto do dia atual do variance_report.json."""
        if not self._variance:
            return "   Variance report não disponível — WDS desconhecido."

        summary = self._variance.get("patterns", {}).get("summary", {})
        today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Tenta encontrar o dia atual no daily_scores
        daily = self._variance.get("daily_scores", [])
        today_data = next((d for d in daily if d.get("day") == today), None)

        if today_data:
            wds   = today_data.get("wds", "N/A")
            cls   = today_data.get("classification", "N/A")
            ci    = today_data.get("choppiness_index", "N/A")
            t1r   = today_data.get("tier1_ratio", 0)
            return (
                f"   WDS hoje:           {wds:.4f}\n"
                f"   Classificação:      {cls}\n"
                f"   Choppiness Index:   {ci:.4f} (< 0.5 = trending, > 0.7 = choppy)\n"
                f"   Tier 1 ratio hoje:  {t1r*100:.1f}%"
            )

        # Fallback: padrão histórico do DOW
        dow_name = datetime.now(timezone.utc).strftime("%a")  # Mon, Tue...
        dow_data = self._variance.get("patterns", {}).get("by_day_of_week", {}).get(dow_name, {})
        if dow_data:
            return (
                f"   WDS hoje: intraday em construção (dia atual fora do backtest)\n"
                f"   Padrão histórico para {dow_name}: "
                f"WDS médio={dow_data.get('avg_wds','?'):.3f} | "
                f"Trend={dow_data.get('trend_pct','?'):.0f}%"
            )

        return "   WDS hoje: dados ainda não disponíveis para esta data."

    def _build_rules_block(self, session: str) -> str:
        """
        Extrai regras operacionais do profile_calibrated.json
        para a sessão atual. Formata como playbook legível pelo LLM.
        """
        if not self._profile or "signatures" not in self._profile:
            return "   Profile não calibrado — usando thresholds genéricos."

        sigs   = self._profile.get("signatures", {})
        thresh = self._profile.get("thresholds", {}).get(session, {})

        lines = []
        for key, stats in sigs.items():
            if f"_{session}" not in key:
                continue
            sig_name = key.replace(f"_{session}", "")
            wr   = stats.get("win_rate", 0)
            pf   = stats.get("profit_factor", 0)
            mfe  = stats.get("avg_mfe", 0)
            cnt  = stats.get("count", 0)
            tier = "T1" if sig_name in TIER1 else ("T2" if sig_name in TIER2 else "T3")

            # Só inclui se tiver amostragem mínima
            if cnt < 30:
                continue

            flag = "✅" if pf >= 1.5 else ("⚠️" if pf >= 1.0 else "❌")
            lines.append(
                f"   {flag} [{tier}] {sig_name}: "
                f"WR={wr:.0%} | PF={pf:.2f} | MFE={mfe/0.00005:.1f}tks | n={cnt}"
            )

        # Thresholds de volume da sessão
        vol_p90 = thresh.get("vol_percentiles", {}).get("90", "?")
        imb_p90 = thresh.get("imb_percentiles", {}).get("90", "?")
        if vol_p90 != "?":
            lines.append(f"\n   📏 vol_p90={vol_p90:.0f} lotes | imb_p90={imb_p90:.0f} lotes "
                         f"(sinal significativo apenas acima desses valores)")

        return "\n".join(lines) if lines else "   Sem dados calibrados para esta sessão ainda."

    def _build_setups_block(self, hotspots: List[Dict], session: str) -> str:
        """
        Calcula EV esperado para cada hotspot Tier 1/2 e ranqueia setups.
        EV = WR × avg_MFE - (1-WR) × avg_MAE
        """
        if not self._profile or not hotspots:
            return "   Sem setups calculáveis no momento."

        sigs = self._profile.get("signatures", {})
        setups = []

        for h in hotspots:
            sig     = h.get("dominant_signature", "unknown")
            price   = h.get("price", 0)
            occ     = h.get("occurrences", 0)
            if sig in NOISE:
                continue

            key     = f"{sig}_{session}"
            stats   = sigs.get(key, {})
            wr      = stats.get("win_rate", 0.5)
            mfe_raw = stats.get("avg_mfe", 0)
            # MAE aproximado: MFE × (1 - PF) / PF como proxy conservador
            pf      = stats.get("profit_factor", 1.0)
            mae_raw = mfe_raw / pf if pf > 0 else mfe_raw

            mfe_tks = mfe_raw / 0.00005
            mae_tks = mae_raw / 0.00005
            ev      = wr * mfe_tks - (1 - wr) * mae_tks

            if ev >= MIN_EV_TICKS:
                tier = "T1" if sig in TIER1 else "T2"
                setups.append({
                    "price": price, "sig": sig, "tier": tier,
                    "wr": wr, "pf": pf,
                    "mfe_tks": mfe_tks, "mae_tks": mae_tks,
                    "ev": ev, "occ": occ,
                })

        if not setups:
            return "   Nenhum setup com EV positivo no momento."

        setups.sort(key=lambda x: x["ev"], reverse=True)
        lines = []
        for i, s in enumerate(setups[:5], 1):
            lines.append(
                f"   #{i} [{s['tier']}] {s['sig']} @ {s['price']:.5f}\n"
                f"       EV=+{s['ev']:.1f}tks | WR={s['wr']:.0%} | "
                f"stop={s['mae_tks']:.1f}tks | alvo={s['mfe_tks']:.1f}tks | "
                f"occ={s['occ']}"
            )
        return "\n".join(lines)

    def _build_confluences_block(self, confluences: List[Dict]) -> str:
        if not confluences:
            return "   Nenhuma confluência ativa no momento."
        lines = []
        for cf in confluences:
            lines.append(
                f"   ⚡ {cf['type']} @ {cf['price']:.5f}\n"
                f"      → {cf['interpretation']}"
            )
        return "\n".join(lines)

    def _build_hotspots_block(self, hotspots: List[Dict]) -> str:
        tier12 = [h for h in hotspots
                  if h.get("dominant_signature", "") not in NOISE][:8]
        if not tier12:
            return "   Nenhum hotspot Tier 1/2 ativo."
        lines = []
        for h in tier12:
            lines.append(
                f"   {h['price']:.5f} | {h.get('dominant_signature','?'):<28} "
                f"| occ={h.get('occurrences',0)}"
            )
        return "\n".join(lines)

    def _build_metadata_block(self, symbol: str) -> str:
        if not self._profile:
            return f"symbol={symbol} | profile=NÃO CALIBRADO"
        meta     = self._profile.get("metadata", {})
        gen_at   = meta.get("generated_at", "?")[:10]
        sigs     = self._profile.get("signatures", {})
        n_sigs   = len(sigs)
        total_n  = sum(v.get("count", 0) for v in sigs.values())
        variance = self._variance.get("patterns", {}).get("summary", {})
        n_days   = variance.get("total_days", "?")
        trend_p  = variance.get("trend_pct", "?")
        return (
            f"symbol={symbol} | profile calibrado em {gen_at} | "
            f"{total_n:,} clusters | {n_sigs} pares sig×sessão | "
            f"{n_days} dias históricos | {trend_p}% Trend Days"
        )

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _load_json(path: Path, label: str) -> Optional[Dict]:
        if not path.exists():
            logger.warning("[SkillContextBuilder] %s não encontrado: %s", label, path)
            return None
        try:
            return json.loads(path.read_text())
        except Exception as e:
            logger.error("[SkillContextBuilder] Erro lendo %s: %s", label, e)
            return None


# ── Template do System Prompt ──────────────────────────────────────────────

_SKILL_TEMPLATE = """\
Você é o **6J Market Analyst**, um analista quantitativo de microestrutura \\
especializado no contrato 6J (JPY/USD Futuro, CME Globex).

## CALIBRAÇÃO DO SISTEMA
{meta}

---

## REGIME ATUAL
{regime}

---

## ESTADO DO DIA
{day_context}

---

## PLAYBOOK EMPÍRICO — SESSÃO {session} ({hour_utc}h UTC)
(Win Rates e Profit Factors derivados de dados MBP-10 reais do CME)
{rules}

---

## SETUPS RANQUEADOS POR EV ESPERADO
(EV = Win Rate × MFE_médio − (1−Win Rate) × MAE_médio, em ticks)
{setups}

---

## CONFLUÊNCIAS ATIVAS
{conf_block}

---

## HOTSPOTS TIER 1/2 ATIVOS
{hotspot_block}

---

## SUA TAREFA — Chain-of-Thought em 4 passos

**STEP 1 — REGIME:** Confirme o regime atual com 1 dado concreto dos hotspots.

**STEP 2 — SETUP:** Para o setup de maior EV: informe direção (long/short), \\
nível exato, stop em ticks (= MAE histórico da assinatura), \\
alvo em ticks (= MFE histórico). Se nenhum setup tiver EV > 1.0 tick, diga "AGUARDAR".

**STEP 3 — IGNORAR:** Existe sinal a descartar? Aplique H3 \\
(breakout NY = tardio) ou H4 (OFF_HOURS = ruído) se aplicável.

**STEP 4 — MONITORAR:** 1 evento ou nível específico para observar nos próximos 30min.

FORMATO: markdown, máximo 200 palavras, números precisos, sem disclaimers.
"""
