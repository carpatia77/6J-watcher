# AUDIT — adaptive_pattern_engine.py

> Última revisão: **2026-06-08**  
> Revisado por: Perplexity / carpatia77  
> Commits desta sessão: ver histórico `main` — fixes P3, P4, P5

---

## Arquitetura geral

`AdaptivePatternEngine` é o classificador central do pipeline. Recebe um `LiquidityCluster`
aggregado (janela de 250ms) e retorna `(BehaviorSignature, confidence: float)`.

A engine opera em dois modos:
- **Produção:** carrega `profile.json` gerado pelo `SignatureProfiler` no startup
- **Backtest:** usa `_fallback_profile()` quando `profile.json` não existe ou está vazio

---

## Fixes aplicados — sessão 2026-06-08

### FIX-APE-01 — Dead code removido (`_legacy_classify`)
**Severidade:** 🟡 Manutenção / risco de drift  
**Problema:** Método `_legacy_classify` presente no código mas nunca chamado. Mantinha lógica de classificação de tick único que divergia silenciosamente das regras de `classify()`.  
**Fix:** Método removido.  
**Impacto residual:** Nenhum. Nenhum módulo externo referenciava `_legacy_classify`.

---

### FIX-APE-02 — `get_signal_quality` aceita `str` e `BehaviorSignature`
**Severidade:** 🔴 Bug de runtime  
**Problema:** `get_signal_quality(sig)` assumia `sig` como `BehaviorSignature` enum e chamava `.value` diretamente. O `narrator.py` passava `str` (resultado de `.behavior_signature.value` de clusters persistidos), causando `AttributeError: 'str' object has no attribute 'value'` em todo relatório narrativo.  
**Fix:** Guard no início do método:
```python
if isinstance(sig, str):
    key_base = sig
else:
    key_base = sig.value
```
**Impacto residual nos módulos dependentes:**
- `narrator.py` → `get_signal_quality()` — agora funciona para ambos os tipos
- `backtest_runner.py` → `narrator.daily_report()` — relatório final do backtest não crashava silenciosamente
- `main.py` (produção) — idem

---

### FIX-APE-03 — Fallback thresholds recalibrados para janelas de 250ms
**Severidade:** 🔴 Classificação degenerada em backtest  
**Problema:** `_fallback_profile()` usava thresholds de tick único (vol_p90 ≈ 5–11 lotes). Após o commit de micro-agregação em 250ms, janelas típicas do 6J acumulam 50–300 lotes, fazendo com que quase todas as janelas fossem classificadas como `vol_p >= 90` → colapso para `ABSORPTION_PASSIVE`.  
**Fix:** Thresholds atualizados para distribuição de janelas de 250ms:

| Sessão | vol_p90 antes | vol_p90 depois | imb_p90 antes | imb_p90 depois |
|---|---|---|---|---|
| ASIAN | ~10 lotes | 80 lotes | ~5 lotes | 30 lotes |
| LONDON | ~11 lotes | 200 lotes | ~5 lotes | 80 lotes |
| NEW_YORK | ~11 lotes | 300 lotes | ~5 lotes | 120 lotes |
| OFF_HOURS | ~10 lotes | 60 lotes | ~5 lotes | 20 lotes |

**Impacto residual nos módulos dependentes:**
- `SignatureProfiler.build_profile()` — substitui os fallbacks com valores empíricos após run completo
- `backtest_runner.py` — classificação agora semanticamente correta desde o primeiro batch
- `narrator.py` — `get_signal_quality()` receberá `win_rate` e `profit_factor` calibrados

---

## Integridade validada (sem fix necessário)

| Ponto | Status |
|---|---|
| `dom_bonus` para SPOOFING_WALL usa `(0,0)` como zero-bonus, não erro | ✅ Seguro |
| `post_classify` elevando DEFENSE_LINE em `hotspots` | ✅ Correto — só altera clusters do batch atual |
| `confidence` clampada em [0.0, 1.0] | ✅ Presente |
| Fallback `_fallback_profile()` tem chave `"signatures": {}` | ✅ Paridade com output do profiler |

---

## Dependência crítica de operações

> O `profile.json` carregado no `__init__` **não é recarregado em runtime**.  
> Após recalibração pelo `SignatureProfiler`, o serviço de produção (`main.py`) precisa de **restart** para carregar o novo perfil.
