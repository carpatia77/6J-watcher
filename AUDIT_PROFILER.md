# AUDIT — signature_profiler.py

> Última revisão: **2026-06-08**  
> Revisado por: Perplexity / carpatia77  
> Commits desta sessão: [`996707f`](https://github.com/carpatia77/6J-watcher/commit/996707fc2502f5fb134974f62705cb48228e0205)

---

## Arquitetura geral

`SignatureProfiler` calcula MFE/MAE histórico via DuckDB CTE e gera tabelas de percentis
empíricos para normalização não-paramétrica de Order Flow. O output (`profile.json`) é
consumido pelo `AdaptivePatternEngine` para classificar `vol_p` e `imb_p`.

O profiler lê `liquidity_clusters` e faz JOIN com `tape_events` usando `timestamp_ns` BIGINT
quando disponível (backtest Databento) ou `TIMESTAMP` como fallback (produção MQL5).

---

## Fixes aplicados — sessão 2026-06-08

### FIX-PROF-01 — MFE/MAE: direção correta por assinatura
**Severidade:** 🔴 `win_rate` e `profit_factor` distorcidos para 3 assinaturas  
**Problema:** O `CASE` de MFE usava apenas dois ramos:
- Bullish: `iceberg_accumulation`, `breakout_genuine`
- Bearish: todo o resto (ELSE)

Isso colocava incorretamente no ramo bearish:
- `defense_line` — linha defensiva de **compra**, MFE deveria ser bullish
- `spoofing_wall` — neutro, MFE deveria ser `GREATEST(up, down)`
- `liquidity_vacuum` — neutro, idem

**Fix:** CASE expandido para 3 ramos:
```sql
CASE
    WHEN sig IN ('iceberg_accumulation','breakout_genuine','defense_line')
         THEN max_future - c_price          -- bullish
    WHEN sig IN ('iceberg_distribution','absorption_passive')
         THEN c_price - min_future          -- bearish
    ELSE GREATEST(max_future - c_price,
                  c_price - min_future)     -- neutro
END AS mfe
```
MAE espelha o mesmo mapeamento.

**Impacto residual nos módulos dependentes:**
- `backtest_runner.save_report()` → `build_profile()` → agora gera `win_rate` correto para `defense_line`
- `narrator.py` → `get_signal_quality()` → `profit_factor` real para `spoofing_wall` e `liquidity_vacuum`
- `profile.json` existente (pré-fix) contém valores distorcidos — **deve ser deletado antes do próximo run**

---

### FIX-PROF-02 — Fallback thresholds recalibrados para 250ms
**Severidade:** 🔴 Fallback inutilizável após migração para janelas de 250ms  
**Problema:** `_get_fallback_thresholds()` tinha vol_p90 de 5–11 lotes (distribuição de tick único). Após a mudança arquitetural do `ingestion.py` para janelas de 250ms, o fallback ficou uma ordem de grandeza abaixo da realidade.  
**Sintoma:** Qualquer sessão com < 100 amostras usaria o fallback e classificaria quase toda janela como `vol_p >= 99`, colapsando sinalização.

**Fix — novos valores por sessão:**

| Sessão | vol_p50 | vol_p75 | vol_p90 | vol_p95 | vol_p99 |
|---|---|---|---|---|---|
| ASIAN | 20 | 40 | 80 | 120 | 200 |
| LONDON | 50 | 100 | 200 | 300 | 500 |
| NEW_YORK | 70 | 150 | 300 | 450 | 700 |
| OFF_HOURS | 15 | 30 | 60 | 90 | 150 |

| Sessão | imb_p50 | imb_p75 | imb_p90 | imb_p95 | imb_p99 |
|---|---|---|---|---|---|
| ASIAN | 8 | 15 | 30 | 50 | 80 |
| LONDON | 20 | 40 | 80 | 120 | 200 |
| NEW_YORK | 30 | 60 | 120 | 180 | 300 |
| OFF_HOURS | 5 | 10 | 20 | 35 | 60 |

**Impacto residual:** Fallback usado apenas quando sessão tem < 100 amostras no banco. Com 8 meses de dados, só sessões de feriado / primeiro dia cairão aqui.

---

### FIX-PROF-03 — `metadata.window_ms: 250` adicionado ao output
**Severidade:** 🟡 Auditabilidade  
**Fix:** Todo `profile.json` gerado agora carrega `"window_ms": 250` em `metadata`. Um `profile.json` gerado com lógica de tick único (sem essa chave) é imediatamente identificável como stale.

---

## Integridade validada (sem fix necessário)

| Ponto | Status |
|---|---|
| `timestamp_ns` existe em todas as tabelas do schema | ✅ `repository_duckdb.py` confirma coluna + guards `ALTER TABLE` |
| `filter_dates` sanitização de aspas simples | ✅ `chr(39)` removal presente |
| Fallback para produção sem `timestamp_ns` via `TIMESTAMP` | ✅ CASE no JOIN cobre ambos |
| `QUANTILE_CONT` DuckDB — nome correto | ✅ (não `PERCENTILE_CONT`) |
| `MIN_SAMPLES=100` com warning antes do fallback | ✅ Log `[Profiler] Sessao X com Y amostras` |

---

## Procedimento de invalidação de perfil

Sempre que houver mudança arquitetural no pipeline de agregação, deletar o perfil antes do run:

```powershell
# Windows
del data\backtest_profile.json
del data\profile_8months.json   # se existir

# Linux / Mac
rm data/backtest_profile.json
```

O `AdaptivePatternEngine` cairá no `_fallback_profile()` (calibrado para 250ms) e o `SignatureProfiler` regerará o perfil empírico ao final do `backtest_runner.run()`.
