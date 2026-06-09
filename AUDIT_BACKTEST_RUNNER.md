# AUDIT — backtest/backtest_runner.py

> Última revisão: **2026-06-08**  
> Revisado por: Perplexity / carpatia77  
> Commits desta sessão: [`13cf643`](https://github.com/carpatia77/6J-watcher/commit/13cf6431e9b8de1dc9e4457de625fe0eab92c943)

---

## Arquitetura geral

`BacktestRunner` orquestra o backtest completo de 8 meses:
1. Download / leitura de arquivo `.dbn.zst` via `DatabentoLoader`
2. Streaming em batches de N segundos via `DatabentoAdapter`
3. Ingestão por `IngestionService.ingest_batch()`
4. CHECKPOINT + prune da `LiquidityMatrix` a cada 500 batches
5. Calibração do `SignatureProfiler` ao final do run
6. Geração de relatório narrativo

---

## Fixes aplicados — sessão 2026-06-08

### FIX-BTR-01 — `top_n=10` passado para `ingest_batch`
**Severidade:** 🟡 Cobertura de sinal  
**Problema:** `ingest_batch` era chamado sem `top_n`, usando default=5. O 6J tem atividade relevante do Compound Man nos níveis 6–10 do Book (Icebergs abaixo do Top-5 visível).  
**Fix:**
```python
# antes
clusters = self.service.ingest_batch(tape_rows, dom_rows, symbol)
# depois
clusters = self.service.ingest_batch(tape_rows, dom_rows, symbol, top_n=10)
```
**Custo:** Zero adicional — `_dom_at` opera em O(log K), lista maior não degrada.  
**Impacto residual:** Apenas o backtest usa `top_n=10`. Produção (`main.py`) mantém default=5 — comportamento inalterado.

---

## Integridade validada (sessões anteriores)

### FIX-BTR-02 — `_last_market_ts` reset entre chunks
**Problema original:** `prune_stale_data` recebia timestamp de 2026 para podar dados de 2025 quando o runner processava chunks mensais sequencialmente.  
**Fix:** `self._last_market_ts = None` no início de cada chunk do `run_backtest_historical.py`.  
**Status:** ✅ Resolvido.

### FIX-BTR-03 — `skip_dom=False` no runner de produção
**Problema original:** `BacktestRunner` inicializava com `skip_dom=True` por default.  
**Fix:** `skip_dom=False` — DOM alimenta `_dom_at()` e o `dom_bonus` de SPOOFING_WALL.  
**Status:** ✅ Resolvido.

### FIX-BTR-04 — SQL date filter em `run_backtest_historical.py`
**Problema original:** Query de verificação de dados existentes usava concatenação de string não sanitizada.  
**Fix:** Parâmetros via `?` placeholder do DuckDB.  
**Status:** ✅ Resolvido.

---

## Fluxo de dados crítico

```
DatabentoLoader.download()
  → DatabentoAdapter.stream_batches()       # yields (tape_rows, dom_rows)
    → IngestionService.ingest_batch(
          tape_rows, dom_rows, symbol,
          top_n=10                           # ← FIX-BTR-01
      )
      → _build_dom_index(top_n=10)          # cobre níveis 6-9 do Book
      → _build_clusters_from_windows()       # janelas de 250ms
      → AdaptivePatternEngine.classify()     # usa fallback 250ms até profiler rodar
      → repo.insert_clusters()
  → CHECKPOINT + prune @ 500 batches
→ SignatureProfiler.build_profile()          # calibração final
→ Narrator.daily_report()                   # relatório narrativo
```

---

## Procedimento de run limpo

```powershell
# Deletar banco e perfil stale antes de cada run completo de 8 meses
del data\backtest_8months.db
del data\profile_8months.json
python run_backtest_historical.py
```

> **Nota:** Não deletar o banco entre runs parciais (skip_download=True) — apenas o perfil.
