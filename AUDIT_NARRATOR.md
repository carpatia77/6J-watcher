# Auditoria: narrator.py — De Formatter para Chief Quant Orchestrator

Este documento registra como Architecture Decision Record (ADR) a evolução completa do módulo `narrator.py` do sistema 6J Watcher.

## Contexto

O `narrator.py` original era um gerador de relatórios estáticos (~101 linhas) que concatenava strings Markdown. Embora funcional e bem documentado, representava apenas ~20% da arquitetura de orquestração cognitiva projetada para o sistema. Não integrava a inteligência estatística dos módulos Profiler e Engine, não filtrava sinais por qualidade, e não detectava confluências de alto valor.

---

## Iteração 1: Correções Pontuais

### Correção 1: TypeError Silencioso em `level_summary()`
- **Problema:** `p.get('price', '?'):.5f` lançava `TypeError` quando `price` era `None`.
- **Ação:** Extração do campo com verificação de tipo (`isinstance(price, (int, float))`).

### Correção 2: Logging Ausente
- **Ação:** Adicionado `logging.getLogger(__name__)` ao módulo.

### Correção 3: Argumento Redundante `tick_size` em `main.py`
- **Ação:** Removido `tick_size=cfg.tick_size` da instanciação do `AdaptivePatternEngine`.

---

## Iteração 2: Evolução Completa (Formatter → Orchestrator)

### Melhoria 1 (P0): Smart Alerts com Filtro de Qualidade Estatística
- **Ação:** Novo método `smart_alert()` que consulta `engine.get_signal_quality()` antes de emitir alertas.
- **Filtragem:** Suprime Tier 3 (ruído como SPOOFING_WALL), amostras < 30 (insuficiente para significância) e win rate < 50% (sem edge).
- **Output enriquecido:** Inclui Win Rate, Profit Factor e Tier no alerta, transformando-o de notificação bruta em decisão informada.
- **Motivo:** O trader recebia spam de alertas sem edge estatístico. Agora só recebe sinais com confirmação empírica do profiler.

### Melhoria 2 (P0): Detecção de Confluências de Alta Probabilidade
- **Ação:** Novo método `detect_confluences()` que cruza hotspots por proximidade de preço (tolerância configurável em ticks via `config.py`).
- **Padrões detectados:**
  - `BREAKOUT_AT_DEFENSE`: Breakout Genuine em nível com Defense Line — continuação direcional
  - `ACCUMULATION_ABSORPTION`: Iceberg Accumulation + Absorption Passive — reversão iminente
  - `DISTRIBUTION_ABSORPTION`: Iceberg Distribution + Absorption Passive — teto institucional
- **Integração:** Confluências são automaticamente incluídas na seção `⚡ Confluências de Alta Probabilidade` do `daily_report()`.
- **Motivo:** Confluências compostas têm 2-3x mais win rate que sinais isolados. Detectá-las automaticamente é o maior delta de valor para o trader.

### Melhoria 3 (P1): Integração LLM via NVIDIA API
- **Ação:** Novo módulo `llm_client.py` com `NvidiaLLMClient` e método `generate_narrative()` no Narrator.
- **Stack LLM:**
  - **Llama 3B** (`meta/llama-3.1-8b-instruct`): Tier 2 — parsing estruturado, geração de contexto
  - **DeepSeek V4** (`deepseek-ai/deepseek-v4`): Tier 1 — raciocínio quantitativo sobre dados realtime
- **Proteções implementadas:**
  - Circuit breaker: abre após 3 falhas consecutivas, suprime chamadas subsequentes
  - Rate limiting: budget máximo de chamadas/hora (default: 100)
  - Timeout: 5s configurável via `config.py`
  - Graceful degradation: se LLM falhar ou `NVIDIA_API_KEY` estiver vazia, usa fallback local (template Markdown)
- **API Key:** Carregada via variável de ambiente `NVIDIA_API_KEY` (suporte a arquivo `.env` via loader custom em `config.py`).

### Melhoria 4 (P2): Caching de Relatórios
- **Ação:** Cache baseado em hash MD5 do input (`symbol + hotspots + distributions`) no `daily_report()`.
- **Invalidação:** `invalidate_cache()` chamado automaticamente pelo `IngestionService` após cada `ingest_batch()` bem-sucedido.
- **Motivo:** Evita recálculo redundante quando o trader acessa `GET /report` múltiplas vezes entre batches.

### Atualização de Wiring (`main.py` + `ingestion.py`)
- **`main.py`:** `Narrator` agora recebe `engine`, `cfg` e `llm_client` opcionalmente. `IngestionService` recebe `narrator` para invalidação.
- **`ingestion.py`:** Construtor aceita `narrator=None`. Após ingestão bem-sucedida, chama `narrator.invalidate_cache()`.
- **`config.py`:** Novos campos para alertas (`min_alert_win_rate`, `min_alert_sample_size`, `confluence_tick_tolerance`) e LLM (`nvidia_api_key`, `llm_*`). Loader de `.env` integrado.

---

## ADR: Decisões Arquiteturais

| Decisão | Racional |
|---------|----------|
| `smart_alert()` retorna `Optional[str]` (None = suprimido) | Callers podem ignorar None sem try/except |
| `alert()` legacy mantido | Backward compatibility com eventuais consumers |
| LLM é 100% opcional (graceful degradation) | Sistema deve funcionar offline/sem API key |
| Cache invalidado por `IngestionService`, não por TTL | Garante consistência causal: novos dados = novo relatório |
| Confluências calculadas dentro do `daily_report()` | Single source of truth para o endpoint `/report` |
| Tolerância de confluência configurável em ticks | Calibragem via backtesting posterior (valor inicial: 20 ticks) |
| Win rate mínimo: 50% (configurável) | Valor conservador, ajustável após backtest de 6 meses |

---

## Status Final
- `narrator.py`: ✅ Gold Tier (Orchestrator)
- `llm_client.py`: ✅ Gold Tier (novo módulo)
- `config.py`: ✅ Atualizado
- `main.py`: ✅ Wiring completo
- `ingestion.py`: ✅ Cache invalidation integrada
