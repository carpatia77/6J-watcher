# 6J Watcher — Architecture & Technical Documentation

> **Version:** v0.1.0-beta  
> **Last Audit:** 2026-06-05  
> **Status:** ✅ Core backend sem bugs de lógica conhecidos. Pronto para coleta de dados de produção.

---

## 1. Visão Geral

O **6J Watcher** é uma **Plataforma de Inteligência de Liquidez Institucional** para o contrato futuro de Iene Japonês (6J) na CME. Não é um sistema de execução automática — é um **Bloomberg Terminal focado em microestrutura**, onde o trader humano toma a decisão final com base em padrões identificados pela plataforma.

### Proposta de Valor

- **Armazenamento histórico** de clusters de liquidez com assinaturas comportamentais
- **Pattern recognition** não-paramétrico calibrado por sessão de mercado
- **Narrativa automatizada** via LLM para síntese de comportamento institucional
- **Dashboard consultivo** com hotspots, confluências e relatório diário

---

## 2. Arquitetura em Camadas

```
┌─────────────────────────────────────────────────────────────────┐
│                  6J WATCHER — INTELLIGENCE TIER                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [Data Layer]      MQL5 Bridge → HTTP POST /ingest               │
│                    parser_tsdom.py normaliza T&S + DOM           │
│                                                                  │
│  [Matrix Layer]    liquidity_matrix.py                           │
│                    Matriz 2D em memória: price × time_bucket     │
│                    Thread-safe com RLock + snapshot/restore      │
│                                                                  │
│  [Pattern Layer]   adaptive_pattern_engine.py                    │
│                    Classificador não-paramétrico por percentis   │
│                    Thresholds recalibrados via signature_profiler │
│                                                                  │
│  [Persistence]     repository_duckdb.py                          │
│                    DuckDB colunar local — tape, DOM, clusters    │
│                    Transações ACID + índices temporais           │
│                                                                  │
│  [Narrative Layer] narrator.py + llm_client.py                   │
│                    Smart alerts, confluências, relatório Markdown │
│                    LLM: NVIDIA API (Llama 3B + DeepSeek V4)      │
│                                                                  │
│  [Human Layer]     GET /report → Trader decide ENTRADA           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Fluxo de Dados

```
MetaTrader 5 (MQL5)
       │
       │  HTTP POST /ingest
       │  { "tape": [...], "dom": [...], "symbol": "6J" }
       ▼
  main.py (ThreadingHTTPServer :8765)
       │
       ▼
  ingestion.py — IngestionService.ingest_batch()
       │
       ├── 1. parser_tsdom.py    → TapeEvent[], DOMLevel[]
       │
       ├── 2. AdaptivePatternEngine.classify()
       │        → BehaviorSignature por cluster (O(1) via stateful cursor)
       │
       ├── 3. DuckDBRepository.begin() / insert_* / commit()
       │        → Persiste tape_events, dom_levels, liquidity_clusters
       │
       ├── 4. LiquidityMatrix.build_from_events()
       │        → Atualiza matriz 2D em memória
       │
       ├── 5. AdaptivePatternEngine.post_classify()
       │        → Eleva assinatura se nível tem 3+ eventos defensivos (DEFENSE_LINE)
       │
       └── 6. narrator.invalidate_cache()
                → Garante que próximo GET /report reflete dados novos

background_scheduler (daemon thread, tick 30s)
       ├── Prune stale data (>4h) da LiquidityMatrix
       ├── Recalibra profile.json a cada 30 min via SignatureProfiler
       └── Gera relatório de fechamento às 22h UTC (CME close)
```

---

## 4. Módulos

### 4.1 `main.py` — Entrypoint

Sobe o `ThreadingHTTPServer` em `127.0.0.1:8765` e orquestra os serviços.

**Endpoints:**

| Método | Path | Descrição |
|--------|------|-----------|
| POST | `/ingest` | Recebe batch de T&S + DOM do MQL bridge |
| GET | `/hotspots` | Retorna hotspots ativos em JSON |
| GET | `/report` | Retorna relatório Markdown completo |

**Scheduler (daemon thread):**
- `30s` — `LiquidityMatrix.prune_stale_data(hours=4)`
- `30min` — `SignatureProfiler.build_profile()` → salva `profile.json`
- `22h UTC` — Relatório de fechamento salvo em `daily_reports` no DuckDB

**Graceful shutdown:** `atexit` registra `asyncio.run(llm_client.close())` para fechar o `httpx.AsyncClient` sem file descriptors zumbis.

---

### 4.2 `ingestion.py` — IngestionService

Pipeline principal de ingestão. Garante que apenas dados commitados no banco alimentam a análise.

**Características:**
- **Cold start fix:** `last_closed_price` inicializado do DuckDB para evitar `delta_price_ticks=0` no primeiro evento após restart
- **Transação ACID:** `begin()` → inserts → `commit()` com `rollback()` explícito em exceção
- **Snapshot/restore da LiquidityMatrix:** se o `build_from_events` falhar, a matriz em memória reverte para o estado anterior
- **Post-classify isolado:** `post_classify` é aplicado apenas aos clusters do `batch_id` atual — não reclassifica histórico

---

### 4.3 `liquidity_matrix.py` — LiquidityMatrix

Matriz 2D em memória que indexa clusters por `(price_bucket, time_bucket)`.

**Estruturas internas:**

| Estrutura | Chave | Valor | Uso |
|-----------|-------|-------|-----|
| `matrix` | `price → time_bucket` | `List[LiquidityCluster]` | Histórico completo por nível |
| `dom_snapshots` | `price → time_bucket` | `List[DOMLevel]` | Snapshots do book por nível |
| `tape_index` | `price → time_bucket` | `List[TapeEvent]` | T&S indexado por preço |
| `active_levels` | `price` | `List[LiquidityCluster]` | Hotspots das últimas 4h |

**Thread safety:** `threading.RLock()` protege todas as operações de leitura/escrita.

**`prune_stale_data(hours=4)`:** Remove buckets com timestamp anterior ao corte. Garante que `active_levels` só contém dados da janela operacional.

**`hotspots(min_occurrences=3)`:** Retorna níveis com `N+` clusters, ordenados por ocorrências, com assinatura dominante e confiança média.

---

### 4.4 `adaptive_pattern_engine.py` — AdaptivePatternEngine

Classificador não-paramétrico baseado em **percentis empíricos por sessão** e **deslocamento de preço**.

**Thresholds carregados de `profile.json`** (gerado pelo `SignatureProfiler`). Fallback hardcoded por sessão quando o arquivo não existe.

**Ordem de classificação em `classify()` (sequencial — a ordem importa):**

```
1. ABSORPTION_PASSIVE   vol_p≥90 AND imb_p≥90 AND |delta|≤1
2. BREAKOUT_GENUINE     vol_p≥75 AND imb_p≥75 AND |delta|≥2
3. SPOOFING_WALL        vol_p≥75 AND imb_p<50  AND delta==0   ← ANTES do ICEBERG (crítico)
4. ICEBERG_ACC/DIST     vol_p≥75 AND delta==0  AND imb_p<90
5. LIQUIDITY_VACUUM     vol_p<50 AND |delta|≥2
6. UNKNOWN              (fallthrough)
```

> ⚠️ **Nota de ordem:** SPOOFING_WALL deve vir ANTES de ICEBERG porque a condição do ICEBERG (`imb_p < 90`) engloba `imb_p < 50`. Sem essa ordenação, SPOOFING nunca seria emitido.

**`post_classify(price, clusters)`:** Eleva assinatura dominante para `DEFENSE_LINE` se 3+ clusters defensivos (ABSORPTION_PASSIVE, ICEBERG_*) ocorreram no mesmo nível.

**`get_signal_quality(signature, session)`:** Consulta `profile.json` para retornar `win_rate`, `profit_factor` e `sample_size` históricos.

---

### 4.5 `signature_profiler.py` — SignatureProfiler

Calcula **percentis empíricos** e **MFE/MAE** via DuckDB window functions. Zero risco de OOM — toda a agregação roda no motor C++ do DuckDB.

**Pipeline:**
1. JOIN `liquidity_clusters` × `tape_events` na janela de `horizon_minutes` (padrão: 30min)
2. Calcula MFE/MAE por cluster com direção inferida de `cumdelta`
3. Agrega `win_rate`, `profit_factor`, `avg_mfe` por `(signature, session)`
4. Calcula `QUANTILE_CONT` de volume e imbalance por sessão
5. Salva em `profile.json`

**Proteção de amostras insuficientes:** Se uma sessão tem menos de 100 amostras, usa fallback hardcoded em vez de percentis instáveis.

**Recalibração:** O `background_scheduler` chama `build_profile()` a cada 30 minutos durante o horário de mercado.

---

### 4.6 `repository_duckdb.py` — DuckDBRepository

Camada de persistência histórica com DuckDB colunar local.

**Schema:**

| Tabela | Uso | Índices |
|--------|-----|---------|
| `tape_events` | T&S bruto normalizado | `(symbol, timestamp)` |
| `dom_levels` | Snapshots do book | `(symbol, timestamp)` |
| `liquidity_clusters` | Clusters classificados | `(symbol, timestamp)` |
| `key_levels` | Níveis recorrentes históricos | PK `(symbol, price)` |
| `daily_reports` | Relatórios de fechamento | PK `(symbol, date)` |

**Queries críticas:**
- `signature_distribution(symbol)` — distribuição de assinaturas para o relatório
- `recurring_levels(symbol, min_occurrences)` — hotspots históricos com `MODE(behavior_signature)`
- `session_analysis(symbol)` — breakdown por sessão Asian/London/NY
- `recent_tape/dom(symbol, minutes)` — janela temporal com `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'`

> ⚠️ **Timezone:** Queries de janela temporal usam `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'` para ancorar em UTC independentemente do timezone do S.O. O `INTERVAL` usa f-string interpolada (`{int(minutes)}`) pois DuckDB não aceita bind parameters dentro de literais `INTERVAL`.

---

### 4.7 `narrator.py` — Narrator

Orquestrador cognitivo da última milha. Transforma dados da matriz e do banco em inteligência acionável.

**Funcionalidades:**

**Smart Alerts:** Filtra sinais por Tier, `min_alert_win_rate` (padrão 50%) e `min_alert_sample_size` (padrão 30 amostras). Tier 3 (SPOOFING_WALL, LIQUIDITY_VACUUM) é suprimido por padrão.

**Confluências:** Detecta padrões compostos cruzando hotspots por proximidade de preço (`confluence_tick_tolerance = 20 ticks`):

| Confluência | Componentes | Interpretação |
|-------------|-------------|---------------|
| `BREAKOUT_AT_DEFENSE` | BREAKOUT_GENUINE + DEFENSE_LINE | Alta probabilidade de continuação |
| `ACCUMULATION_ABSORPTION` | ICEBERG_ACCUMULATION + ABSORPTION_PASSIVE | Reversão iminente |
| `DISTRIBUTION_ABSORPTION` | ICEBERG_DISTRIBUTION + ABSORPTION_PASSIVE | Teto ativo, evitar compras |

**Cache TTL 5 minutos:** Evita reprocessamento em múltiplas chamadas ao `/report`. Cache invalidado em cada `ingest_batch`. Hash key via MD5 dos campos relevantes dos hotspots.

**LLM Integration (2 estágios):**
1. **Llama 3B** (NVIDIA API) — estrutura dados brutos em contexto legível
2. **DeepSeek V4** (NVIDIA API) — raciocina sobre o contexto e gera narrativa institucional

Fallback para template local se LLM indisponível.

---

### 4.8 `llm_client.py` — NvidiaLLMClient

Client assíncrono (`httpx.AsyncClient`) para a NVIDIA API.

**Proteções:**
- **Rate limiting:** máximo `N` calls/hora com janela deslizante
- **Circuit breaker:** abre após 3 falhas consecutivas; fecha automaticamente após sucesso
- **Timeout configurável:** padrão 5s por chamada
- **Graceful degradation:** retorna `None` em qualquer falha — o Narrator usa fallback local

---

### 4.9 `models.py` — Modelos de Domínio

```python
class Side(str, Enum):          # buy / sell / unknown
class BehaviorSignature(str, Enum)  # 9 assinaturas + UNKNOWN

@dataclass TapeEvent            # Evento de T&S normalizado
@dataclass DOMLevel             # Nível do book normalizado
@dataclass LiquidityCluster     # Cluster classificado com metadados
@dataclass KeyLevel             # Nível histórico persistido
```

> **MAGNET_EFFECT:** Mantido no Enum com deprecation notice. Nunca emitido pelo classificador atual — requer rastreamento de convergência de preço ao longo do tempo. Não remover para não quebrar queries históricas.

---

### 4.10 `mql_bridge.mq5` — MQL5 Bridge

EA (Expert Advisor) para MetaTrader 5 que publica dados do ClusterDelta para o servidor Python.

**Características:**
- Coleta T&S (tape) e snapshots do DOM a cada tick
- HTTP POST para `http://127.0.0.1:8765/ingest` com payload JSON
- Client ID gerado com entropia via `MathRand()` para evitar colisões
- Reconexão automática com backoff exponencial
- Buffer local para batches — não bloqueia o fluxo do tick

---

### 4.11 `config.py` — Config

Dataclass central de configuração. Carrega `.env` automaticamente se existir.

**Parâmetros relevantes:**

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `symbol` | `"6J"` | Símbolo CME |
| `tick_size` | `0.00005` | Tick mínimo do 6J |
| `db_path` | `output/6j_liquidity.db` | Caminho do DuckDB |
| `host:port` | `127.0.0.1:8765` | Servidor HTTP |
| `min_occurrences` | `3` | Mínimo para hotspot |
| `min_alert_win_rate` | `0.50` | Win rate mínimo para smart alert |
| `min_alert_sample_size` | `30` | Amostras mínimas para smart alert |
| `confluence_tick_tolerance` | `20` | Ticks de proximidade para confluência |
| `llm_max_calls_hour` | `100` | Budget de chamadas LLM/hora |

> **Sessões (UTC):** ASIAN 00-08h | LONDON 08-13h | NEW_YORK 13-22h | OFF_HOURS resto

---

## 5. As 8 Behavior Signatures

| Signature | Tier | Descrição | Valor Operacional |
|-----------|------|-----------|-------------------|
| `ABSORPTION_PASSIVE` | 1 | Agressão extrema sem deslocar preço | Baleia absorvendo — sinal de reversão |
| `BREAKOUT_GENUINE` | 1 | Agressão consome liquidez e desloca preço | Movimento institucional real — seguir |
| `DEFENSE_LINE` | 1 | Nível defendido 3+ vezes (post-classify) | Suporte/resistência institucional forte |
| `ICEBERG_ACCUMULATION` | 2 | Parede passiva de compra absorvendo vendas | Baleia acumulando — expectativa de alta |
| `ICEBERG_DISTRIBUTION` | 2 | Parede passiva de venda absorvendo compras | Baleia distribuindo — expectativa de baixa |
| `MAGNET_EFFECT` | 2 | ⚠️ DEPRECATED — não emitido atualmente | Requer rastreamento de convergência |
| `SPOOFING_WALL` | 3 | Volume alto, imbalance baixo, preço parado | Falsa liquidez — não confiar no nível |
| `LIQUIDITY_VACUUM` | 3 | Volume baixo, preço desloca muito | Zona de aceleração — preço atravessa rápido |

> **Tier 1** — Alta confiança direcional/reversão  
> **Tier 2** — Contexto/acumulação (requer confluência para máxima confiança)  
> **Tier 3** — Filtros/ruído (suprimidos nos smart alerts por padrão)

---

## 6. Dependências

```
duckdb==0.10.3      # banco colunar local (pin para evitar breaking changes v1.x)
pandas>=2.0.0       # manipulação de DataFrames no profiler
numpy>=1.24.0       # arrays numéricos
httpx>=0.27.0       # client HTTP assíncrono para NVIDIA API
```

**Variável de ambiente opcional:**
```
NVIDIA_API_KEY=...  # habilita narrativa LLM (Llama 3B + DeepSeek V4)
```

---

## 7. Histórico de Auditorias e Fixes

### Auditoria 1 — 2026-06-05 (commit `3396bb3`)
**Escopo:** Primeira revisão completa do projeto pós-realinhamento estratégico.

| Severidade | Problema | Status |
|------------|----------|--------|
| P0 | Múltiplos `CREATE INDEX` em um único `execute()` (DuckDB não suporta) | ✅ Corrigido |
| P0 | Cold start: `last_closed_price = None` causava `delta_price_ticks = 0` sempre | ✅ Corrigido |
| P0 | Sem transação ACID no pipeline de ingestão | ✅ Corrigido |
| Alta | `pattern_engine.py` (legado) ainda ativo em paralelo com o novo engine | ✅ Isolado |
| Alta | Profiler sem proteção de lock e sem retry em caso de falha | ✅ Corrigido |
| Média | Relatório diário não persistido no banco | ✅ Implementado |

### Auditoria 2 — 2026-06-05 (commit `62dd98b`)
**Escopo:** Pente-fino pós-realinhamento para pronta operacionalidade.

| Severidade | Problema | Status |
|------------|----------|--------|
| Alta | `SPOOFING_WALL` nunca emitido — condição encoberta pelo `ICEBERG` (`imb_p < 90` engolia `imb_p < 50`) | ✅ Corrigido — reordenação das condições |
| Alta | `session_for()` retornava `"off_hours"` (minúsculo) vs `"OFF_HOURS"` no fallback profile | ✅ Corrigido — padronizado para maiúsculo |
| Alta | `INTERVAL ? MINUTE` com bind parameter inválido no DuckDB | ✅ Corrigido — f-string com `int(minutes)` |
| Média | `NOW()` sem qualificador UTC em `recent_tape`/`recent_dom` | ✅ Corrigido — `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'` |
| Alta | `requirements.txt` sem `httpx` e sem pins de versão | ✅ Corrigido — pins adicionados |
| Baixa | `httpx.AsyncClient` sem `aclose()` no shutdown | ✅ Corrigido — `atexit` handler |
| Baixa | `print(hotspots[0])` sem `json.dumps` no loop principal | ✅ Corrigido |

---

## 8. Roadmap

### ✅ Concluído (v0.1.0-beta)
- Pipeline de ingestão T&S + DOM com transação ACID
- LiquidityMatrix 2D thread-safe com snapshot/restore
- Classificador não-paramétrico com 7 signatures ativas
- SignatureProfiler com MFE/MAE via DuckDB window functions
- Narrator com smart alerts, confluências e cache TTL
- LLM integration com circuit breaker e rate limiting
- Scheduler autônomo (prune + recalibração + relatório diário)
- MQL5 bridge com reconexão e batching

### 🔲 Pendente (v0.2.0)
- **Dashboard Streamlit** com heatmaps de liquidez e gráficos de hotspots
- **Alertas push** (Telegram ou desktop notification) para smart alerts em tempo real
- **MAGNET_EFFECT** implementado com rastreamento de convergência de preço
- **Backtesting formal** das 8 signatures com lookback configurável
- **Multi-ativo** (ES, NQ, CL, GC) com correlação entre ativos

---

## 9. Execução

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar chave LLM (opcional)
cp .env.example .env
# editar .env com NVIDIA_API_KEY=...

# Iniciar plataforma
python main.py
# → 6J Watcher running on http://127.0.0.1:8765

# Endpoints disponíveis
curl http://127.0.0.1:8765/hotspots
curl http://127.0.0.1:8765/report
```

---

*Documentação gerada em 2026-06-05. Para contribuir, abrir issue ou PR no repositório.*
