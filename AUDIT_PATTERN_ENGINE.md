# Auditoria e Refatoração: Pattern Engine (V1 → V2)

Este documento registra como um Architecture Decision Record (ADR) a migração do motor de classificação de clusters do sistema 6J Watcher, partindo de uma implementação monolítica estática (V1) para uma arquitetura bi-modular adaptativa (V2).

## Contexto: Por Que o V1 Foi Deprecado

O `pattern_engine.py` original utilizava regras heurísticas com thresholds hardcoded para classificar `LiquidityCluster` em `BehaviorSignature`. Essa abordagem apresentava três falhas estruturais fundamentais:

### Falha #1: Thresholds Estáticos (Cross-Session Failure)
- **Problema:** Valores como `total >= 20` ou `imbalance_ratio >= 0.5` eram absolutos e fixos. O volume médio na sessão Asiática é ordens de magnitude menor que na sessão de Nova York. Um threshold que funciona em NY classifica tudo como `UNKNOWN` na Ásia.
- **Impacto:** Perda massiva de sinais durante sessões de baixa liquidez.

### Falha #2: Falácia Gaussiana (Normalização Inválida)
- **Problema:** Qualquer tentativa de normalizar os volumes via Z-Score (Média + Desvio Padrão) seria matematicamente incorreta. Distribuições de volume em mercados financeiros são leptocúrticas (fat-tailed): spikes de volume como o Non-Farm Payroll ou decisões do FOMC distorcem a média e o desvio padrão, tornando Z-Scores inúteis.
- **Impacto:** Falsos positivos massivos durante eventos de alta volatilidade.

### Falha #3: Ausência de Tick Displacement (ΔP)
- **Problema:** O V1 classificava "Absorção" baseado apenas em volume alto e imbalance baixo. Porém, na microestrutura real, Absorção requer que o preço NÃO se desloque (ΔP ≤ 1 tick) apesar do volume. O V1 não recebia nem processava essa informação.
- **Impacto:** Confusão entre Absorção e simples consolidação de preço.

---

## Arquitetura V2: Sistema Bi-Modular Adaptativo

A solução adotada decompõe o problema em duas responsabilidades completamente desacopladas:

### Módulo A: `signature_profiler.py` (Offline Calibrator)
- **Responsabilidade:** Análise histórica via DuckDB SQL Window Functions. Calcula MFE/MAE (Maximum Favorable/Adverse Excursion) e gera tabelas de percentis empíricos por sessão.
- **Saída:** Arquivo `profile.json` contendo os percentis p50, p75, p90, p95, p99 para volume e imbalance, segmentados por sessão (ASIAN, LONDON, NEW_YORK, OFF_HOURS).
- **Frequência:** Executado periodicamente (ex: diário, pré-pregão) para recalibrar os thresholds com base nos últimos N dias de dados.
- **Princípio:** Rank Normalization (percentis empíricos) em vez de Z-Scores. Imune a outliers.

### Módulo B: `adaptive_pattern_engine.py` (Online Classifier)
- **Responsabilidade:** Classificação em tempo real com latência O(1). Carrega `profile.json` em memória e compara cada cluster contra os percentis da sessão corrente.
- **Parâmetro Obrigatório:** `delta_price_ticks` (ΔP) — número de ticks que o preço se deslocou durante a formação do cluster.
- **Heurísticas de Microestrutura:**
  - `ABSORPTION_PASSIVE`: Volume ≥ p90, Imbalance ≥ p90, |ΔP| ≤ 1 tick
  - `BREAKOUT_GENUINE`: Volume ≥ p75, Imbalance ≥ p75, |ΔP| ≥ 2 ticks
  - `ICEBERG_ACCUMULATION/DISTRIBUTION`: Volume ≥ p75, |ΔP| = 0, Imbalance < p90
  - `DEFENSE_LINE`: 3+ eventos defensivos no mesmo nível (via `post_classify`)
  - `MAGNET_EFFECT`: 3+ toques no mesmo nível de preço (via `post_classify`)

---

## Impacto Residual nos Módulos Existentes

| Módulo | Nível | Alteração |
|---|---|---|
| `ingestion.py` | ALTO | Import atualizado. Loop de criação de clusters agora calcula ΔP e passa para `classify()`. |
| `main.py` | MÉDIO | Instanciação de `AdaptivePatternEngine` com path do profile e tick_size. |
| `liquidity_matrix.py` | BAIXO | Sem alteração necessária (fallback `classify` usa default `delta_price_ticks=0`). |
| `requirements.txt` | BAIXO | Adição de `numpy` e `pandas`. |
| `config.py` | BAIXO | Padronização de sessões para UPPERCASE (pendente aprovação). |

## Log de Execução da Migração

### Passo 1: Padronização de Sessões (`config.py`)
- **Ação:** Modificação das chaves do dicionário `session_utc` de minúsculas para maiúsculas (`ASIAN`, `LONDON`, `NEW_YORK`).
- **Motivo:** O `signature_profiler` e o `adaptive_pattern_engine` utilizam strings UPPERCASE nativamente e as chaves do `profile.json` são exportadas dessa forma. A padronização no `config.py` evita divergências e operações de fallback ou lookup inválidas quando o engine for consultar o `thresholds[session]`.

### Passo 2: Atualização de Dependências (`requirements.txt`)
- **Ação:** Inclusão da biblioteca `numpy` (já havia `pandas`) no arquivo de dependências do projeto.
- **Motivo:** O DuckDB nativamente expõe a função `.fetchdf()` que retorna um DataFrame Pandas para facilitar a análise vetorial, e a biblioteca NumPy será utilizada extensivamente no `signature_profiler` para o cálculo otimizado e seguro dos percentis empíricos de Volume e Imbalance (`np.percentile`).

### Passo 3: Criação do Calibrador Offline (`signature_profiler.py`)
- **Ação:** Implementação do Módulo A da arquitetura V2. O módulo usa DuckDB (`read_only=True`) para ler o histórico, aplicar SQL Window Functions para calcular MFE/MAE (Maximum Favorable/Adverse Excursion) e exportar um `profile.json` com os percentis (rank normalization).
- **Ajustes Aplicados em relação ao Manifesto Original:**
  1. O SQL foi parametrizado (via f-strings) para suportar a injeção do `horizon_minutes`, evitando hardcodes.
  2. Implementado a função `LAG()` no SQL para assegurar o cálculo histórico de `delta_price_ticks`, vital para paridade de dados entre backtest e o pipeline de Ingestão em memória.
  3. O print foi substituído por chamadas ao módulo interno `logging`.

### Passo 4: Criação do Classificador Online (`adaptive_pattern_engine.py`)
- **Ação:** Implementação do Módulo B da arquitetura V2. Este módulo efetua inferência em O(1) através de rank lookup no `profile.json` carregado em memória. O classificador agora obriga o envio do `delta_price_ticks`.
- **Ajustes Aplicados em relação ao Manifesto Original:**
  1. Corrigidas as constantes `TIER` para utilizarem strings em lowercase, permitindo o correto cruzamento com `BehaviorSignature.value` no método `get_signal_quality()`.
  2. Implementado fallback de thresholds estáticos que abrange todas as quatro sessões (ASIAN, LONDON, NEW_YORK, OFF_HOURS) como contramedida de robustez.
  3. Lógica do `post_classify` corrigida. Antes o código interceptava clusters de forma não intencional gerando dead-code. Agora ele avalia a predominância primeiro e aplica MAGNET_EFFECT como elevação de prioridade quando aplicável.

### Passo 5: Atualização do Ingestion Pipeline (`ingestion.py`)
- **Ação:** O pipeline de ingestão foi atualizado para utilizar o novo `AdaptivePatternEngine`. A mudança arquitetural mais crítica (Decisão #3) foi a introdução do "Stateful Cursor" (`self.last_closed_price`).
- **Motivo:** No ambiente Live Trading de alta frequência, é proibitivo consultar o banco de dados (DuckDB) apenas para descobrir o `delta_price_ticks`. A solução implementada introduziu um cursor de estado em memória que sobrevive às transições de batch e mantém registro do último preço executado, permitindo o cálculo do ΔP (deslocamento em ticks) localmente com complexidade O(1) sem overhead de I/O, antes de acionar a classificação.

### Passo 6: Integração no Orquestrador (`main.py`)
- **Ação:** Substituição da instância do antigo `PatternEngine` pela nova classe `AdaptivePatternEngine`. Configurada a inicialização passando o caminho absoluto do arquivo `profile.json` carregado a partir do `BASE_DIR` e injetando o `tick_size` via Configuração Global.

### Passo 7: Deprecação do V1 (`pattern_engine.py`)
- **Ação:** O arquivo `pattern_engine.py` recebeu um header de deprecação (`# DEPRECATED — replaced by adaptive_pattern_engine.py (V2)`).
- **Motivo:** Conforme a Decisão #1, em vez de deletar o arquivo e correr o risco de quebrar dependências de commits antigos (git history), o arquivo é mantido congelado para preservar a timeline evolutiva e servir de referência histórica do baseline heurístico do projeto. Não deve ser invocado em código novo.

## Correções da Auditoria (Iteração 2)

### Correção 1: Adição de `delta_price_ticks` no Modelo (`models.py`)
- **Ação:** Adicionado o atributo `delta_price_ticks: int = 0` na dataclass `LiquidityCluster`.
- **Motivo:** Solucionar a falha semântica do `LAG()` no Profiler. Em vez de deduzir o delta a partir de consultas SQL lentas e imprecisas, o delta exato calculado em O(1) pelo Ingestion Service será transportado no próprio objeto de cluster e persistido nativamente.

### Correção 2: Viés de Seleção no SQL (`signature_profiler.py`)
- **Ação:** O `JOIN` com a tabela `tape_events` foi alterado para `LEFT JOIN`, e o cálculo do pandas passou a usar `fillna(df['c_price'])`.
- **Motivo:** O INNER JOIN silenciosamente descartava clusters do final da sessão (que não tinham eventos subsequentes na janela de 30 minutos). O `LEFT JOIN` com fillna preenche "deslocamento zero" para essas ocasiões, removendo o viés de otimismo matemático.

### Correção 3: Ruído Estatístico em Amostras Pequenas (`signature_profiler.py`)
- **Ação:** Inserida trava `MIN_SAMPLES_FOR_PERCENTILES = 100` antes de calcular percentis empíricos por sessão. Se a amostra não atingir o limite, o profiler faz um bypass inserindo thresholds predeterminados via fallback estático.
- **Motivo:** Evitar que distribuições não-representativas (ex: OFF_HOURS com apenas 10 eventos) gerem percentis p90/p95 extremamente sensíveis a outliers únicos, bagunçando a inferência online subsequente.

### Correção 4: Robustez no Logging e Exceptions (`signature_profiler.py`)
- **Ação:** O módulo instanciou formalmente um `logger = logging.getLogger(__name__)`. No método `build_profile()`, falhas do DuckDB não mais retornam um ditado mudo de erro (`return {"error": ...}`), mas registram a falha no logger e invocam um explícito `raise`.
- **Motivo:** O antigo comportamento "engolia" exceções. Num pipeline produtivo, falhas de DB ou query mal formada devem ser capturadas pelo runtime e escalar imediatamente.

### Melhoria de Engenharia 1: Validação de SQL contra Injection (`signature_profiler.py`)
- **Ação:** Inserida trava de validação (`isinstance(horizon_minutes, int)`) e formatado com limite máximo de 1440 minutos. A query utiliza variável local formatada invés da interpolação direta na string multi-line.
- **Motivo:** Boas práticas de segurança em montagem de strings SQL no Python, blindando o DuckDB contra vetores f-string em cláusulas `INTERVAL`.

### Melhoria de Engenharia 2: Documentação de Limitação de Memória (`signature_profiler.py`)
- **Ação:** Documentado na docstring de `build_profile` o risco inerente do uso atual de `.fetchdf()`.
- **Motivo:** O `.fetchdf()` carrega todos os milhões de linhas num DataFrame Pandas em RAM. Para um símbolo e um horizonte de 30 dias de *tick data* (condição nominal), é a forma mais ágil de processamento. Contudo, em projeções de _Big Data_ futuras, o motor não escalará horizontalmente para anos de dados sem OOM (Out-of-Memory). A alternativa para o futuro (chunks via `.fetchmany()` + streaming percentiles/T-Digest) agora está registrada na documentação para não pegar os desenvolvedores de surpresa.

