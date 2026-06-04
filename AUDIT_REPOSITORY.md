# Auditoria e Refatoração: DuckDB Repository

Este documento registra como um Architecture Decision Record (ADR) as diretrizes, implementações corretas e futuras otimizações aplicadas na camada de banco de dados e persistência `repository_duckdb.py`. O foco é assegurar I/O de altíssima performance num banco colunar atrelado a um pipeline de alta frequência.

## 🏆 Fundações Consolidadas e Validadas

O core do repositório já se encontra ancorado em três pilares fundamentais, validados pela auditoria:

### 1. Transações ACID Sincronizadas
- **Status:** Perfeito.
- **Implementação:** Os blocos de conexão nativa expõem `begin()`, `commit()` e `rollback()`.
- **Justificativa:** Isso garante o repasse atômico de responsabilidade. O motor de persistência jamais grava dados parciais. Ele trabalha em uníssono perfeito com o pipeline transacional em memória do `ingestion.py`.

### 2. Bulk Inserts de Alta Performance (executemany)
- **Status:** Perfeito.
- **Implementação:** Todos os métodos de ingestão (`insert_tape`, `insert_dom_snapshots`, `insert_clusters`) utilizam exclusamente `executemany` combinados com list comprehensions eficientes.
- **Justificativa:** Em um sistema que recebe centenas ou milhares de ticks simultâneos via WebSockets ou requisições HTTP, emitir N comandos `INSERT` geraria saturação de disco. A consolidação em Bulk Operations resolve isso aproveitando a arquitetura colunar analítica do DuckDB ao máximo.

### 3. Schema Completo e Semântico
- **Status:** Perfeito.
- **Implementação:** As tabelas lógicas mapeiam as instâncias do sistema de forma bidirecional (Tape, DOM, Clusters, Key Levels).
- **Justificativa:** Permite varreduras analíticas em background e recuperação integral do estado sistêmico sem corromper as abstrações de dados nativas de cada tipo de evento.

## Otimizações Aplicadas (Auditoria Ativa)

### 1. Atomicidade no Upsert de Key Levels
- **Problema:** O método `upsert_key_level` executava duas operações não-atômicas sequenciais (`DELETE` seguido de `INSERT`), o que abria brechas para condições de corrida (Race Conditions) e gerava overhead no banco ao forçar duas passagens distintas pela árvore de índices.
- **Solução:** A tabela `key_levels` (em `_init_schema`) recebeu a restrição formal `PRIMARY KEY (symbol, price)`. A dupla query foi extirpada e substituída por uma operação transacional O(1) unificada através de `INSERT INTO ... ON CONFLICT DO UPDATE SET`, encapsulando a mutação num único bloco atômico no engine DuckDB.

### 2. Remoção de Artefatos Incompatíveis (Warm-up)
- **Problema:** O método `_init_schema` executava uma query nula `self.conn.executemany("", [])` com o intuito de fazer "warm-up" na conexão. Esse é um padrão legado (comum em pools antigos de banco de dados) que o motor colunar do DuckDB não exige. O uso de queries vazias poderia causar exceções em versões futuras ou comportamentos imprevisíveis.
- **Solução:** A linha inútil foi completamente removida. O DuckDB inicializa a base e a conexão no momento do `.connect()` e processa os `CREATE TABLE` imediatamente sem necessidade de pré-aquecimento.

### 3. Precisão Estatística em `recurring_levels` (ANY_VALUE vs MODE)
- **Problema:** A query `recurring_levels` usava `ANY_VALUE(behavior_signature) dominant` para tentar extrair a assinatura de comportamento principal de um nível de preço. No entanto, `ANY_VALUE` é não-determinístico e retorna uma assinatura aleatória do grupo, gerando falsos positivos na identificação de perfis institucionais.
- **Solução:** A query foi corrigida para utilizar a função agregadora estatística `MODE(behavior_signature)`. O DuckDB suporta o `MODE()` nativamente, o que garante matematicamente o retorno da assinatura que apareceu com a maior frequência (moda estatística) naquele bloco de preço, mantendo a performance da query sem a necessidade de CTEs complexas.

### 4. Ajuste de Consistência e Design de Schema (`batch_id`)
- **Decisão Arquitetural:** O `batch_id` (introduzido no `models.py` para resolver ambiguidades e isolamento no loop do `ingestion.py`) não foi incluído no schema do banco de dados (tabela `liquidity_clusters`).
- **Justificativa:** Essa ausência é estritamente proposital e reflete um bom design de dados. O `batch_id` é um identificador *transiente* e volátil (usado apenas em RAM para tracking de pipeline e rollbacks). Persisti-lo no DuckDB consumiria espaço em disco sem agregar nenhum valor analítico de longo prazo. A assimetria Modelo-Schema neste ponto é correta.

## Correções da Auditoria (Iteração 2)

### 1. Atualização do Schema (`liquidity_clusters`)
- **Ação:** Adicionado o campo `delta_price_ticks INTEGER` no `CREATE TABLE` e atualizada a query `INSERT INTO` (de 13 para 14 parâmetros).
- **Motivo:** Persistir em disco o deslocamento de preço real medido em memória (O(1)) durante a ingestão do cluster, abolindo de vez a necessidade de recorrer à ineficiente e falha função `LAG()` no SQL analítico do Profiler. A mudança consolida o banco como um *true sink* determinístico da pipeline online.
