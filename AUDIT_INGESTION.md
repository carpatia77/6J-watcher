# Auditoria e Refatoração: Ingestion Pipeline

Este documento registra cronologicamente todas as correções arquiteturais e de performance aplicadas no módulo `ingestion.py` e suas dependências diretas (`liquidity_matrix.py` e `models.py`) durante a auditoria técnica. 

O objetivo principal foi garantir consistência ACID (banco vs memória), prevenir vazamentos de memória e resolver ambiguidades de dados.

## Histórico de Modificações

### 1. Prevenção de Condições de Corrida e I/O Bloqueante
- **Problema:** O servidor HTTP em Python processava requisições do MQL5 de forma síncrona e modificava dicionários iterados simultaneamente pela thread principal.
- **Solução:** O servidor base em `main.py` foi migrado para `ThreadingHTTPServer`. Introduziu-se um `threading.RLock()` na `LiquidityMatrix` para serializar operações de leitura e escrita.

### 2. Otimização de Transações e Prevenção de OOM
- **Problema:** Múltiplas inserções ao DuckDB não transacionais degradavam o disco, e a matriz crescia indefinidamente em memória, causando Out-Of-Memory (OOM).
- **Solução:**
  - `repository_duckdb.py`: Foram implementados `begin()`, `commit()` e `rollback()`.
  - `ingestion.py`: Todo o lote (Tape, DOM, Clusters) passou a ser inserido via Bulk Transactions ACID.
  - `liquidity_matrix.py`: Introduzido o método `prune_stale_data(hours=4)` chamado pelo loop principal, evitando vazamentos e uso desnecessário de RAM.

### 3. Remoção de Classificação Redundante
- **Problema:** O `PatternEngine` classificava os mesmos clusters até 3 vezes repetidamente (na matrix, no loop, e no post-classify).
- **Solução:** O parâmetro `classify` foi removido da chamada principal da `LiquidityMatrix`. A classificação individual primária foi centralizada unicamente no loop gerador do batch.

### 4. Unificação da "Single Source of Truth" (Clusters)
- **Problema:** A `LiquidityMatrix` criava seus próprios clusters isolados (sem variáveis contextuais como `session`), enquanto o `ingestion.py` criava outra lista independente para o banco de dados. A memória e o Disco armazenavam objetos de fato divergentes.
- **Solução:** O `ingestion.py` assumiu o papel de construtor universal. Agora ele cria os clusters uma única vez, envia para o DuckDB e injeta os mesmos objetos já prontos no `build_from_events`.

### 5. Inversão da Ordem Transacional (DB antes da Memória)
- **Problema:** Os *hotspots* eram calculados e a matriz preenchida *antes* da gravação no DuckDB. Se ocorresse uma falha no disco, a memória ficava permanentemente corrompida com dados "fantasmas" que não existiam no banco.
- **Solução:** O bloco de transação (`begin`/`insert`/`commit`) foi movido para as primeiras linhas. Somente dados garantidos no disco (pós-commit) são repassados para a alimentação em memória e para os motores de processamento de hotspots.

### 6. Mecanismo de Snapshot/Restore em Memória
- **Problema:** Se os métodos `build_from_events` ou `post_classify` falhassem pela metade, a `LiquidityMatrix` ficaria dessincronizada do BD para sempre, pois o block `except` fazia o rollback apenas do banco DuckDB.
- **Solução:** Implementou-se um controle transacional na RAM via Truncamento. Antes de tocar nos dados, o ingestion pede `snap = self.matrix.snapshot()` (que faz cache apenas do `len()` de todas as listas no estado atual). Em caso de crash, o `except` roda um `self.matrix.restore(snap)`, que apara as listas de volta aos comprimentos originais (sem gastar memória com `deepcopy()`).

### 7. Post-Classify Seguro Contra Sobrescrita
- **Problema:** A rotina de re-classificação de hotspots iterava cegamente sobre os `active_levels`, sobrescrevendo indiscriminadamente as assinaturas de clusters criados horas atrás e que já estavam confirmados no DuckDB.
- **Solução:** Adicionado filtro para assegurar que apenas os clusters criados milissegundos antes, estritamente no batch atual, fossem afetados pelo refinamento do `post_classify`.

### 8. Robustez no Tracking de Identidade (Injeção de `batch_id`)
- **Problema:** Utilizar o ID de memória do Python (`id()`) para fazer o filtro do item #7 era uma quebra de modelo perigosa: não sobreviveria a cópias profundas se um desenvolvedor alterasse a matrix futuramente.
- **Solução:** A classe `LiquidityCluster` (`models.py`) recebeu o campo `batch_id: str`. Um timestamp exato do relógio via `time.time_ns()` é gerado *fora do laço* de leitura no `ingestion.py` e carimbado em todo o lote de clusters simultaneamente.

### 9. Validação Lógica de Payloads
- **Problema:** Se o JSON oriundo do MQL5 estivesse avariado e os dicionários fossem mal parseados, o parser retornava listas vazias sem estourar exceções. O pipeline rodava silenciosamente, mascarando dados truncados ou conexões de rede instáveis.
- **Solução:** Foram fixados warnings de logs explícitos. Se `tape_rows` e `dom_rows` não forem nulos mas os parsers devolverem 0 registros, logs como `"payload pode estar malformado"` ou `"DOM sensor pode estar offline"` são gerados para ajudar em trouble-shooting no servidor.

### 10. Transferência do `delta_price_ticks` para o Objeto e Persistência O(1)
- **Problema:** O classificador O(1) exigia o `delta_price_ticks`, e o cálculo do stateful cursor em memória funcionava bem, mas esse valor vital era descartado da persistência. Isso forçava o DuckDB a tentar recalcular o delta depois usando a função analítica `LAG()`, o que era semântica e tecnicamente falho (gerava deltas incorretos baseados no registro e não no preço do bucket).
- **Solução:** O valor exato `dp`, medido via stateful cursor em memória na construção do `LiquidityCluster`, foi roteado diretamente para o atributo interno `c.delta_price_ticks` do modelo, tornando-o disponível para persistência direta no DuckDB sem cálculos complexos *offline*.
