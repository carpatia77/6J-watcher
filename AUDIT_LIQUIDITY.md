# Auditoria e Refatoração: Liquidity Matrix

Este documento registra como um Architecture Decision Record (ADR) as decisões e correções aplicadas ao arquivo base de estado do sistema, `liquidity_matrix.py`. O objetivo destas modificações foi garantir que a matriz suporte alta concorrência multithread sem corrupção, evite vazamentos de memória em tempo de execução contínua e mantenha paridade atômica com o armazenamento persistente.

## Histórico de Modificações

### 1. Prevenção de Data Races (Thread Safety)
- **Problema:** A `LiquidityMatrix` era instanciada no processo principal, mas os endpoints HTTP iteravam sobre dicionários que podiam ter seu tamanho alterado (`RuntimeError: dictionary changed size during iteration`) pelo fluxo de background recebendo rajadas de dados.
- **Solução:** Introdução de um objeto `threading.RLock()`. Todo e qualquer acesso de mutação (`ingest_cluster`, `ingest_tape`, `ingest_dom`) e leitura analítica (`get_price_matrix`, `hotspots`) agora é isolado transacionalmente por blocos de contexto `with self.lock:`.

### 2. Prevenção de Memory Leaks e OOM (Out Of Memory)
- **Problema:** As coleções `matrix`, `dom_snapshots` e `tape_index` acumulavam chaves temporais indefinidamente. Se o sistema operasse em nuvem ou máquina local ininterruptamente, causaria esgotamento de memória RAM.
- **Solução:** Implementação do método `prune_stale_data(hours: int = 4)`. Chamado periodicamente pelo `main.py`, ele faz varreduras atômicas sob lock, calculando deltas de tempo. Tudo o que ultrapassa o _cutoff_ configurado (ex: 4 horas passadas) é truncado e deletado, reconstruindo `active_levels` na sequência.

### 3. Garantia de Compatibilidade com Scripts de Backtest
- **Problema:** O fluxo unificado do `ingestion.py` tornou a criação de clusters uma exclusividade dele. Se o pipeline chamasse `build_from_events` passando só `tape_events` cruamente, nenhum cluster era gerado na memória (quebra de retrocompatibilidade para eventuais bots avulsos de backtest).
- **Solução:** A assinatura foi expandida de volta aceitando o parâmetro de fábrica genérica (`classify: Optional[Callable] = None`), e incluiu-se um bloco explícito de `fallback` (um `else:` caso a flag `clusters` falte), onde a matriz auto-orquestra a instanciação básica para garantir resiliência modular.

### 4. Transações ACID em Memória (Snapshot / Restore)
- **Problema:** Transações ao banco de dados usavam `rollback()` ao falhar, porém o estado do Python já havia alterado os dados em memória. Isso causava a pior das inconsistências para sistemas de trade analíticos (RAM dessincronizada do Disco).
- **Solução:** Implementação de `snapshot()` e `restore()` hiper-otimizados.
  - Ao invés de onerar a CPU com `copy.deepcopy()`, o snapshot mapeia de forma super leviana a métrica nativa das coleções (comprimento/tamanho dos arrays). 
  - Em caso de falha, o `restore(snap)` trunca as listas da matriz com cortes instantâneos em C (`array[:len]`), deletando apenas deltas recentes e mantendo um rollback transacional que opera em fração de microssegundos.

### 5. Correções Literais de Indentação (SyntaxError)
- **Problema:** Um erro clássico em blocos `for` provocava interrupção letal (Crash/IndentationError) porque lógicas de filtragem em `hotspots()` estavam soltas na hierarquia Python.
- **Solução:** Reposicionamento arquitetural do bloco (indentando-se para o subnível correto no contexto transacional). O erro silencioso foi removido.

### 6. Otimização de Performance O(1) no Pruning de Dados
- **Problema:** O método `prune_stale_data` realizava o parse reverso das chaves de tempo (`t`) convertendo milhares de strings para objetos `datetime` via `strptime` a cada 30 segundos, causando desperdício excessivo de CPU.
- **Solução:** A conversão foi invertida. O limite cronológico (`cutoff_ts`) agora é pré-formatado em uma única string (`cutoff_str`). A verificação dentro dos loops iterativos passou a usar uma simples comparação lexicográfica de strings (`if t < cutoff_str:`), reduzindo uma operação custosa de parsing para uma operação O(1) atômica do CPython.

### 7. Refatoração In-Place O(N) para `active_levels`
- **Problema:** Após expurgar as chaves de tempo antigas, a matriz usava `self.active_levels.clear()` e iterava sob a estrutura recursiva completa para readicionar os clusters sobreviventes, causando um overhead logístico em O(N*M).
- **Solução:** O algoritmo foi otimizado para atuar in-place (direto na memória). Ao invés de reconstruir do zero através das hierarquias antigas, o código passa as listas de `active_levels` em um *List Comprehension* filtrando-as nativamente pela condição `c.timestamp >= cutoff_ts`. O(N).
