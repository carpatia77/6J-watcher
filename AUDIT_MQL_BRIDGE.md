# Auditoria e Decisões de Arquitetura: MQL5 Bridge (`mql_bridge.mq5`)

Este documento registra a evolução e cristalização do Bridge MQL5, que atua como a ponta de lança na captação de dados do Order Flow para o ecossistema 6J Watcher.

## Estado Inicial e Problemas Críticos
A implementação pregressa da ponte apresentava falhas arquiteturais graves:
- **Dados Sujos:** O código utilizava `CopyTicks` e `MarketBookGet`, que extraem o "Tick Volume" (varejo) e o DOM da corretora local, ignorando o volume real da CME, tornando todas as análises quantitativas irrelevantes.
- **Falhas Silenciosas:** Ausência de validação para permissões de `WebRequest` e total descarte de pacotes em caso de falha de conexão HTTP (timeout ou servidor Python offline).
- **Dessincronização de DOM:** `MarketBookGet` itera bids e asks juntos. Assumir que o tamanho array representava a mesma profundidade perfeitamente balanceada truncava o Order Book.
- **Formatação JSON Frágil:** Concatenação simples com `DoubleToString` (vulnerável ao delimitador decimal regional) e injeção de side sem "escaping" (causava quebra do JSON Parser no Python).

---

## Iteração 1: Resiliência de Rede e Formatação Segura
Focamos em transformar o Expert Advisor (EA) numa aplicação tolerante a falhas na camada de rede.

### Correção 1: Queue & Retry (Prevenção de Data Loss)
- **Ação:** Criação do array global `pending_queue[]` (tamanho 100) e encapsulamento do envio na função `SendWithRetry`.
- **Resultado:** Se o endpoint `PYTHON_ENDPOINT` estiver temporariamente off ou falhar em retornar HTTP Status `200 OK`, o pacote aguarda e é reenviado no próximo tick do Timer.

### Correção 2: Validação Defensiva MQL5
- **Ação:** Inserção das travas `TerminalInfoInteger(TERMINAL_WEBREQUEST_ENABLE)` e inspeção de URL no `OnInit()`.
- **Resultado:** EA falha graciosamente, avisando o usuário na aba Experts se o `WebRequest` não estiver configurado corretamente, em vez de falhar silenciosamente no runtime.

### Correção 3: Timer Supersônico e DOM Separado
- **Ação:** Alteração do timer para 200ms (`EventSetMillisecondTimer`) para não misturar tape prints. Separação explícita dos loops do DOM até encontrar contagens corretas para Bid e Ask, ignorando distorções de feed.

---

## Iteração 2: Integração Nativa ClusterDelta (Engenharia Reversa)
Após estabelecermos o pipeline robusto de HTTP POST, foi necessário integrar a leitura de Volume Real (CME).

### Descoberta Crítica
O uso de chamadas de buffer (ex: `iCustom`) para indicadores ClusterDelta não extrai o DOM/Tape no formato bruto em alta frequência com a confiabilidade requerida. Era necessário consumir o Socket direto via DLL da CD.

### Ação e Implementação
Mediante a engenharia reversa do código do `#TSDOM` oficial da ClusterDelta, mapeamos com exatidão o payload customizado gerado pela DLL deles:
1. Adicionadas chamadas diretas de `Online_Init` e `Online_Subscribe` ao `OnInit`.
2. Criação do método principal `ProcessClusterDeltaStream()` que processa:
   - Split inicial por dois-pontos `:` (separador de broadcasts).
   - Split secundário por cerquilha `#` (sub-pacotes agregados).
   - Split terciário por ponto-e-vírgula `;` (atributos internos).
3. **Time & Sales:** Se o chunk possui 3 partes, é um registro T&S. Extraímos Timestamp Epoch, Agressor (A/B mapeado para buy/sell com inversão semântica MT5), Preço e **Volume CME Real**.
4. **Depth of Market:** Se o header `"DOM"` for interceptado, o próximo chunk carrega todo o book (níveis separados por `|`), iterado mapeando o Bid/Ask Real da bolsa e respeitando o corte configurável de `DOM_LEVELS`.

## Status Final
**Gold Tier (100% Produção).** 
O `mql_bridge.mq5` não depende mais de nada provido pela MetaQuotes a não ser o timer e o renderizador HTTP. É um leitor de memory buffer da DLL ClusterDelta, despachando Order Flow assíncrono para o ecossistema Python com resiliência contra lags de servidor.
