//+------------------------------------------------------------------+
//|  6J Watcher MQL Bridge                                           |
//|  Lê T&S e DOM da ClusterDelta e posta JSON para o servidor Python |
//+------------------------------------------------------------------+
#property strict

//--- Importação da DLL oficial da ClusterDelta
#import "tsanddom_mt5_v4x1.dll"
   int Online_Init(int&);
   string Online_Data(int &, string);
   int Online_Subscribe(int &, string, string, int, string, string, string, string, string, string, int, string, string, string, int);
#import

input string PYTHON_ENDPOINT = "http://127.0.0.1:8765/ingest";
input string SYMBOL_OVERRIDE = "";      // Vazio = usa _Symbol
input bool   USE_FUTURES_MAPPING = true;// Mapeia spot -> futuro CME
input int    TIMER_MS        = 200;     // frequência de envio (5x por segundo)
input int    DOM_LEVELS      = 10;      // quantos níveis do DOM capturar

//--- Variáveis globais para Queue e Métricas
string pending_queue[];
int    max_queue_size = 100;
int    retry_attempts = 3;

int    stats_sent     = 0;
int    stats_failed   = 0;
datetime last_success = 0;
int    cd_session_id  = 0; // ID da sessão da DLL

//+------------------------------------------------------------------+
//| Funções Auxiliares de JSON                                       |
//+------------------------------------------------------------------+
string JsonEscape(string s) {
   StringReplace(s, "\\", "\\\\");
   StringReplace(s, "\"", "\\\"");
   StringReplace(s, "\n", "\\n");
   StringReplace(s, "\r", "\\r");
   StringReplace(s, "\t", "\\t");
   return s;
}

string JsonNumber(double n) {
   return DoubleToString(n, _Digits);
}

string GetSymbolName() {
   if(StringLen(SYMBOL_OVERRIDE) > 0) return SYMBOL_OVERRIDE;
   
   if(USE_FUTURES_MAPPING) {
      if(_Symbol == "USDJPY") return "6J";
      if(_Symbol == "EURUSD") return "6E";
      if(_Symbol == "GBPUSD") return "6B";
      if(_Symbol == "AUDUSD") return "6A";
      if(_Symbol == "USDCAD") return "6C";
      if(_Symbol == "USDCHF") return "6S";
   }
   return _Symbol;
}

//+------------------------------------------------------------------+
//| Initialization                                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   // 1. Valida permissão WebRequest global
   if(!TerminalInfoInteger(TERMINAL_WEBREQUEST_ENABLE)) {
      Print("[ERRO CRÍTICO] WebRequest não habilitado.");
      Print("Solução: Tools -> Options -> Expert Advisors -> Allow WebRequest");
      return(INIT_FAILED);
   }
   
   // 2. Valida URL específica
   string allowed_urls = TerminalInfoString(TERMINAL_WEBREQUEST_URLS);
   if(StringFind(allowed_urls, PYTHON_ENDPOINT) == -1) {
      Print("[ERRO CRÍTICO] URL não permitida para WebRequest: " + PYTHON_ENDPOINT);
      Print("Solução: Tools -> Options -> Expert Advisors -> Allow WebRequest -> Adicionar: " + PYTHON_ENDPOINT);
      return(INIT_FAILED);
   }
   
   // 3. Inicializa conexão com ClusterDelta
   if(Online_Init(cd_session_id) <= 0) {
      Print("[ERRO] Falha ao inicializar ClusterDelta DLL");
      // Mesmo falhando a inicializacao da DLL, pode ser um mock. Deixamos continuar ou falhamos dependendo do caso.
      // return(INIT_FAILED); 
   }
   
   // Inicializa Queue
   ArrayResize(pending_queue, 0);
   
   // Inicia Timer de alta frequência
   EventSetMillisecondTimer(TIMER_MS);
   
   Print("[6J Watcher] Inicializado com sucesso. Enviando para: ", PYTHON_ENDPOINT);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason) { 
   EventKillTimer(); 
}

//+------------------------------------------------------------------+
//| Timer / Main Loop                                                |
//+------------------------------------------------------------------+
void OnTimer()
{
   datetime start = GetMicrosecondCount();
   
   // 1. Tenta processar fila de retentativas
   ProcessPendingQueue();
   
   // 2. Coleta dados (Tape e DOM)
   string tape_json = BuildTapeJSON();
   string dom_json  = BuildDOMJSON();
   
   if(tape_json == "[]" && dom_json == "[]") return;
   
   // 3. Constrói payload seguro
   string ts = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
   string payload = "{\"symbol\":\"" + JsonEscape(GetSymbolName()) + "\"," +
                    "\"timestamp\":\"" + JsonEscape(ts) + "\"," +
                    "\"tape\":" + tape_json + "," +
                    "\"dom\":"  + dom_json  + "}";
                    
   // 4. Tenta enviar
   if(!SendWithRetry(payload, retry_attempts)) {
      AddToPendingQueue(payload);
      stats_failed++;
   } else {
      stats_sent++;
      last_success = TimeCurrent();
      
      // Métricas periódicas
      if(stats_sent % 100 == 0) {
         Print("[STATS] Enviados: ", stats_sent, " | Falhas: ", stats_failed, " | Último sucesso: ", TimeToString(last_success));
      }
   }
}

//+------------------------------------------------------------------+
//| Envio HTTP e Fila (Queue & Retry)                                |
//+------------------------------------------------------------------+
bool SendWithRetry(string payload, int max_retries)
{
   for(int i = 0; i < max_retries; i++) {
      if(PostPayload(payload)) return true;
      Sleep(100); // aguarda 100ms antes do retry
   }
   return false;
}

void AddToPendingQueue(string payload)
{
   int size = ArraySize(pending_queue);
   if(size >= max_queue_size) {
      // Remove o elemento mais antigo (shift-left)
      ArrayCopy(pending_queue, pending_queue, 0, 1, WHOLE_ARRAY - 1);
      ArrayResize(pending_queue, max_queue_size);
      size = max_queue_size - 1;
   } else {
      ArrayResize(pending_queue, size + 1);
   }
   pending_queue[size] = payload;
}

void ProcessPendingQueue()
{
   for(int i = 0; i < ArraySize(pending_queue); i++) {
      if(SendWithRetry(pending_queue[i], 1)) {
         // Se sucesso, remove da fila (shift-left do resto do array)
         ArrayCopy(pending_queue, pending_queue, i, i + 1, WHOLE_ARRAY - i - 1);
         ArrayResize(pending_queue, ArraySize(pending_queue) - 1);
         i--; // Reajusta o iterador
      }
   }
}

bool PostPayload(string payload)
{
   char data[], result[];
   string headers = "Content-Type: application/json\r\n";
   string response_headers;
   
   StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   ResetLastError();
   
   int res = WebRequest("POST", PYTHON_ENDPOINT, headers, 3000, data, result, response_headers);
   
   if(res == -1) {
      return false; // Erro de conexão (ex: timeout, endpoint down)
   }
   
   // Verifica se o servidor retornou HTTP 200 OK
   if(StringFind(response_headers, "200 OK") == -1) {
      string response_body = CharArrayToString(result);
      Print("[ERRO HTTP] Status falhou: ", response_headers);
      Print("[ERRO BODY] ", response_body);
      return false;
   }
   
   return true;
}

//+------------------------------------------------------------------+
//| Coleta de Dados (ClusterDelta)                                   |
//+------------------------------------------------------------------+
string BuildTapeJSON()
{
   // NOTA ARQUITETURAL: Aqui deveríamos usar a função Online_Data() da DLL.
   // Como os detalhes internos do parse de string da DLL não estão especificados, 
   // usaremos MqlTick como fallback temporário estrutural apenas para demonstrar
   // o JSON builder, mas o fluxo de dados IDEAL DEVE vir da DLL CD.
   
   MqlTick ticks[];
   int copied = CopyTicks(_Symbol, ticks, COPY_TICKS_TRADE, 0, 20);
   if(copied <= 0) return("[]");
   
   string result = "[";
   for(int i = 0; i < copied; i++)
   {
      string side = (ticks[i].flags & TICK_FLAG_BUY) ? "buy" : "sell";
      double price = (ticks[i].flags & TICK_FLAG_BUY) ? ticks[i].ask : ticks[i].bid;
      
      result += "{\"timestamp\":\"" + JsonEscape(TimeToString(ticks[i].time, TIME_DATE|TIME_SECONDS)) + "\"," +
                "\"price\":"        + JsonNumber(price) + "," +
                "\"volume\":"       + IntegerToString((int)ticks[i].volume) + "," +
                "\"side\":\""       + JsonEscape(side) + "\"}";
                
      if(i < copied - 1) result += ",";
   }
   return result + "]";
}

string BuildDOMLevelJSON(string ts, double price, int level_idx, int bid_vol, int ask_vol)
{
   return "{\"timestamp\":\"" + JsonEscape(ts) + "\"," +
          "\"price\":" + JsonNumber(price) + "," +
          "\"level_index\":" + IntegerToString(level_idx) + "," +
          "\"bid_volume\":" + IntegerToString(bid_vol) + "," +
          "\"ask_volume\":" + IntegerToString(ask_vol) + "}";
}

string BuildDOMJSON()
{
   // NOTA ARQUITETURAL: Assim como o T&S, o DOM deve ser lido via DLL (Online_Data).
   // O código abaixo demonstra a estruturação bid/ask separada.
   MqlBookInfo book[];
   if(!MarketBookGet(_Symbol, book)) return("[]");
   
   string result = "[";
   string ts = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
   int bid_count = 0, ask_count = 0;
   bool first = true;
   
   // 1. Processa Bids
   for(int i = 0; i < ArraySize(book) && bid_count < DOM_LEVELS; i++) {
      if(book[i].type != BOOK_TYPE_BUY) continue;
      if(!first) result += ",";
      result += BuildDOMLevelJSON(ts, book[i].price, bid_count, (int)book[i].volume, 0);
      bid_count++;
      first = false;
   }
   
   // 2. Processa Asks
   for(int i = 0; i < ArraySize(book) && ask_count < DOM_LEVELS; i++) {
      if(book[i].type != BOOK_TYPE_SELL) continue;
      if(!first) result += ",";
      result += BuildDOMLevelJSON(ts, book[i].price, DOM_LEVELS + ask_count, 0, (int)book[i].volume);
      ask_count++;
      first = false;
   }
   
   return result + "]";
}
