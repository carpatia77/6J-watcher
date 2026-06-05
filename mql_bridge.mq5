//+------------------------------------------------------------------+
//|  6J Watcher MQL Bridge                                           |
//|  Lê T&S e DOM institucional da ClusterDelta via DLL e            |
//|  posta JSON formatado para o servidor Python.                    |
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
input bool   DEBUG_PARSE_ERRORS = false;// Habilitar para logar formatos inesperados

//--- Variáveis globais
string pending_queue[];
int    max_queue_size = 100;
int    retry_attempts = 3;

int    stats_sent     = 0;
int    stats_failed   = 0;
datetime last_success = 0;
datetime last_dll_check = 0; // Para health check proativo

int    cd_session_id  = 0; // ID da sessão da DLL
string clusterdelta_client = ""; // Gerado no OnInit

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

string JoinJsonArray(const string &arr[], int count) {
   if(count == 0) return "[]";
   string res = "[";
   for(int i = 0; i < count; i++) {
      res += arr[i];
      if(i < count - 1) res += ",";
   }
   return res + "]";
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
   if(!TerminalInfoInteger(TERMINAL_WEBREQUEST_ENABLE)) {
      Print("[ERRO CRÍTICO] WebRequest não habilitado.");
      return(INIT_FAILED);
   }
   
   string allowed_urls = TerminalInfoString(TERMINAL_WEBREQUEST_URLS);
   if(StringFind(allowed_urls, PYTHON_ENDPOINT) == -1) {
      Print("[ERRO CRÍTICO] URL não permitida para WebRequest: " + PYTHON_ENDPOINT);
      return(INIT_FAILED);
   }
   
   // Inicializa conexão com ClusterDelta
   if(Online_Init(cd_session_id) <= 0) {
      Print("[ERRO] Falha ao inicializar ClusterDelta DLL");
   }
   
   // Gera client ID com alta entropia para evitar colisão entre terminais
   clusterdelta_client = "CDPT" + StringSubstr(IntegerToString(TimeLocal()),7,3) + IntegerToString(GetTickCount() % 1000) + IntegerToString(MathAbs(MathRand() % 1000));
   string cmt = AccountInfoString(ACCOUNT_COMPANY);
   int acnt = (int)AccountInfoInteger(ACCOUNT_LOGIN);
   
   int sub = Online_Subscribe(cd_session_id, clusterdelta_client, Symbol(), Period(), 
                              TimeToString(TimeCurrent()), TimeToString(TimeCurrent()), 
                              GetSymbolName(), TimeToString(0), "0", "5.63", 0, 
                              TimeToString(D'2017.01.01 00:00'), TimeToString(D'2017.01.01 00:00'), 
                              cmt, acnt);
   
   ArrayResize(pending_queue, 0);
   EventSetMillisecondTimer(TIMER_MS);
   
   Print("[6J Watcher] Ponte ClusterDelta->Python inicializada. Enviando para: ", PYTHON_ENDPOINT);
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
   ProcessPendingQueue();
   ProcessClusterDeltaStream();
   
   // Health check proativo da conexão com a DLL
   if(TimeCurrent() - last_dll_check > 60) {
      int dummy = 0;
      string test = Online_Data(dummy, clusterdelta_client);
      if(dummy == 0 && StringLen(test) < 5) {
         Print("[ALERTA] ClusterDelta DLL pode estar offline ou sem dados. Tentando reconexão completa...");
         Online_Init(cd_session_id);
         // Re-subscrever é obrigatório — sem isso, Online_Data retorna vazio indefinidamente
         string cmt = AccountInfoString(ACCOUNT_COMPANY);
         int acnt = (int)AccountInfoInteger(ACCOUNT_LOGIN);
         Online_Subscribe(cd_session_id, clusterdelta_client, Symbol(), Period(),
                          TimeToString(TimeCurrent()), TimeToString(TimeCurrent()),
                          GetSymbolName(), TimeToString(0), "0", "5.63", 0,
                          TimeToString(D'2017.01.01 00:00'), TimeToString(D'2017.01.01 00:00'),
                          cmt, acnt);
         Print("[ALERTA] Reconexão completa executada (Init + Subscribe).");
      }
      last_dll_check = TimeCurrent();
   }
}

//+------------------------------------------------------------------+
//| Parser do Stream da ClusterDelta (Engenharia Reversa)            |
//+------------------------------------------------------------------+
void ProcessClusterDeltaStream()
{
   int length = 0;
   string stream = Online_Data(length, clusterdelta_client);
   
   if(length == 0 || StringLen(stream) < 10) return;
   
   string allpackets[], packet[], internal[], domdata[];
   int all = StringSplit(stream, ':', allpackets);
   if(all == 0 || allpackets[0] != clusterdelta_client) return;
   
   string tape_parts[];
   ArrayResize(tape_parts, 500);
   int tape_count = 0;
   
   string dom_parts[];
   ArrayResize(dom_parts, 200);
   int dom_count = 0;
   
   int DOM_saved = 0;
   
   for(int l = 1; l < all; l++) {
      int num_packets = StringSplit(allpackets[l], '#', packet);
      
      for(int i = 0; i < num_packets; i++) {
         if(packet[i] == ":" || StringLen(packet[i]) < 3) continue;
         
         int ts = StringSplit(packet[i], ';', internal);
         
         if(packet[i] == "DOM") { 
             DOM_saved = 1; 
             continue; 
         }
         
         // INVARIANTE: DOM_saved==1 significa que o packet anterior era literalmente "DOM".
         // O flag é SEMPRE resetado para 0 ao final deste bloco, garantindo que
         // mesmo dois blocos DOM consecutivos sem tape entre eles sejam processados corretamente.
         if(DOM_saved == 1) {
            for(int k = 0; k < ts; k++) {
               if(StringSplit(internal[k], '|', domdata) >= 2) {
                  bool is_ask = (StringSubstr(domdata[0], 0, 1) == "A");
                  int index = (int)StringToInteger(StringSubstr(domdata[0], 1, 2));
                  double price = StringToDouble(StringSubstr(domdata[0], 3));
                  int vol = (int)StringToInteger(domdata[1]);
                  
                  if(index < DOM_LEVELS) {
                     int bid_vol = is_ask ? 0 : vol;
                     int ask_vol = is_ask ? vol : 0;
                     
                     if(dom_count < 200) {
                        dom_parts[dom_count++] = "{\"timestamp\":\"" + JsonEscape(TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS)) + "\"," +
                                      "\"price\":" + JsonNumber(price) + "," +
                                      "\"level_index\":" + IntegerToString(index) + "," +
                                      "\"bid_volume\":" + IntegerToString(bid_vol) + "," +
                                      "\"ask_volume\":" + IntegerToString(ask_vol) + "}";
                     } else {
                        Print("[ALERTA] Buffer DOM overflow (200): eventos descartados neste ciclo");
                     }
                  }
               }
            }
            DOM_saved = 0;
            continue;
         }
         
         // Se possui 3 campos (TimestampTypePrice ; ... ; Volume), é um registro de Time & Sales
         if(ts == 3) {
            string timestamp_str = TimeToString((datetime)StringToInteger(StringSubstr(internal[1], 0, 10)), TIME_DATE|TIME_SECONDS);
            string type_char = StringSubstr(internal[1], 10, 1);
            // Na ClusterDelta: 'A' = Ask (Aggressor de Compra), 'B' = Bid (Aggressor de Venda)
            string side = (type_char == "B") ? "sell" : "buy"; 
            double price = StringToDouble(StringSubstr(internal[1], 11));
            int vol = (int)StringToInteger(internal[2]);
            
            if(tape_count < 500) {
               tape_parts[tape_count++] = "{\"timestamp\":\"" + JsonEscape(timestamp_str) + "\"," +
                              "\"price\":" + JsonNumber(price) + "," +
                              "\"volume\":" + IntegerToString(vol) + "," +
                              "\"side\":\"" + JsonEscape(side) + "\"}";
            } else {
               Print("[ALERTA] Buffer Tape overflow (500): eventos descartados neste ciclo");
            }
         } else if(DEBUG_PARSE_ERRORS && StringLen(packet[i]) > 5) {
            Print("[DEBUG] Formato inesperado no tape: '", packet[i], "' | Campos: ", ts);
         }
      }
   }
   
   string tape_result = JoinJsonArray(tape_parts, tape_count);
   string dom_result = JoinJsonArray(dom_parts, dom_count);
   
   if(tape_result == "[]" && dom_result == "[]") return;
   
   string current_ts = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
   string payload = "{\"symbol\":\"" + JsonEscape(GetSymbolName()) + "\"," +
                    "\"timestamp\":\"" + JsonEscape(current_ts) + "\"," +
                    "\"tape\":" + tape_result + "," +
                    "\"dom\":"  + dom_result  + "}";
                    
   if(!SendWithRetry(payload, retry_attempts)) {
      AddToPendingQueue(payload);
      stats_failed++;
   } else {
      stats_sent++;
      last_success = TimeCurrent();
      if(stats_sent % 100 == 0) {
         Print("[STATS] Enviados: ", stats_sent, " | Falhas: ", stats_failed, " | Último: ", TimeToString(last_success));
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
      Sleep(100);
   }
   return false;
}

void AddToPendingQueue(string payload)
{
   int size = ArraySize(pending_queue);
   if(size >= max_queue_size) {
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
         ArrayCopy(pending_queue, pending_queue, i, i + 1, WHOLE_ARRAY - i - 1);
         ArrayResize(pending_queue, ArraySize(pending_queue) - 1);
         i--;
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
   
   if(res == -1) return false;
   if(StringFind(response_headers, "200 OK") == -1) return false;
   
   return true;
}
