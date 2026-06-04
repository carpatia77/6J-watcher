//+------------------------------------------------------------------+
//|  6J Watcher MQL Bridge                                           |
//|  Lê T&S e DOM da ClusterDelta e posta JSON para o servidor Python |
//+------------------------------------------------------------------+
#property strict


input string PYTHON_ENDPOINT = "http://127.0.0.1:8765/ingest";
input string SYMBOL_NAME     = "6J";
input int    TIMER_SECONDS   = 1;   // frequência de envio
input int    DOM_LEVELS      = 10;  // quantos níveis do DOM capturar

//--- inclui o indicador TSDOM da ClusterDelta que já está no gráfico
//--- O acesso real é feito via variáveis globais compartilhadas pelo TSDOM

int OnInit()
{
   EventSetTimer(TIMER_SECONDS);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason) { EventKillTimer(); }

void OnTimer()
{
   string tape_json = BuildTapeJSON();
   string dom_json  = BuildDOMJSON();
   if(tape_json == "[]" && dom_json == "[]") return;
   string payload = "{\"symbol\":\"" + SYMBOL_NAME + "\"," +
                    "\"timestamp\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"," +
                    "\"tape\":" + tape_json + "," +
                    "\"dom\":"  + dom_json  + "}";
   PostPayload(payload);
}

//--- Constrói array JSON com os últimos ticks do T&S
string BuildTapeJSON()
{
   MqlTick ticks[];
   int copied = CopyTicks(_Symbol, ticks, COPY_TICKS_TRADE, 0, 20);
   if(copied <= 0) return("[]");
   string result = "[";
   for(int i = 0; i < copied; i++)
   {
      string side = (ticks[i].flags & TICK_FLAG_BUY) ? "buy" : "sell";
      double price = (ticks[i].flags & TICK_FLAG_BUY) ? ticks[i].ask : ticks[i].bid;
      result += "{\"timestamp\":\"" + TimeToString(ticks[i].time, TIME_DATE|TIME_SECONDS) + "\"," +
                "\"price\":"        + DoubleToString(price, Digits()) + "," +
                "\"volume\":"       + IntegerToString((int)ticks[i].volume) + "," +
                "\"side\":\""       + side + "\"}";
      if(i < copied - 1) result += ",";
   }
   return result + "]";
}

//--- Constrói array JSON com os níveis atuais do DOM
string BuildDOMJSON()
{
   MqlBookInfo book[];
   if(!MarketBookGet(_Symbol, book)) return("[]");
   int total = ArraySize(book);
   if(total == 0) return("[]");
   string result = "[";
   string ts = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
   int count = 0;
   for(int i = 0; i < total && count < DOM_LEVELS * 2; i++)
   {
      string side;
      int bid_vol = 0, ask_vol = 0;
      if(book[i].type == BOOK_TYPE_SELL)      { side = "ask"; ask_vol = (int)book[i].volume; }
      else if(book[i].type == BOOK_TYPE_BUY)  { side = "bid"; bid_vol = (int)book[i].volume; }
      else continue;
      if(count > 0) result += ",";
      result += "{\"timestamp\":\"" + ts + "\"," +
                "\"price\":"        + DoubleToString(book[i].price, Digits()) + "," +
                "\"level_index\":"  + IntegerToString(count) + "," +
                "\"bid_volume\":"   + IntegerToString(bid_vol) + "," +
                "\"ask_volume\":"   + IntegerToString(ask_vol) + "}";
      count++;
   }
   return result + "]";
}

//--- HTTP POST
void PostPayload(string payload)
{
   char data[], result[];
   string headers = "Content-Type: application/json\r\n";
   StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   ResetLastError();
   int res = WebRequest("POST", PYTHON_ENDPOINT, headers, 3000, data, result, headers);
   if(res == -1) Print("6J Bridge — WebRequest error: ", GetLastError());
}
