#property strict
#property indicator_chart_window

input string PYTHON_ENDPOINT = "http://127.0.0.1:8765/ingest";
input string SYMBOL_NAME = "6J";

double last_price = 0.0;

int OnInit()
{
   EventSetTimer(1);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
}

void OnTimer()
{
   string payload = BuildPayload();
   SendPayload(payload);
}

string BuildPayload()
{
   string ts = TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS);
   string tape = "[";
   tape += "{"timestamp":"" + ts + "","price":150.250,"volume":8,"side":"buy"},";
   tape += "{"timestamp":"" + ts + "","price":150.250,"volume":10,"side":"sell"}";
   tape += "]";

   string dom = "[";
   dom += "{"timestamp":"" + ts + "","price":150.250,"level_index":1,"bid_volume":120,"ask_volume":80},";
   dom += "{"timestamp":"" + ts + "","price":150.200,"level_index":2,"bid_volume":100,"ask_volume":60}";
   dom += "]";

   return "{" +
          ""symbol":"" + SYMBOL_NAME + ""," +
          ""timestamp":"" + ts + ""," +
          ""tape":" + tape + "," +
          ""dom":" + dom +
          "}";
}

void SendPayload(string payload)
{
   char data[];
   char result[];
   string headers = "Content-Type: application/json
";
   StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
   ResetLastError();
   int res = WebRequest("POST", PYTHON_ENDPOINT, headers, 3000, data, result, headers);
   if(res == -1)
   {
      Print("WebRequest failed: ", GetLastError());
   }
}
