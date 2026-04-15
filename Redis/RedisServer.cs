using Serilog;
using System.Collections;
using System.Net.Sockets;

namespace codecrafters_redis.src
{
    public class RedisServer
    {
        // Haupt-Key-Value-Store – speichert alle SET/GET-Werte sowie Listen.
        // Werte mit TTL werden als Tuple<object, DateTime> gespeichert, wobei
        // DateTime der Ablaufzeitpunkt in UTC ist.
        private Dictionary<string, object> store = new();

        // Für jeden Key eine FIFO-Queue der wartenden BLPOP/BRPOP-Clients.
        // Wenn ein neues Element in eine Liste gepusht wird, bekommt der erste
        // wartende Client sofort die Antwort, statt sie in die Liste zu schreiben.
        private Dictionary<string, Queue<WaitingClient>> queues = new();

        // Lock-Objekt um Race Conditions zwischen HandlePush (Client-Threads)
        // und CheckQueues (Timer-Thread) auf den queues-Dictionary zu verhindern.
        private readonly object _queuesLock = new();

        private TcpListener listener;
        private Timer? _timer;

        private const int DefaultPort = 6379;
        private const int timerIntervalMs = 100;

        public RedisServer(int? parameterPort)
        {
            int port = parameterPort ?? DefaultPort;

            this.listener = new(System.Net.IPAddress.Any, port);

            //Log.Logger = new LoggerConfiguration()
            //    .MinimumLevel.Verbose()
            //    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
            //    .CreateLogger();

            Log.Logger.Information("RedisServer initialisiert auf Port {Port}", port);
        }

        ~RedisServer()
        {
            this._timer?.Dispose();
            Log.Logger.Information("RedisServer wird beendet, Timer gestoppt");
        }

        public void Start()
        {
            this.listener.Start();
            Log.Logger.Information("Lausche auf {Endpoint}", (System.Net.IPEndPoint)this.listener.LocalEndpoint);

            while (true)
            {
                var client = this.listener.AcceptSocket();
                Log.Logger.Information("Neuer Client verbunden: {Endpoint}", client.RemoteEndPoint);

                // Jeden Client in einem eigenen Task (Thread-Pool) behandeln,
                // damit der Haupt-Thread sofort auf den naechsten Client warten kann.
                Task.Run(() => this.HandleClient(client));
            }
        }

        /// <summary>
        /// Liest in einer Endlosschleife Befehle vom Client-Socket, parst sie als RESP
        /// und leitet sie an den zustaendigen Handler weiter.
        /// Jeder Client laeuft in seinem eigenen Task (siehe Start()).
        /// </summary>
        private void HandleClient(Socket client)
        {
            while (client.Connected)
            {
                try
                {
                    byte[] buffer = new byte[1024];
                    int bytesRead = client.Receive(buffer);

                    if (bytesRead == 0)
                    {
                        // Verbindung wurde vom Client sauber geschlossen
                        Log.Logger.Information("Client {Endpoint} hat die Verbindung getrennt", client.RemoteEndPoint);
                        break;
                    }

                    string request = System.Text.Encoding.UTF8.GetString(buffer, 0, bytesRead);

                    if (string.IsNullOrEmpty(request))
                    {
                        continue;
                    }

                    // RESP-Protokoll parsen → Liste von Strings (Command + Argumente)
                    var parts = (IEnumerable)RESPParser.Parse(request);
                    List<string> args = parts.Cast<string>().ToList();

                    if (args.Count == 0)
                    {
                        Log.Logger.Warning("Leere Anfrage von {Endpoint} erhalten, wird ignoriert", client.RemoteEndPoint);
                        continue;
                    }

                    var command = args[0].ToLower();
                    Log.Logger.Verbose("-> [{Endpoint}] {Command} {Args}",
                        client.RemoteEndPoint, command.ToUpper(), string.Join(" ", args.Skip(1)));

                    bool success = true;

                    switch (command)
                    {
                        case "ping":
                            HandlePing(client);
                            break;
                        case "echo":
                            HandleEcho(client, args);
                            break;
                        case "set":
                            this.HandleSet(client, args);
                            break;
                        case "get":
                            this.HandleGet(client, args);
                            break;
                        case "rpush":
                        case "lpush":
                            this.HandlePush(client, args);
                            break;
                        case "lrange":
                            this.HandleLRange(client, args);
                            break;
                        case "llen":
                            this.HandleLLen(client, args);
                            break;
                        case "lpop":
                        case "rpop":
                            this.HandlePop(client, args);
                            break;
                        case "blpop":
                        case "brpop":
                            this.HandleBPop(client, args);
                            break;
                        case "type":
                            this.HandleType(client, args);
                            break;
                        case "xadd":
                            this.HandleXAdd(client, args);
                            break;
                        default:
                            success = false;
                            Log.Logger.Warning("Unbekannter Befehl '{Command}' von {Endpoint}", command, client.RemoteEndPoint);
                            HandleError(client, $"ERR unknown command '{command}'");
                            break;
                    }

                    if (success)
                    {
                        Log.Logger.Verbose("<- [{Endpoint}] {Command} erfolgreich verarbeitet", client.RemoteEndPoint, command.ToUpper());
                    }
                }
                catch (SocketException ex) when (ex.ErrorCode == 10054)
                {
                    // Error 10054 = WSAECONNRESET: Client hat die Verbindung hart getrennt (Windows)
                    Log.Logger.Information("Client {Endpoint} hat die Verbindung zurueckgesetzt (ECONNRESET)", client.RemoteEndPoint);
                    break;
                }
                catch (Exception ex)
                {
                    Log.Logger.Error(ex, "Unerwarteter Fehler beim Verarbeiten der Anfrage von {Endpoint}", client.RemoteEndPoint);
                    HandleError(client, $"ERR internal error: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Liest den Wert fuer einen Key aus dem Store.
        /// Falls der Wert ein Tuple mit Ablaufzeit ist und die Zeit ueberschritten wurde,
        /// wird der Key automatisch geloescht und null zurueckgegeben (Lazy Expiry).
        /// </summary>
        private object? GetValue(string key)
        {
            if (!this.store.TryGetValue(key, out var value))
            {
                return null;
            }

            if (value is Tuple<string, DateTime> tuple)
            {
                if (tuple.Item2 < DateTime.UtcNow)
                {
                    // TTL abgelaufen → Key aus dem Store entfernen
                    Log.Logger.Verbose("Key '{Key}' ist abgelaufen (TTL war {ExpireAt:HH:mm:ss.fff}), wird geloescht", key, tuple.Item2);
                    this.store.Remove(key);
                    return null;
                }

                return tuple.Item1;
            }

            return value;
        }

        /// <summary>
        /// Gibt die Warteschlange der blockierenden Clients fuer einen Key zurueck,
        /// oder null wenn keine vorhanden.
        /// </summary>
        private Queue<WaitingClient>? GetQueue(string key)
        {
            this.queues.TryGetValue(key, out var queue);
            return queue;
        }

        /// <summary>
        /// Schreibt einen Wert in den Store. Ueberschreibt vorhandene Werte.
        /// </summary>
        private void SetValue(string key, object value)
        {
            bool isUpdate = this.store.ContainsKey(key);
            this.store[key] = value;

            Log.Logger.Verbose("Store: Key '{Key}' {Action}", key, isUpdate ? "aktualisiert" : "neu gesetzt");
        }

        private static void SendResponse(Socket client, string response)
        {
            client.Send(System.Text.Encoding.UTF8.GetBytes(response));
        }

        private static void HandleError(Socket client, string message)
        {
            Log.Logger.Error("Fehler an Client {Endpoint}: {Message}", client.RemoteEndPoint, message);
            SendResponse(client, RESPParser.ToError(message));
        }

        // ─── Command Handler ──────────────────────────────────────────────────────
        #region Commands
        private static void HandlePing(Socket client)
        {
            SendResponse(client, "+PONG\r\n");
        }

        private static void HandleEcho(Socket client, List<string> args)
        {
            var message = string.Join(' ', args.Skip(1));
            SendResponse(client, RESPParser.ToBulkString(message));
        }

        /// <summary>
        /// SET key value [PX milliseconds]
        /// Speichert einen Wert, optional mit Ablaufzeit in Millisekunden.
        /// </summary>
        private void HandleSet(Socket client, List<string> args)
        {
            string key = args[1];
            object value = args[2];

            // Optionale Parameter auswerten (PX = Ablaufzeit in Millisekunden)
            if (args.Count >= 5)
            {
                var parameter = args[3];
                var parameterValue = args[4];

                if (string.Equals(parameter, "px", StringComparison.OrdinalIgnoreCase))
                {
                    int ttlMs = int.Parse(parameterValue);
                    var expiresAt = DateTime.UtcNow.AddMilliseconds(ttlMs);
                    value = new Tuple<string, DateTime>(value.ToString()!, (DateTime)expiresAt);
                    this.SetValue(key, value);
                    Log.Logger.Verbose("SET '{Key}' mit TTL {Ttl}ms (laeuft ab um {ExpiresAt:HH:mm:ss.fff})", key, ttlMs, expiresAt);
                }
                else
                {
                    Log.Logger.Warning("SET: Unbekannter Parameter '{Param}', wird ignoriert", parameter);
                }
            }
            else
            {
                // Set speichert immer einen String-Wert, auch wenn die Eingabe z.B. eine Zahl ist (Redis-Verhalten).
                this.SetValue(key, value.ToString()!);
            }

            SendResponse(client, RESPParser.ToSimpleString("OK"));
        }

        /// <summary>
        /// GET key
        /// Gibt den Wert eines Keys zurueck, oder Null-Bulk-String wenn nicht vorhanden/abgelaufen.
        /// </summary>
        private void HandleGet(Socket client, List<string> args)
        {
            string key = args[1];
            var value = this.GetValue(key);

            if (value is null)
            {
                Log.Logger.Verbose("GET '{Key}': nicht gefunden oder abgelaufen", key);
            }
            else
            {
                Log.Logger.Verbose("GET '{Key}': gefunden -> '{Value}'", key, value);
            }

            SendResponse(client, RESPParser.ToBulkString(value?.ToString()));
        }

        /// <summary>
        /// LPUSH key value [value ...] / RPUSH key value [value ...]
        /// Fuegt ein oder mehrere Elemente an den Anfang (L) oder das Ende (R) einer Liste ein.
        ///
        /// Wichtig: Bevor Elemente in die Liste geschrieben werden, wird geprueft ob wartende
        /// BLPOP/BRPOP-Clients fuer diesen Key vorhanden sind. Diese bekommen das Element direkt,
        /// ohne dass es in die Liste gelangt (FIFO-Fairness wie bei echtem Redis).
        /// </summary>
        private void HandlePush(Socket client, List<string> args)
        {
            string command = args[0];
            string key = args[1];
            var value = this.GetValue(key);

            // Liste anlegen falls der Key noch nicht existiert
            value ??= new List<string>();

            if (value is not List<string> list)
            {
                Log.Logger.Warning("{Command}: Key '{Key}' existiert, ist aber keine Liste (Typ: {Type})", command, key, value.GetType().Name);
                SendResponse(client, RESPParser.ToError("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            // Command und Key aus args entfernen, damit nur noch die Werte uebrig sind
            args.RemoveAt(0);
            args.RemoveAt(0);

            var queue = this.GetQueue(key);
            int directlyServed = 0;

            lock (this._queuesLock)
            {
                // Solange wartende Clients UND noch Werte zum Pushen vorhanden sind:
                // Clients direkt bedienen statt in die Liste zu schreiben.
                while (queue != null && queue.Count > 0 && args.Count > 0)
                {
                    var waitingClient = queue.Dequeue();

                    // Wenn TrySetResult fehlschlaegt, wurde der Client bereits bedient (z.B. durch Timeout) -> ignorieren
                    if (!waitingClient.TCS.TrySetResult("Push"))
                    {
                        Log.Logger.Debug("TCS bereits abgeschlossen für Client {Endpoint} auf Key '{Key}'", waitingClient.Socket.RemoteEndPoint, key);
                        continue; // Client hat bereits geantwortet (z.B. durch Timeout) → ignorieren
                    }

                    if (string.Equals(waitingClient.Command, "blpop", StringComparison.OrdinalIgnoreCase))
                    {
                        // BLPOP wartet auf das erste Element → vorne nehmen
                        string item = args[0];
                        args.RemoveAt(0);
                        SendResponse(waitingClient.Socket, RESPParser.ToArray(new List<string> { key.ToString()!, item }));
                        Log.Logger.Information(
                            "BLPOP direkt bedient: Client {Endpoint} bekommt '{Item}' von Key '{Key}'",
                            waitingClient.Socket.RemoteEndPoint, item, key);
                    }
                    else if (string.Equals(waitingClient.Command, "brpop", StringComparison.OrdinalIgnoreCase))
                    {
                        // BRPOP wartet auf das letzte Element → hinten nehmen
                        string item = args[^1];
                        args.RemoveAt(args.Count - 1);
                        SendResponse(waitingClient.Socket, RESPParser.ToArray(new List<string> { key.ToString()!, item }));
                        Log.Logger.Information(
                            "BRPOP direkt bedient: Client {Endpoint} bekommt '{Item}' von Key '{Key}'",
                            waitingClient.Socket.RemoteEndPoint, item, key);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Unbekannter wartender Befehl '{waitingClient.Command}' für Key '{key}'");
                    }

                    directlyServed++;
                }
            }

            // Uebrige Elemente (die nicht direkt an wartende Clients gingen) in die Liste einfuegen
            if (args.Count > 0)
            {
                if (string.Equals(command, "rpush", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var item in args)
                    {
                        list.Add(item);
                    }
                }
                else // lpush
                {
                    foreach (var item in args)
                    {
                        list.Insert(0, item);
                    }
                }

                this.store[key] = list;
                Log.Logger.Verbose("{Command}: {Count} Element(e) in Liste '{Key}' geschrieben, Listenlänge jetzt {Length}",
                    command.ToUpper(), args.Count, key, list.Count);
            }

            if (directlyServed > 0)
            {
                Log.Logger.Verbose("{Command} '{Key}': {Served} wartende(r) Client(s) direkt bedient, {Stored} in Liste geschrieben",
                    command.ToUpper(), key, directlyServed, args.Count);
            }

            // Redis gibt die Laenge zurueck, die die Liste *haette*, wenn alle Elemente eingefuegt worden waeren
            SendResponse(client, RESPParser.ToInteger(list.Count + directlyServed));
        }

        /// <summary>
        /// LRANGE key start stop
        /// Gibt einen Ausschnitt der Liste zurueck. Negative Indizes zaehlen vom Ende.
        /// Beispiel: LRANGE mylist 0 -1 gibt die gesamte Liste zurueck.
        /// </summary>
        private void HandleLRange(Socket client, List<string> args)
        {
            var key = args[1];
            var value = this.GetValue(key);

            if (value is not List<string> list)
            {
                SendResponse(client, value is null
                    ? RESPParser.ToArray([])
                    : RESPParser.ToError("WRONGTYPE Operation against a key holding the wrong kind of value"));
                return;
            }

            int start = int.Parse(args[2]);
            int stop = int.Parse(args[3]);

            // Negative Indizes: -1 ist das letzte Element, -2 das vorletzte usw.
            if (start < 0) start = list.Count + start;
            if (stop < 0) stop = list.Count + stop;

            var result = list.Skip(start).Take(stop - start + 1).ToList();

            Log.Logger.Verbose("LRANGE '{Key}' [{Start}..{Stop}]: {Count} Element(e) zurueckgegeben", key, start, stop, result.Count);
            SendResponse(client, RESPParser.ToArray(result));
        }

        /// <summary>
        /// LLEN key
        /// Gibt die Anzahl der Elemente einer Liste zurueck, oder 0 wenn der Key nicht existiert.
        /// </summary>
        private void HandleLLen(Socket client, List<string> args)
        {
            var key = args[1];
            var value = this.GetValue(key);

            if (value is List<string> list)
            {
                Log.Logger.Verbose("LLEN '{Key}': {Count} Elemente", key, list.Count);
                SendResponse(client, RESPParser.ToInteger(list.Count));
            }
            else if (value is null)
            {
                Log.Logger.Verbose("LLEN '{Key}': Key nicht vorhanden, gebe 0 zurueck", key);
                SendResponse(client, RESPParser.ToInteger(0));
            }
            else
            {
                SendResponse(client, RESPParser.ToError("WRONGTYPE Operation against a key holding the wrong kind of value"));
            }
        }

        /// <summary>
        /// LPOP key [count] / RPOP key [count]
        /// Entfernt und gibt count Elemente vom Anfang (L) oder Ende (R) der Liste zurueck.
        /// Ohne count-Argument wird genau 1 Element als Bulk String zurueckgegeben.
        /// </summary>
        private void HandlePop(Socket client, List<string> args)
        {
            string command = args[0];
            var key = args[1];
            var value = this.GetValue(key);

            if (value is not List<string> list || list.Count == 0)
            {
                Log.Logger.Verbose("{Command} '{Key}': Liste leer oder nicht vorhanden -> Null", command.ToUpper(), key);
                SendResponse(client, RESPParser.ToBulkString(null));
                return;
            }

            // count ist optional; fehlt es, wird 1 verwendet
            int count = args.Count >= 3 && int.TryParse(args[2], out int parsedCount) ? parsedCount : 1;

            List<string> result = string.Equals(command, "lpop", StringComparison.OrdinalIgnoreCase)
                ? LPop(list, count)
                : RPop(list, count);

            Log.Logger.Verbose("{Command} '{Key}': {Count} Element(e) entfernt, {Remaining} verbleiben in der Liste",
                command.ToUpper(), key, result.Count, list.Count);

            // Einzelnes Element als Bulk String, mehrere als Array (Redis-Protokoll)
            if (result.Count == 1)
            {
                SendResponse(client, RESPParser.ToBulkString(result[0]));
            }
            else
            {
                SendResponse(client, RESPParser.ToArray(result));
            }
        }

        /// <summary>
        /// Entfernt count Elemente vom Anfang der Liste und gibt sie zurueck.
        /// Modifiziert die uebergebene Liste direkt (in-place).
        /// </summary>
        private static List<string> LPop(List<string> list, int count)
        {
            List<string> result = new();
            while (count > 0 && list.Count > 0)
            {
                result.Add(list[0]);
                list.RemoveAt(0);
                count--;
            }
            return result;
        }

        /// <summary>
        /// Entfernt count Elemente vom Ende der Liste und gibt sie zurueck.
        /// Modifiziert die uebergebene Liste direkt (in-place).
        /// </summary>
        private static List<string> RPop(List<string> list, int count)
        {
            List<string> result = new();
            while (count > 0 && list.Count > 0)
            {
                int lastIndex = list.Count - 1;
                result.Add(list[lastIndex]);
                list.RemoveAt(lastIndex);
                count--;
            }
            return result;
        }

        /// <summary>
        /// BLPOP key [key ...] timeout / BRPOP key [key ...] timeout
        ///
        /// Blockierende Pop-Variante:
        /// 1. Wenn einer der Keys eine nicht-leere Liste enthaelt, wird sofort geantwortet.
        /// 2. Sonst wird der Client als "WaitingClient" in die Queue des ersten Keys eingereiht.
        ///    Die Antwort kommt spaeter entweder von HandlePush (neues Element) oder
        ///    CheckQueues (Timeout abgelaufen → Null-Antwort).
        ///
        /// timeout == 0 bedeutet unbegrenzt warten.
        /// </summary>
        private void HandleBPop(Socket client, List<string> args)
        {
            // Syntax: BLPOP key [key ...] timeout  →  Timeout ist immer args[^1]
            var command = args[0];
            double timeout = double.Parse(args[^1], System.Globalization.CultureInfo.InvariantCulture);
            var keys = args.Skip(1).Take(args.Count - 2).ToList();

            Log.Logger.Verbose("{Command} von {Endpoint}: Keys=[{Keys}], Timeout={Timeout}s",
                command.ToUpper(), client.RemoteEndPoint, string.Join(", ", keys), timeout);

            // Schritt 1: Alle angegebenen Keys der Reihe nach pruefen.
            // Den ersten Key mit einem verfuegbaren Element sofort bedienen.
            foreach (var key in keys)
            {
                var value = this.GetValue(key);

                if (value is List<string> list && list.Count > 0)
                {
                    string item = string.Equals(command, "blpop", StringComparison.OrdinalIgnoreCase)
                        ? LPop(list, 1).FirstOrDefault(string.Empty)
                        : RPop(list, 1).FirstOrDefault(string.Empty);

                    Log.Logger.Information("{Command} '{Key}': sofort bedient -> '{Item}' an {Endpoint}",
                        command.ToUpper(), key, item, client.RemoteEndPoint);

                    SendResponse(client, RESPParser.ToArray(new List<string> { key, item }));
                    return;
                }
            }

            // Schritt 2: Alle Keys sind leer → Client blockierend warten lassen.
            // Wir registrieren den Client nur auf den ERSTEN Key (Redis-Verhalten:
            // BLPOP wartet auf den ersten Key in der Reihenfolge, der Daten bekommt).
            var waitKey = keys[0];
            DateTime? expireAt = timeout == 0 ? null : DateTime.UtcNow.AddSeconds(timeout);

            var waitingClient = new WaitingClient
            {
                Socket = client,
                Command = command,
                TCS = new TaskCompletionSource<string>()
            };

            lock (this._queuesLock)
            {
                if (!this.queues.ContainsKey(waitKey))
                {
                    this.queues[waitKey] = new Queue<WaitingClient>();
                }

                this.queues[waitKey].Enqueue(waitingClient);
            }

            if (timeout > 0)
            {
                Log.Logger.Information("{Command}: Client {Endpoint} wartet auf Key '{Key}' bis {ExpireAt:HH:mm:ss.fff}",
                    command.ToUpper(), client.RemoteEndPoint, waitKey, expireAt);

                // Nach ablauf des Timeouts (falls angegeben) wird geprueft, ob der Client noch wartet (TCS.TrySetResult) und ggf. mit Null-Antwort bedient.
                Task.Run(async () =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(timeout));

                    if (waitingClient.TCS.TrySetResult("Timeout"))
                    {
                        SendResponse(waitingClient.Socket, RESPParser.ToArray(null));

                        Log.Logger.Information("{Command} Timeout: Client {Endpoint} hat auf Key '{Key}' gewartet, aber kein Element wurde verfuegbar (Timeout abgelaufen)",
                            command.ToUpper(), waitingClient.Socket.RemoteEndPoint, waitKey);
                    }
                });
            }
            else
            {
                Log.Logger.Information("{Command}: Client {Endpoint} wartet unbegrenzt auf Key '{Key}'",
                    command.ToUpper(), client.RemoteEndPoint, waitKey);
            }
        }

        private void HandleType(Socket client, List<string> args)
        {
            string key = args[1];
            var value = this.GetValue(key);

            string type = value switch
            {
                null => "none",
                List<string> => "list",
                Stream => "stream",
                string => "string",
                _ => "unknown"
            };
            Log.Logger.Verbose("TYPE '{Key}': {Type}", key, type);
            SendResponse(client, RESPParser.ToSimpleString(type));
        }

        private void HandleXAdd(Socket client, List<string> args)
        {
            string key = args[1];
            var value = this.GetValue(key);

            value ??= new Stream();

            if (value is Stream stream)
            {
                args.RemoveAt(0); // Command
                args.RemoveAt(0); // Key

                string nextID;

                // Split Current ID
                string[] current_id_parts = new string[2];
                current_id_parts = args[0].Split('-');

                // Prepare for Last ID
                string[] last_id_parts = new string[2];
                var last = stream.Values.LastOrDefault();
                int last_seq = -1;

                if (last is not null)
                {
                    last_id_parts = last.Item1.Split('-');

                    if (string.Equals(current_id_parts[0], last_id_parts[0]))
                    {
                        last_seq = (last is null) ? 0 : int.Parse(last.Item1.Split('-')[1]);
                    }
                }

                bool generated = false;

                if (string.Equals(current_id_parts[0], "*"))
                {
                    generated = true;
                    current_id_parts = new string[2] { DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(), (last_seq + 1).ToString() };
                }
                else if (string.Equals(current_id_parts[1], "*"))
                {
                    generated = true;
                    current_id_parts[1] = (last_seq + 1).ToString();
                }

                if (generated)
                {
                    if (string.Equals(current_id_parts[0], "0") && string.Equals(current_id_parts[1], "0"))
                    {
                        Log.Logger.Warning("XADD '{Key}': Generierte ID ist 0-0, was nicht erlaubt ist", key);
                        current_id_parts[1] = "1";
                    }
                }

                long current_id = long.Parse(string.Join(null, current_id_parts));
                long last_id = (last is null) ? -1 : long.Parse(string.Join(null, last_id_parts));

                if (current_id == 0)
                {
                    HandleError(client, "ERR The ID specified in XADD must be greater than 0-0");
                    return;
                }

                if (current_id <= last_id)
                {
                    HandleError(client, "ERR The ID specified in XADD is equal or smaller than the target stream top item");
                    return;
                }


                //long current_ms = long.Parse(current_id_parts[0] ?? "0");
                //long last_ms = long.Parse(last_id_parts[0] ?? "0");

                //if (current_ms > 0 && current_ms != last_ms)
                //{
                //    current_id_parts[1] = "0";
                //}

                //int current_seq = int.Parse(current_id_parts[1] ?? "0");
                //last_seq = int.Parse(last_id_parts[1] ?? "0");

                //if (current_ms == last_ms)
                //{
                //    if (current_seq <= last_seq)
                //    {
                //        if (current_ms == 0 && current_seq == 0)
                //        {
                //            HandleError(client, "ERR The ID specified in XADD must be greater than 0-0");
                //        }
                //        else
                //        {
                //            HandleError(client, "ERR The ID specified in XADD is equal or smaller than the target stream top item");
                //        }
                //        return;
                //    }
                //}

                nextID = string.Join('-', current_id_parts);

                for (int i = 1; i < args.Count; i += 2)
                {
                    string streamkey = args[i];
                    object streamvalue = args[i + 1];

                    var entry = new Tuple<string, Dictionary<string, object>>(nextID, new Dictionary<string, object> { { streamkey, streamvalue } });
                    stream.Values.Add(entry);

                    Log.Logger.Verbose("XADD '{Key}': Neues Stream-Element mit ID '{ID}' hinzugefügt", key, nextID);
                }

                this.SetValue(key, stream);

                SendResponse(client, RESPParser.ToBulkString(nextID));
            }
            else
            {
                SendResponse(client, RESPParser.ToError("WRONGTYPE Operation against a key holding the wrong kind of value"));
            }
        }
        #endregion
    }
}
