using Serilog;
using System.Collections;
using System.Net.Sockets;

namespace codecrafters_redis.src
{
    public class RedisServer
    {
        private TcpListener listener;

        // Key Value Storage 
        private Dictionary<object, object> store = new();

        public RedisServer(int port)
        {
            this.listener = new(System.Net.IPAddress.Any, port);

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            Log.Logger.Information("Server start");
        }

        public void Start()
        {
            this.listener.Start();
            Log.Logger.Verbose("Redis Server started on port " + ((System.Net.IPEndPoint)this.listener.LocalEndpoint).Port);
            while (true)
            {
                var client = this.listener.AcceptSocket();
                Log.Logger.Information("Client connected: " + client.RemoteEndPoint);
                Task.Run(() => this.HandleClient(client));
            }
        }

        private void HandleClient(Socket client)
        {
            while (client.Connected)
            {
                try
                {
                    // Read data from the client
                    byte[] buffer = new byte[1024];
                    int bytesRead = client.Receive(buffer);
                    if (bytesRead > 0)
                    {
                        string request = System.Text.Encoding.UTF8.GetString(buffer, 0, bytesRead);

                        if (!string.IsNullOrEmpty(request))
                        {
                            // Parse the request using RESPParser
                            var parts = (IEnumerable)RESPParser.Parse(request);
                            List<string> args = parts.Cast<string>().ToList();

                            var command = args[0].ToLower();
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

                                default:
                                    success = false;
                                    HandleError(client, "Unknown command");
                                    break;
                            }

                            if (success)
                            {
                                Log.Logger.Verbose("Handled command: " + command);
                            }
                            else
                            {
                                Log.Logger.Verbose("Failed to handle command: " + command);
                            }
                        }
                    }
                }
                catch (SocketException ex)
                {
                    if (ex.ErrorCode == 10054)
                    {
                        Log.Logger.Information("Client disconnected: " + client.RemoteEndPoint);
                    }
                }
                catch (Exception ex)
                {
                    HandleError(client, "Error handling client: " + ex.Message);

                    //Log.Logger.Error("Error handling client: " + ex.Message);
                }
                //finally
                //{
                //    if (client.Connected == false)
                //    {
                //        client.Close();

                //        Log.Logger.Information("Client connection closed");
                //    }
                //}
            }
        }

        private object? GetValue(object key)
        {
            if (this.store.TryGetValue(key, out var value))
            {
                if (value is Tuple<object, DateTime> tuple)
                {
                    if (tuple.Item2 < DateTime.UtcNow)
                    {
                        this.store.Remove(key);
                        return null;
                    }
                    else
                    {
                        return tuple.Item1;
                    }
                }
                return value;
            }
            return null;
        }

        private static void SendResponse(Socket client, string response)
        {
            client.Send(System.Text.Encoding.UTF8.GetBytes(response));
        }

        private static void HandlePing(Socket client)
        {
            SendResponse(client, "+PONG\r\n");
        }

        private static void HandleEcho(Socket client, List<string> args)
        {
            SendResponse(client, RESPParser.ToBulkString(string.Join(' ', args.Skip(1))));
        }

        private void HandleSet(Socket client, List<string> args)
        {
            object key = args[1];
            object value = args[2];

            try
            {
                var parameter = args[3];
                var parameterValue = args[4];

                if (string.Equals(parameter, "px", StringComparison.OrdinalIgnoreCase))
                {
                    value = new Tuple<object, DateTime>(value, DateTime.UtcNow.AddMilliseconds(int.Parse(parameterValue)));
                }
                else
                {
                    throw new NotImplementedException();
                }

            }
            catch (Exception)
            {
                // nothing to do, parameter is optional
            }

            if (this.store.ContainsKey(key))
            {
                this.store[key] = value;
            }
            else
            {
                if (this.store.TryAdd(key, value) == false)
                {
                    throw new NotImplementedException();
                }
            }

            SendResponse(client, RESPParser.ToSimpleString("OK"));
        }

        private void HandleGet(Socket client, List<string> args)
        {
            object key = args[1];

            var value = this.GetValue(key);

            SendResponse(client, RESPParser.ToBulkString(value?.ToString()));
        }

        private void HandlePush(Socket client, List<string> args)
        {
            string command = args[0];
            object key = args[1];
            var value = this.GetValue(args[1]);

            value ??= new List<string>();

            if (value is List<string> list)
            {
                args.RemoveAt(0); // remove command
                args.RemoveAt(0); // remove listname

                if (string.Equals(command, "RPush", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var item in args)
                    {
                        list.Add(item);
                    }
                }
                else if (string.Equals(command, "LPush", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var item in args)
                    {
                        list.Insert(0, item);
                    }
                }

                this.store[key] = list;

                SendResponse(client, RESPParser.ToInteger(list.Count));
            }
            else
            {
                SendResponse(client, RESPParser.ToError("Value is not a list"));
            }
        }

        private void HandleLRange(Socket client, List<string> args)
        {
            var value = this.GetValue(args[1]);

            if (value is List<string> list)
            {
                int start = int.Parse(args[2]);
                int stop = int.Parse(args[3]);
                if (start < 0)
                {
                    start = list.Count + start;
                }
                if (stop < 0)
                {
                    stop = list.Count + stop;
                }

                var result = list.Skip(start).Take(stop - start + 1).ToArray();

                SendResponse(client, RESPParser.ToArray(result));
            }
            else
            {
                if (value is null)
                {
                    SendResponse(client, RESPParser.ToArray());
                }
                else
                {
                    SendResponse(client, RESPParser.ToError("Value is not a list"));
                }
            }
        }

        private void HandleLLen(Socket client, List<string> args)
        {
            var value = this.GetValue(args[1]);
            if (value is List<string> list)
            {
                SendResponse(client, RESPParser.ToInteger(list.Count));
            }
            else
            {
                if (value is null)
                {
                    SendResponse(client, RESPParser.ToInteger(0));
                }
                else
                {
                    SendResponse(client, RESPParser.ToError("Value is not a list"));
                }
            }
        }

        private void HandlePop(Socket client, List<string> args)
        {
            string command = args[0];
            var value = this.GetValue(args[1]);

            if (value is List<string> list)
            {
                if (list.Count > 0)
                {
                    int count;

                    List<string> result = new();

                    try
                    {
                        count = int.Parse(args[2]);
                    }
                    catch (Exception)
                    {
                        count = 1;
                    }

                    if (string.Equals(command, "LPop", StringComparison.OrdinalIgnoreCase))
                    {
                        while (count > 0 && list.Count > 0)
                        {
                            result.Add(list[0]);

                            list.RemoveAt(0);

                            count--;
                        }
                    }
                    else if (string.Equals(command, "RPop", StringComparison.OrdinalIgnoreCase))
                    {
                        while (count > 0 && list.Count > 0)
                        {
                            int lastIndex = list.Count - 1;

                            result.Add(list[lastIndex]);

                            list.RemoveAt(lastIndex);

                            count--;
                        }
                    }

                    if (result.Count == 1)
                    {
                        SendResponse(client, RESPParser.ToBulkString(result[0]));
                    }
                    else
                    {
                        SendResponse(client, RESPParser.ToArray(result.ToArray()));
                    }

                    return;
                }
            }

            SendResponse(client, RESPParser.ToBulkString(null));
        }

        private void HandleBLPop(Socket client, List<string> args)
        {
            // Not implemented, as it requires blocking behavior which is more complex to handle in this simple server
            SendResponse(client, RESPParser.ToError("BLPop is not implemented"));
        }
        private static void HandleError(Socket client, string message)
        {
            Log.Logger.Error(message);

            SendResponse(client, RESPParser.ToError(message));
        }
    }
}
