using codecrafters_redis.src;

// You can use print statements as follows for debugging, they'll be visible when running tests.
//Console.WriteLine($"Redis Server start at {DateTime.Now.ToShortTimeString()}");

RedisServer server = new(6379);
server.Start();

//// Uncomment the code below to pass the first stage
//TcpListener server = new(IPAddress.Any, 6379);
//server.Start();

//Console.WriteLine($"Server listening on {server.LocalEndpoint}");

//// Key Value Storage 
//Dictionary<object, object> store = new();

//// Main server loop
//while (true)
//{
//    var socket = server.AcceptSocket(); // wait for client connection
//    //Console.WriteLine("Client connected");
//    _ = Task.Run(() => HandleClient(socket)); // fire and forget client handler
//}

//object? GetValue(object key)
//{
//    if (store.TryGetValue(key, out var value))
//    {
//        if (value is Tuple<object, DateTime> tuple)
//        {
//            if (tuple.Item2 < DateTime.UtcNow)
//            {
//                store.Remove(key);
//                return null;
//            }
//            else
//            {
//                return tuple.Item1;
//            }
//        }
//        return value;
//    }
//    return null;
//}

//void SendResponse(Socket client, string response)
//{
//    client.Send(System.Text.Encoding.UTF8.GetBytes(response));
//}

//void HandleClient(Socket client)
//{
//    while (true)
//    {
//        try
//        {
//            byte[] buffer = new byte[1024];
//            int bytesRead = client.Receive(buffer);
//            if (bytesRead > 0)
//            {
//                string request = System.Text.Encoding.UTF8.GetString(buffer, 0, bytesRead);

//                if (!string.IsNullOrEmpty(request))
//                {
//                    var parts = (IEnumerable)RESPParser.Parse(request);
//                    List<string> args = new();

//                    //Console.WriteLine("Parts:");
//                    foreach (var part in parts)
//                    {
//                        //Console.WriteLine($"- {part}");
//                        var arg = part.ToString();

//                        if (!string.IsNullOrEmpty(arg))
//                        {
//                            args.Add(arg);
//                        }
//                    }

//                    var command = args[0].ToLower();

//                    if (string.Equals(command, "Ping", StringComparison.OrdinalIgnoreCase))
//                    {
//                        SendResponse(client, "+PONG\r\n");
//                    }
//                    else if (string.Equals(command, "Echo", StringComparison.OrdinalIgnoreCase))
//                    {
//                        args.RemoveAt(0);

//                        SendResponse(client, RESPParser.ToBulkString(string.Join(' ', args)));
//                    }
//                    else if (string.Equals(command, "Set", StringComparison.OrdinalIgnoreCase))
//                    {
//                        object key = args[1];
//                        object value = args[2];

//                        try
//                        {
//                            var parameter = args[3];
//                            var parameterValue = args[4];

//                            if (string.Equals(parameter, "px", StringComparison.OrdinalIgnoreCase))
//                            {
//                                value = new Tuple<object, DateTime>(value, DateTime.UtcNow.AddMilliseconds(int.Parse(parameterValue)));
//                            }
//                            else
//                            {
//                                throw new NotImplementedException();
//                            }

//                        }
//                        catch (Exception)
//                        {
//                            // nothing to do, parameter is optional
//                        }

//                        if (store.ContainsKey(key))
//                        {
//                            store[key] = value;
//                        }
//                        else
//                        {
//                            if (store.TryAdd(key, value) == false)
//                            {
//                                throw new NotImplementedException();
//                            }
//                        }

//                        SendResponse(client, RESPParser.ToSimpleString("OK"));
//                    }
//                    else if (string.Equals(command, "Get", StringComparison.OrdinalIgnoreCase))
//                    {
//                        object key = args[1];

//                        var value = GetValue(key);

//                        SendResponse(client, RESPParser.ToBulkString(value?.ToString()));
//                    }
//                    else if (string.Equals(command, "RPush", StringComparison.OrdinalIgnoreCase) || string.Equals(command, "LPush", StringComparison.OrdinalIgnoreCase))
//                    {
//                        object key = args[1];
//                        var value = GetValue(args[1]);

//                        value ??= new List<string>();

//                        if (value is List<string> list)
//                        {
//                            args.RemoveAt(0); // remove command
//                            args.RemoveAt(0); // remove listname

//                            if (string.Equals(command, "RPush", StringComparison.OrdinalIgnoreCase))
//                            {
//                                foreach (var item in args)
//                                {

//                                    list.Add(item);
//                                }
//                            }
//                            else if (string.Equals(command, "LPush", StringComparison.OrdinalIgnoreCase))
//                            {
//                                foreach (var item in args)
//                                {
//                                    list.Insert(0, item);
//                                }
//                            }

//                            store[key] = list;

//                            SendResponse(client, RESPParser.ToInteger(list.Count));
//                        }
//                        else
//                        {
//                            SendResponse(client, RESPParser.ToError("Value is not a list"));
//                        }
//                    }
//                    else if (string.Equals(command, "LRange", StringComparison.OrdinalIgnoreCase))
//                    {
//                        var value = GetValue(args[1]);

//                        if (value is List<string> list)
//                        {
//                            int start = int.Parse(args[2]);
//                            int stop = int.Parse(args[3]);
//                            if (start < 0)
//                            {
//                                start = list.Count + start;
//                            }
//                            if (stop < 0)
//                            {
//                                stop = list.Count + stop;
//                            }

//                            var result = list.Skip(start).Take(stop - start + 1).ToArray();

//                            SendResponse(client, RESPParser.ToArray(result));
//                        }
//                        else
//                        {
//                            if (value is null)
//                            {
//                                SendResponse(client, RESPParser.ToArray());
//                            }
//                            else
//                            {
//                                SendResponse(client, RESPParser.ToError("Value is not a list"));
//                            }
//                        }
//                    }
//                    else if (string.Equals(command, "LLen", StringComparison.OrdinalIgnoreCase))
//                    {
//                        var value = GetValue(args[1]);
//                        if (value is List<string> list)
//                        {
//                            SendResponse(client, RESPParser.ToInteger(list.Count));
//                        }
//                        else
//                        {
//                            if (value is null)
//                            {
//                                SendResponse(client, RESPParser.ToInteger(0));
//                            }
//                            else
//                            {
//                                SendResponse(client, RESPParser.ToError("Value is not a list"));
//                            }
//                        }
//                    }
//                    else if (string.Equals(command, "LPop", StringComparison.OrdinalIgnoreCase))
//                    {
//                        var value = GetValue(args[1]);

//                        if (value is List<string> list)
//                        {
//                            if (list.Count > 0)
//                            {
//                                int count;

//                                try
//                                {
//                                    count = int.Parse(args[2]);
//                                }
//                                catch (Exception)
//                                {
//                                    count = 1;
//                                }

//                                List<string> result = new();

//                                while (count > 0 && list.Count > 0)
//                                {
//                                    result.Add(list[0]);

//                                    list.RemoveAt(0);

//                                    count--;
//                                }

//                                if (result.Count == 1)
//                                {
//                                    SendResponse(client, RESPParser.ToBulkString(result[0]));
//                                }
//                                else
//                                {
//                                    SendResponse(client, RESPParser.ToArray(result.ToArray()));
//                                }
//                            }
//                            else
//                            {
//                                SendResponse(client, RESPParser.ToBulkString(null));
//                            }
//                        }
//                        else
//                        {
//                            SendResponse(client, RESPParser.ToBulkString(null));
//                        }
//                    }
//                    else
//                    {
//                        SendResponse(client, RESPParser.ToError("Unknown Command"));
//                    }
//                }
//            }
//        }
//        catch (Exception ex)
//        {
//            Console.WriteLine($"Fehler: {ex.Message}");
//            break;
//        }
//    }
//}