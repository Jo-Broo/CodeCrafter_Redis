using Serilog;

namespace codecrafters_redis.src
{
    public static class RESPParser
    {
        /// <summary>
        /// Formats a single Value to the RESP Format
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToBulkString(string? value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return "$-1\r\n";
            }

            return $"${value.Length}\r\n{value}\r\n";
        }

        public static string ToInteger(int value)
        {
            return $":{value}\r\n";
        }

        public static string ToError(string value)
        {
            return $"-{value}\r\n";
        }

        public static string ToArray(List<string>? values)
        {
            if (values is null)
            {
                return "*-1\r\n";
            }

            if (values.Count == 0)
            {
                return "*0\r\n";
            }

            string result = $"*{values.Count}\r\n";
            foreach (var value in values)
            {
                result += ToBulkString(value);
            }
            return result;
        }

        public static string ToSimpleString(string value)
        {
            return $"+{value}\r\n";
        }

        public static Object? GetValue(string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return null;
            }

            char prefix = input[0];

            switch (prefix)
            {
                case '+': // Simple String
                case '-': // Error
                    return input.Substring(1).TrimEnd('\n').TrimEnd('\r');
                case ':': // Integer
                    return int.Parse(input.Substring(1).TrimEnd('\n').TrimEnd('\r'));
                case '$': // Bulk String
                    return ToBulkString(input.Split("\r\n")[1]);
                case '*': // Array
                    Log.Logger.Error("Array parsing is not implemented in GetValue. Use ParseCommand instead.");
                    return null;
                // throw new InvalidOperationException("Array parsing is not implemented in GetValue. Use ParseCommand instead.");
                default:
                    Log.Logger.Error($"Invalid RESP format. Unrecognized prefix: {prefix}");
                    return null;
            }
        }

        public static object Parse(string input)
        {
            if (!string.IsNullOrEmpty(input))
            {
                char prefix = input[0];
                string content = input.Substring(1);

                //if (prefix == '*')
                //{
                //    var elements = content.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                //    IEnumerable<string> result = new List<string>();

                //    if (int.Parse(elements[0]) == 1)
                //    {
                //        result = result.Append<string>(elements[2]);
                //    }
                //    else
                //    {
                //        for (int i = 1; i < (int.Parse(elements[0])) * 2; i += 2)
                //        {
                //            // construct new input
                //            var element = $"{elements[i]}\r\n{elements[i + 1]}\r\n";
                //            // Get raw value
                //            var test = RESPParser.GetValue(element);
                //            // Add if not null
                //            if (test != null)
                //            {
                //                result = result.Append<string>(test.ToString() ?? string.Empty);
                //            }
                //        }
                //    }

                //    return result;
                //}
                //else
                //{
                //    Log.Logger.Error($"Invalid RESP format for command parsing. Expected RESP Array but got prefix: {prefix}");
                //    throw new InvalidOperationException("Only RESP Arrays are supported for parsing commands.");
                //}

                switch (prefix)
                {
                    case '+': // Simple String
                        return content;
                    case '-': // Error
                              // Der Server behandelt keine Fehler
                        break;
                    case ':': // Integer
                        break;
                    case '$': // Bulk String
                        return content.Split("\r\n")[1];
                    case '*': // Array
                        var elements = content.Split("\r\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                        IEnumerable<object> result = new List<object>();

                        if (int.Parse(elements[0]) == 1)
                        {
                            result = result.Append<object>(elements[2]);
                        }
                        else
                        {
                            for (int i = 1; i < (int.Parse(elements[0])) * 2; i += 2)
                            {
                                // construct new input
                                var element = $"{elements[i]}\r\n{elements[i + 1]}\r\n";

                                var test = RESPParser.Parse(element);
                                result = result.Append<object>(test.ToString() ?? string.Empty);
                            }
                        }

                        return result;
                    default:
                        break;
                }
            }

            return string.Empty;
        }
    }
}
