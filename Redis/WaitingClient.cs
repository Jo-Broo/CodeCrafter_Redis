using System.Net.Sockets;

namespace codecrafters_redis.src
{
    public class WaitingClient
    {
        public required Socket Socket { get; set; }
        public required DateTime? ExpireAt { get; set; }
        public required string Command { get; set; }
    }
}
