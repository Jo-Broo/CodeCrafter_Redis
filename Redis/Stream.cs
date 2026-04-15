namespace codecrafters_redis.src
{
    public class Stream
    {
        public List<Tuple<string, Dictionary<string, object>>> Values { get; set; } = new();
    }
}
