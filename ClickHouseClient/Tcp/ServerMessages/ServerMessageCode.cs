namespace ClickHouseClient.Tcp.ServerMessages
{
    internal static class ServerMessageCode
    {
        public const int Hello = 0;
        public const int Data = 1;
        public const int Exception = 2;
        public const int Progress = 3;
        public const int Pong = 4;
        public const int EndOfStream = 5;
        public const int ProfileInfo = 6;
        public const int Totals = 7;
        public const int Extremes = 8;
    }
}
