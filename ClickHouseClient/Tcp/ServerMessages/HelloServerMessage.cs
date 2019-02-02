namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class HelloServerMessage : ServerMessage
    {
        public ServerInfo ServerInfo { get; private set; }

        protected override void ReadImpl(StreamReader reader)
        {
            var name = reader.ReadString();
            var versionMajor = (int) reader.ReadInteger();
            var versionMinor = (int) reader.ReadInteger();
            var build = (int) reader.ReadInteger();
            var timeZone = reader.ReadString();
            ServerInfo = new ServerInfo
            {
                Name = name,
                VersionMajor = versionMajor,
                VersionMinor = versionMinor,
                Build = build,
                TimeZone = timeZone
            };
        }
    }
}
