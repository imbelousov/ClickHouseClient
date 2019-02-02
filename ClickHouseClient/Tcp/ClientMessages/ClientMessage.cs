namespace ClickHouseClient.Tcp.ClientMessages
{
    internal abstract class ClientMessage
    {
        protected const string ClientName = "ClickHouse client for .NET";
        protected const int ClientVersionMajor = 1;
        protected const int ClientVersionMinor = 1;
        protected const int ClientRevision = 54140;

        protected abstract int Code { get; }

        public void Write(StreamWriter writer)
        {
            writer.WriteInteger(Code);
            WriteImpl(writer);
        }

        protected abstract void WriteImpl(StreamWriter writer);

        protected void WriteClientInfo(StreamWriter writer)
        {
            writer.WriteString(ClientName);
            writer.WriteInteger(ClientVersionMajor);
            writer.WriteInteger(ClientVersionMinor);
            writer.WriteInteger(ClientRevision);
        }
    }
}
