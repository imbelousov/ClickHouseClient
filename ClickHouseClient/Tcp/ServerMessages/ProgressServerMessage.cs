namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class ProgressServerMessage : ServerMessage
    {
        protected override void ReadImpl(StreamReader reader)
        {
            reader.ReadInteger();
            reader.ReadInteger();
            reader.ReadInteger();
        }
    }
}
