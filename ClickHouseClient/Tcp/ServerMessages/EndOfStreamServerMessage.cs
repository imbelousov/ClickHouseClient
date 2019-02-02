namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class EndOfStreamServerMessage : ServerMessage
    {
        protected override void ReadImpl(StreamReader reader)
        {
        }
    }
}
