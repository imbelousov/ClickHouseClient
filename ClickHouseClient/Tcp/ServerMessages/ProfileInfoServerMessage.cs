namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class ProfileInfoServerMessage : ServerMessage
    {
        protected override void ReadImpl(StreamReader reader)
        {
            reader.ReadInteger();
            reader.ReadInteger();
            reader.ReadInteger();
            reader.ReadByte();
            reader.ReadInteger();
            reader.ReadByte();
        }
    }
}
