namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class ExceptionServerMessage : ServerMessage
    {
        public ClickHouseException Exception { get; private set; }

        protected override void ReadImpl(StreamReader reader)
        {
            Exception = reader.ReadException();
        }
    }
}
