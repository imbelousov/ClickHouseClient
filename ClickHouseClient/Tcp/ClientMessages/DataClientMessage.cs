namespace ClickHouseClient.Tcp.ClientMessages
{
    internal class DataClientMessage : ClientMessage
    {
        private readonly Block _block;

        protected override int Code => ClientMessageCode.Data;

        public DataClientMessage(Block block)
        {
            _block = block;
        }

        protected override void WriteImpl(StreamWriter writer)
        {
            writer.WriteInteger(0);
            writer.WriteInteger(1);
            writer.WriteByte(0); // Overflow
            writer.WriteInteger(2);
            writer.WriteInt32(-1); // Bucket number
            writer.WriteInteger(0);
            writer.WriteInteger(_block.ColumnCount);
            writer.WriteInteger(_block.RowCount);
        }
    }
}
