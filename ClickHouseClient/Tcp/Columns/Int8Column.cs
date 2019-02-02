namespace ClickHouseClient.Tcp.Columns
{
    internal class Int8Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return reader.ReadByte();
        }
    }
}
