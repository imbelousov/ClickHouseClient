namespace ClickHouseClient.Tcp.Columns
{
    internal class Int64Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return reader.ReadInt64();
        }
    }
}
