namespace ClickHouseClient.Tcp.Columns
{
    internal class Int16Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return reader.ReadInt16();
        }
    }
}
