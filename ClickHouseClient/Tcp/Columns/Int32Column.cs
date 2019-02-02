namespace ClickHouseClient.Tcp.Columns
{
    internal class Int32Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return reader.ReadInt32();
        }
    }
}
