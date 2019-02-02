namespace ClickHouseClient.Tcp.Columns
{
    internal class UInt64Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return (ulong) reader.ReadInt64();
        }
    }
}
