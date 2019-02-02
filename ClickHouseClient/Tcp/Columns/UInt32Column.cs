namespace ClickHouseClient.Tcp.Columns
{
    internal class UInt32Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return (uint) reader.ReadInt32();
        }
    }
}
