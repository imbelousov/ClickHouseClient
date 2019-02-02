namespace ClickHouseClient.Tcp.Columns
{
    internal class UInt16Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return (ushort) reader.ReadInt16();
        }
    }
}
