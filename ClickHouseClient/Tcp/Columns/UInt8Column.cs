namespace ClickHouseClient.Tcp.Columns
{
    internal class UInt8Column : Column
    {
        protected override object ReadRow(StreamReader reader)
        {
            return (sbyte) reader.ReadByte();
        }
    }
}
