namespace ClickHouseClient.Tcp.Data
{
    internal class NothingArray : DataArray
    {
        public override object this[int index] => null;

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            var buffer = new byte[rowCount];
            reader.Read(buffer, 0, buffer.Length);
        }
    }
}
