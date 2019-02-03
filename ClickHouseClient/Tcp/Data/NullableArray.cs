namespace ClickHouseClient.Tcp.Data
{
    internal class NullableArray : DataArray
    {
        private DataArray _innerArray;
        private byte[] _nulls;

        public override object this[int index] => _nulls[index] == 0 ? _innerArray[index] : null;

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            _nulls = new byte[rowCount];
            reader.Read(_nulls, 0, _nulls.Length);
            var innerType = GetInnerType(type);
            _innerArray = Read(reader, innerType, rowCount);
        }

        private string GetInnerType(string type)
        {
            return type.Substring(9, type.Length - 10);
        }
    }
}
