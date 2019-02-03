namespace ClickHouseClient.Tcp.Data
{
    internal class StringArray : DataArray
    {
        private string[] _data;

        public override object this[int index] => _data[index];

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            _data = new string[rowCount];
            for (var i = 0; i < rowCount; i++)
            {
                _data[i] = reader.ReadString();
            }
        }
    }
}
