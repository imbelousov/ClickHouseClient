namespace ClickHouseClient.Tcp.Columns
{
    internal abstract class Column
    {
        private object[] _rows;

        public string Name { get; private set; }
        public string Type { get; private set; }
        public int RowCount { get; private set; }

        public static Column Create(string name, string type)
        {
            Column column;
            switch (type)
            {
                case "UInt8":
                    column = new UInt8Column();
                    break;
                case "Int32":
                    column = new Int32Column();
                    break;
                default:
                    throw new TcpProtocolException($"Unsupported column type: {type}");
            }
            column.Name = name;
            column.Type = type;
            return column;
        }

        public void Read(StreamReader reader, int rowCount)
        {
            RowCount = rowCount;
            _rows = new object[rowCount];
            for (var i = 0; i < rowCount; i++)
            {
                _rows[i] = ReadRow(reader);
            }
        }

        protected abstract object ReadRow(StreamReader reader);
    }
}
