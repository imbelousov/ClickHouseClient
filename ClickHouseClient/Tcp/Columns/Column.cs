using System.Collections;

namespace ClickHouseClient.Tcp.Columns
{
    internal abstract class Column
    {
        private object[] _rows;

        public string Name { get; private set; }
        public string Type { get; private set; }
        public bool Nullable { get; private set; }
        public int RowCount { get; private set; }

        public static Column Create(string name, string type)
        {
            Column column;
            var nullable = false;
            var innerType = type;
            if (type.StartsWith("Nullable("))
            {
                innerType = type.Substring(9, type.Length - 10);
                nullable = true;
            }
            switch (innerType)
            {
                case "Int8":
                    column = new Int8Column();
                    break;
                case "UInt8":
                    column = new UInt8Column();
                    break;
                case "Int16":
                    column = new Int16Column();
                    break;
                case "UInt16":
                    column = new UInt16Column();
                    break;
                case "Int32":
                    column = new Int32Column();
                    break;
                case "UInt32":
                    column = new UInt32Column();
                    break;
                case "Int64":
                    column = new Int64Column();
                    break;
                case "UInt64":
                    column = new UInt64Column();
                    break;
                default:
                    throw new TcpProtocolException($"Unsupported column type: {type}");
            }
            column.Name = name;
            column.Type = type;
            column.Nullable = nullable;
            return column;
        }

        public void Read(StreamReader reader, int rowCount)
        {
            RowCount = rowCount;
            _rows = new object[rowCount];
            var nulls = null as BitArray;
            if (Nullable)
            {
                nulls = new BitArray(rowCount);
                for (var i = 0; i < rowCount; i++)
                {
                    nulls[i] = reader.ReadByte() != 0;
                }
            }
            for (var i = 0; i < rowCount; i++)
            {
                var value = ReadRow(reader);
                if (nulls != null && !nulls[i])
                    _rows[i] = value;
            }
        }

        protected abstract object ReadRow(StreamReader reader);
    }
}
