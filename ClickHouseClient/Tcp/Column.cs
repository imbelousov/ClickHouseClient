using ClickHouseClient.Tcp.Data;

namespace ClickHouseClient.Tcp
{
    internal class Column
    {
        public string Name { get; }
        public string Type { get; }
        public DataArray Data { get; }

        public int RowCount => Data.Count;

        public Column(string name, string type, DataArray data)
        {
            Name = name;
            Type = type;
            Data = data;
        }
    }
}
