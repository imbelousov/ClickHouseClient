namespace ClickHouseClient.Tcp
{
    internal class TcpDataBatch : IDataBatch
    {
        private readonly Block _block;

        public int RowCount => _block.RowCount;

        public int ColumnCount => _block.ColumnCount;

        public TcpDataBatch(Block block)
        {
            _block = block;
        }

        public object GetValue(int column, int row)
        {
            return _block.Columns[column].Data[row];
        }

        public string GetColumnName(int column)
        {
            return _block.Columns[column].Name;
        }
    }
}
