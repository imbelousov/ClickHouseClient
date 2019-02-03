namespace ClickHouseClient
{
    /// <summary>
    /// Represents a batch of data including all columns and n rows
    /// </summary>
    internal interface IDataBatch
    {
        /// <summary>
        /// Returns a number of rows in the batch
        /// </summary>
        int RowCount { get; }

        /// <summary>
        /// Returns a number of columns
        /// </summary>
        int ColumnCount { get; }

        /// <summary>
        /// Returns a CLR value that stored in the column with specified index
        /// </summary>
        /// <param name="column">Index of the column</param>
        /// <param name="row">Index of the row</param>
        /// <returns>Stored value</returns>
        object GetValue(int column, int row);

        /// <summary>
        /// Returns a name of the column with specified index
        /// </summary>
        /// <param name="column">Index of the column</param>
        /// <returns>Name</returns>
        string GetColumnName(int column);
    }
}
