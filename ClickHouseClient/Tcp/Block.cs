using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouseClient.Tcp.Columns;

namespace ClickHouseClient.Tcp
{
    internal class Block
    {
        public static readonly Block Empty = new Block(new List<Column>(), false, 0);

        public List<Column> Columns { get; }
        public bool Overflow { get; }
        public int BucketNumber { get; }
        public int ColumnCount => Columns.Count;
        public int RowCount => Columns.FirstOrDefault()?.RowCount ?? 0;

        public Block(List<Column> columns, bool overflow, int bucketNumber)
        {
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
            Overflow = overflow;
            BucketNumber = bucketNumber;
        }
    }
}
