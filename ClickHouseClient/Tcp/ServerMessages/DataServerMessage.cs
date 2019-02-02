using System.Collections.Generic;
using ClickHouseClient.Tcp.Columns;

namespace ClickHouseClient.Tcp.ServerMessages
{
    internal class DataServerMessage : ServerMessage
    {
        public Block Block { get; private set; }

        protected override void ReadImpl(StreamReader reader)
        {
            reader.ReadString();
            var overflow = false;
            var bucketNumber = -1;
            for (var i = 0; i < 3; i++)
            {
                var field = reader.ReadByte();
                if (field == 0)
                    break;
                switch (field)
                {
                    case 1:
                        overflow = reader.ReadByte() != 0;
                        break;
                    case 2:
                        bucketNumber = reader.ReadInt32();
                        break;
                    default:
                        throw new TcpProtocolException("Unexpected field in data message header");
                }
            }
            var columnCount = (int) reader.ReadInteger();
            var rowCount = (int) reader.ReadInteger();
            var columns = new List<Column>(columnCount);
            for (var i = 0; i < columnCount; i++)
            {
                columns.Add(ReadColumn(reader, rowCount));
            }
            Block = new Block(columns, overflow, bucketNumber);
        }

        private Column ReadColumn(StreamReader reader, int rowCount)
        {
            var name = reader.ReadString();
            var type = reader.ReadString();
            var column = Column.Create(name, type);
            column.Read(reader, rowCount);
            return column;
        }
    }
}
