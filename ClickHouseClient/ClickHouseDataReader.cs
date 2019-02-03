using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    public class ClickHouseDataReader : DbDataReader
    {
        private readonly ClickHouseConnection _connection;
        private IDataBatch _currentBatch;
        private int _currentRow;
        private bool _isEof;
        private bool _hasRows;
        private int _recordsAffected;

        public override int FieldCount => _currentBatch.ColumnCount;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected => _recordsAffected;

        public override bool HasRows => _hasRows;

        public override bool IsClosed { get; }

        public override int Depth { get; }

        public ClickHouseDataReader(ClickHouseConnection connection)
        {
            _connection = connection;
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool) GetValue(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte) GetValue(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime) GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal) GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double) GetValue(ordinal);
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            return (float) GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid) GetValue(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return (short) GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (int) GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (long) GetValue(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return _currentBatch.GetColumnName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            return (string) GetValue(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return _currentBatch.GetValue(ordinal, _currentRow);
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return false;
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        public override bool Read()
        {
            if (_isEof)
                return false;
            if (_currentRow + 1 >= _currentBatch?.RowCount)
                _currentBatch = null;
            if (_currentBatch == null)
            {
                if (!ReadBatch())
                    return false;
            }
            _currentRow++;
            _recordsAffected++;
            return true;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        internal void ReadSchema()
        {
            _hasRows = ReadBatch();
        }

        private bool ReadBatch()
        {
            _currentRow = -1;
            while (!_isEof)
            {
                _currentBatch = _connection.Protocol.ReadData();
                if (_currentBatch == null)
                    _isEof = true;
                else if (_currentBatch.RowCount > 0)
                    return true;
            }
            return false;
        }
    }
}
