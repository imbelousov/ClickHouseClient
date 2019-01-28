using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    public class ClickHouseCommand : DbCommand
    {
        public override string CommandText { get; set; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection { get; }
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }

        public override int ExecuteNonQuery()
        {
            throw new System.NotImplementedException();
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public override object ExecuteScalar()
        {
            throw new System.NotImplementedException();
        }

        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new System.NotImplementedException();
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new System.NotImplementedException();
        }

        public override void Prepare()
        {
            throw new System.NotImplementedException();
        }

        public override void Cancel()
        {
            throw new System.NotImplementedException();
        }
    }
}
