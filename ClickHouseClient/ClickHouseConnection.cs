using System;
using System.Data;
using System.Data.Common;

namespace ClickHouseClient
{
    public class ClickHouseConnection : DbConnection
    {
        public override string ConnectionString { get; set; }
        public override string Database { get; }
        public override ConnectionState State { get; }
        public override string DataSource { get; }
        public override string ServerVersion { get; }

        public override void Open()
        {
            throw new System.NotImplementedException();
        }

        public override void Close()
        {
            throw new System.NotImplementedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            throw new System.NotImplementedException();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new System.NotImplementedException();
        }
    }
}
