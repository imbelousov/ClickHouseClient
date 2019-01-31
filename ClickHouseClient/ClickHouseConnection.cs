using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    public class ClickHouseConnection : DbConnection
    {
        private ConnectionSettings _connectionSettings;
        private ConnectionState _state;
        private ServerInfo _serverInfo;

        internal IProtocol Protocol { get; private set; }

        public override string ConnectionString
        {
            get => _connectionSettings.ToString();
            set => _connectionSettings = new ConnectionSettings(value);
        }

        public override string Database => _connectionSettings.Database;

        public override ConnectionState State => _state;

        public override string DataSource => _connectionSettings.Host;

        public override string ServerVersion => _serverInfo != null ? $"{_serverInfo.Name} {_serverInfo.VersionMajor}.{_serverInfo.VersionMinor}.{_serverInfo.Build}" : null;

        public ClickHouseConnection()
            : this(null)
        {
        }

        public ClickHouseConnection(string connectionString)
        {
            _connectionSettings = new ConnectionSettings(connectionString);
            _state = ConnectionState.Closed;
        }

        public override void Open()
        {
            if (IsOpen())
                throw new InvalidOperationException("Connection is open");
            _state = ConnectionState.Connecting;
            Protocol = ProtocolFactory.Instance.Create(_connectionSettings);
            try
            {
                Protocol.Connect();
                _serverInfo = Protocol.Handshake();
            }
            catch
            {
                _state = ConnectionState.Broken;
                throw;
            }
            _state = ConnectionState.Open;
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (IsOpen())
                throw new InvalidOperationException("Connection is open");
            _state = ConnectionState.Connecting;
            Protocol = ProtocolFactory.Instance.Create(_connectionSettings);
            try
            {
                await Protocol.ConnectAsync(cancellationToken);
                _serverInfo = await Protocol.HandshakeAsync(cancellationToken);
            }
            catch
            {
                _state = ConnectionState.Broken;
                throw;
            }
            _state = ConnectionState.Open;
        }

        public override void Close()
        {
            if (Protocol != null)
            {
                Protocol.Dispose();
                Protocol = null;
            }
            _state = ConnectionState.Closed;
        }

        protected override DbCommand CreateDbCommand()
        {
            return new ClickHouseCommand(this);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            _connectionSettings.Database = !string.IsNullOrWhiteSpace(databaseName) ? databaseName.Trim() : "default";
        }

        private bool IsOpen()
        {
            return _state == ConnectionState.Open || _state == ConnectionState.Connecting || _state == ConnectionState.Executing || _state == ConnectionState.Fetching;
        }
    }
}
