using System;
using System.Data;
using System.Data.Common;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    public class ClickHouseConnection : DbConnection
    {
        private ConnectionSettings _connectionSettings;
        private ConnectionState _state;
        private TcpClient _tcpClient;

        public override string ConnectionString
        {
            get => _connectionSettings.ToString();
            set => _connectionSettings = new ConnectionSettings(value);
        }

        public override string Database => _connectionSettings.Database;

        public override ConnectionState State => _state;

        public override string DataSource => _connectionSettings.Host;

        public override string ServerVersion { get; }

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
            _tcpClient = CreateTcpClient();
            try
            {
                _tcpClient.Connect(_connectionSettings.Host, _connectionSettings.Port);
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
            _tcpClient = CreateTcpClient();
            try
            {
                await _tcpClient.ConnectAsync(_connectionSettings.Host, _connectionSettings.Port);
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
            if (_tcpClient != null)
            {
                _tcpClient.Close();
                _tcpClient.Dispose();
                _tcpClient = null;
            }
            _state = ConnectionState.Closed;
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
            _connectionSettings.Database = !string.IsNullOrWhiteSpace(databaseName) ? databaseName.Trim() : "default";
        }

        private TcpClient CreateTcpClient()
        {
            var tcpClient = new TcpClient();
            tcpClient.ReceiveTimeout = tcpClient.SendTimeout = _connectionSettings.Timeout;
            return tcpClient;
        }

        private bool IsOpen()
        {
            return _state == ConnectionState.Open || _state == ConnectionState.Connecting || _state == ConnectionState.Executing || _state == ConnectionState.Fetching;
        }
    }
}
