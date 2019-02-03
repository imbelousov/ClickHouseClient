using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouseClient.Tcp.ClientMessages;
using ClickHouseClient.Tcp.ServerMessages;

namespace ClickHouseClient.Tcp
{
    internal class TcpProtocol : IProtocol
    {
        private readonly ConnectionSettings _connectionSettings;
        private readonly TcpClient _tcpClient;
        private Stream _stream;
        private StreamReader _reader;
        private StreamWriter _writer;
        private ServerInfo _serverInfo;

        public TcpProtocol(ConnectionSettings connectionSettings)
        {
            _connectionSettings = connectionSettings;
            _tcpClient = new TcpClient();
            _tcpClient.ReceiveTimeout = _tcpClient.SendTimeout = connectionSettings.Timeout;
        }

        public void Connect()
        {
            _tcpClient.Connect(_connectionSettings.Host, _connectionSettings.Port);
            _stream = _tcpClient.GetStream();
            _reader = new StreamReader(_stream);
            _writer = new StreamWriter(_stream);
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            await _tcpClient.ConnectAsync(_connectionSettings.Host, _connectionSettings.Port);
            _stream = _tcpClient.GetStream();
            _reader = new StreamReader(_stream);
            _writer = new StreamWriter(_stream);
        }

        public ServerInfo Handshake()
        {
            WriteMessage(new HelloClientMessage(_connectionSettings.Database, _connectionSettings.User, _connectionSettings.Password));
            _stream.Flush();
            if (!(ReadMessage() is HelloServerMessage helloMessage))
                throw new TcpProtocolException("Unexpected response");
            _serverInfo = helloMessage.ServerInfo;
            return helloMessage.ServerInfo;
        }

        public async Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken)
        {
            WriteMessage(new HelloClientMessage(_connectionSettings.Database, _connectionSettings.User, _connectionSettings.Password));
            _stream.Flush();
            if (!(ReadMessage() is HelloServerMessage helloMessage))
                throw new TcpProtocolException("Unexpected response");
            _serverInfo = helloMessage.ServerInfo;
            return helloMessage.ServerInfo;
        }

        public void SendQuery(string query)
        {
            WriteMessage(new QueryClientMessage(query, _tcpClient.Client.RemoteEndPoint));
            WriteMessage(new DataClientMessage(Block.Empty));
            _stream.Flush();
        }

        public IDataBatch ReadData()
        {
            ServerMessage message;
            do
            {
                message = ReadMessage();
            } while (!(message is EndOfStreamServerMessage || message is DataServerMessage));
            if (message is DataServerMessage dataMessage)
                return new TcpDataBatch(dataMessage.Block);
            return null;
        }

        private void WriteMessage(ClientMessage message)
        {
            message.Write(_writer);
        }

        private ServerMessage ReadMessage()
        {
            var message = ServerMessage.Read(_reader);
            if (message is ExceptionServerMessage exceptionMessage)
                throw exceptionMessage.Exception;
            return message;
        }

        public void Dispose()
        {
            _tcpClient?.Dispose();
        }
    }

    public class TcpProtocolException : Exception
    {
        public TcpProtocolException(string message)
            : base(message)
        {
        }
    }
}
