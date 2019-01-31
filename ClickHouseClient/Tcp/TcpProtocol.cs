using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient.Tcp
{
    internal class TcpProtocol : IProtocol
    {
        private const string ClientName = "ClickHouse client for .NET";
        private const int ClientVersionMajor = 1;
        private const int ClientVersionMinor = 1;
        private const int ClientRevision = 54140;
        private const int TimeZoneMinBuild = 54058;
        private const int ClientInfoWithQueryMinBuild = 54032;
        private const int QuotaKeyWithQueryMinBuild = 54060;

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
            _writer.WriteInteger(ClientMessageCode.Hello);
            _writer.WriteString(ClientName);
            _writer.WriteInteger(ClientVersionMajor);
            _writer.WriteInteger(ClientVersionMinor);
            _writer.WriteInteger(ClientRevision);
            _writer.WriteString(_connectionSettings.Database);
            _writer.WriteString(_connectionSettings.User);
            _writer.WriteString(_connectionSettings.Password);
            _stream.Flush();
            switch (_reader.ReadInteger())
            {
                case ServerMessageCode.Hello:
                    var info = new ServerInfo();
                    info.Name = _reader.ReadString();
                    info.VersionMajor = (int) _reader.ReadInteger();
                    info.VersionMinor = (int) _reader.ReadInteger();
                    info.Build = (int) _reader.ReadInteger();
                    if (info.Build >= TimeZoneMinBuild)
                        info.TimeZone = _reader.ReadString();
                    _serverInfo = info;
                    return info;
                case ServerMessageCode.Exception:
                    throw _reader.ReadException();
                default:
                    throw new TcpProtocolException("Unexpected response");
            }
        }

        public async Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken)
        {
            await _writer.WriteIntegerAsync(ClientMessageCode.Hello, cancellationToken);
            await _writer.WriteStringAsync(ClientName, cancellationToken);
            await _writer.WriteIntegerAsync(ClientVersionMajor, cancellationToken);
            await _writer.WriteIntegerAsync(ClientVersionMinor, cancellationToken);
            await _writer.WriteIntegerAsync(ClientRevision, cancellationToken);
            await _writer.WriteStringAsync(_connectionSettings.Database, cancellationToken);
            await _writer.WriteStringAsync(_connectionSettings.User, cancellationToken);
            await _writer.WriteStringAsync(_connectionSettings.Password, cancellationToken);
            await _stream.FlushAsync(cancellationToken);
            switch (await _reader.ReadIntegerAsync(cancellationToken))
            {
                case ServerMessageCode.Hello:
                    var info = new ServerInfo();
                    info.Name = await _reader.ReadStringAsync(cancellationToken);
                    info.VersionMajor = (int) await _reader.ReadIntegerAsync(cancellationToken);
                    info.VersionMinor = (int) await _reader.ReadIntegerAsync(cancellationToken);
                    info.Build = (int) await _reader.ReadIntegerAsync(cancellationToken);
                    if (info.Build >= TimeZoneMinBuild)
                        info.TimeZone = await _reader.ReadStringAsync(cancellationToken);
                    _serverInfo = info;
                    return info;
                case ServerMessageCode.Exception:
                    throw await _reader.ReadExceptionAsync(cancellationToken);
                default:
                    throw new TcpProtocolException("Unexpected response");
            }
        }

        public void SendQuery(string query)
        {
            _writer.WriteInteger(ClientMessageCode.Query);
            _writer.WriteInteger(0);
            if (_serverInfo.Build >= ClientInfoWithQueryMinBuild)
            {
                _writer.WriteByte(1);
                _writer.WriteInteger(0);
                _writer.WriteInteger(0);
                _writer.WriteString(GetClientAddress());
                _writer.WriteByte(1);
                _writer.WriteString(GetOsUser());
                _writer.WriteString(GetComputerName());
                _writer.WriteString(ClientName);
                _writer.WriteInteger(ClientVersionMajor);
                _writer.WriteInteger(ClientVersionMinor);
                _writer.WriteInteger(ClientRevision);
            }
            if (_serverInfo.Build >= QuotaKeyWithQueryMinBuild)
                _writer.WriteInteger(0);
            _writer.WriteInteger(0);
            _writer.WriteInteger(2);
            _writer.WriteInteger(0);
            _writer.WriteString(query);
            _stream.Flush();
        }

        private string GetClientAddress() => _tcpClient.Client.RemoteEndPoint.ToString();

        private string GetOsUser() => Environment.UserName;

        private string GetComputerName() => Environment.MachineName;

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

    public class ClickHouseException : Exception
    {
        public int Code { get; }
        public string Name { get; }
        public new string StackTrace { get; }

        public ClickHouseException(int code, string name, string message, string stackTrace, Exception innerException)
            : base(message, innerException)
        {
            Code = code;
            Name = name;
            StackTrace = stackTrace;
        }
    }
}
