using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    internal abstract class Protocol
    {
        protected readonly TcpClient TcpClient;

        protected Protocol(TcpClient tcpClient)
        {
            TcpClient = tcpClient;
        }

        public abstract ServerInfo Handshake();

        public abstract Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken);
    }
}
