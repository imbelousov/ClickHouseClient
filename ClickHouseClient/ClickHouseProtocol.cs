using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    internal class ClickHouseProtocol : Protocol
    {
        public ClickHouseProtocol(TcpClient tcpClient)
            : base(tcpClient)
        {
        }

        public override ServerInfo Handshake()
        {
            throw new System.NotImplementedException();
        }

        public override Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }
    }
}
