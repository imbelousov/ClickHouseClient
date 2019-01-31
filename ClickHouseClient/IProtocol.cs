using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    internal interface IProtocol : IDisposable
    {
        void Connect();

        Task ConnectAsync(CancellationToken cancellationToken);

        ServerInfo Handshake();

        Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken);

        void SendQuery(string query);
    }
}
