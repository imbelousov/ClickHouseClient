using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient
{
    /// <summary>
    /// Represents a protocol used for data exchange between ClickHouse and applications
    /// </summary>
    internal interface IProtocol : IDisposable
    {
        /// <summary>
        /// Establishes a connection to ClickHouse
        /// </summary>
        void Connect();

        /// <summary>
        /// Establishes a connection to ClickHouse
        /// </summary>
        Task ConnectAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Initializes open connection and returns basic information about ClickHouse back-end
        /// </summary>
        /// <returns>Basic information about ClickHouse server</returns>
        ServerInfo Handshake();

        /// <summary>
        /// Initializes open connection and returns basic information about ClickHouse back-end
        /// </summary>
        /// <returns>Basic information about ClickHouse server</returns>
        Task<ServerInfo> HandshakeAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Sends a query to ClickHouse but doesn't read response
        /// </summary>
        /// <param name="query">ClickHouse SQL query</param>
        void SendQuery(string query);
    }
}
