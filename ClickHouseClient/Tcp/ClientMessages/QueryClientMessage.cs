using System;
using System.Net;

namespace ClickHouseClient.Tcp.ClientMessages
{
    internal class QueryClientMessage : ClientMessage
    {
        private readonly string _query;
        private readonly EndPoint _clientEndPoint;

        protected override int Code => ClientMessageCode.Query;

        public QueryClientMessage(string query, EndPoint clientEndPoint)
        {
            _query = query;
            _clientEndPoint = clientEndPoint;
        }

        protected override void WriteImpl(StreamWriter writer)
        {
            writer.WriteInteger(0);
            // Client info
            writer.WriteByte(1); // Query kind
            writer.WriteInteger(0);
            writer.WriteInteger(0);
            writer.WriteString(_clientEndPoint.ToString());
            writer.WriteByte(1); // Tcp interface
            writer.WriteString(Environment.UserName);
            writer.WriteString(Environment.MachineName);
            writer.WriteString(ClientName);
            writer.WriteInteger(ClientVersionMajor);
            writer.WriteInteger(ClientVersionMinor);
            writer.WriteInteger(ClientRevision);
            writer.WriteInteger(0); // Quota key
            // Settings
            writer.WriteInteger(0);
            // SQL
            writer.WriteInteger(2); // Stage
            writer.WriteInteger(0); // Compression
            writer.WriteString(_query);
        }
    }
}
