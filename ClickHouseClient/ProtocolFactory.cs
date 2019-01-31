using ClickHouseClient.Tcp;

namespace ClickHouseClient
{
    internal class ProtocolFactory
    {
        public static readonly ProtocolFactory Instance = new ProtocolFactory();

        private ProtocolFactory()
        {
        }

        public IProtocol Create(ConnectionSettings connectionSettings)
        {
            return new TcpProtocol(connectionSettings);
        }
    }
}
