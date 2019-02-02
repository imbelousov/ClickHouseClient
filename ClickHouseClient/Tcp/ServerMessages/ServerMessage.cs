namespace ClickHouseClient.Tcp.ServerMessages
{
    internal abstract class ServerMessage
    {
        public static ServerMessage Read(StreamReader reader)
        {
            var code = (int) reader.ReadInteger();
            ServerMessage message;
            switch (code)
            {
                case ServerMessageCode.Hello:
                    message = new HelloServerMessage();
                    break;
                case ServerMessageCode.Data:
                    message = new DataServerMessage();
                    break;
                case ServerMessageCode.Progress:
                    message = new ProgressServerMessage();
                    break;
                case ServerMessageCode.Exception:
                    message = new ExceptionServerMessage();
                    break;
                case ServerMessageCode.EndOfStream:
                    message = new EndOfStreamServerMessage();
                    break;
                case ServerMessageCode.ProfileInfo:
                    message = new ProfileInfoServerMessage();
                    break;
                default:
                    throw new TcpProtocolException("Unsupported message code");
            }
            message.ReadImpl(reader);
            return message;
        }

        protected abstract void ReadImpl(StreamReader reader);
    }
}
