namespace ClickHouseClient.Tcp.ClientMessages
{
    internal class HelloClientMessage : ClientMessage
    {
        private readonly string _database;
        private readonly string _user;
        private readonly string _password;

        protected override int Code => ClientMessageCode.Hello;

        public HelloClientMessage(string database, string user, string password)
        {
            _database = database;
            _user = user;
            _password = password;
        }

        protected override void WriteImpl(StreamWriter writer)
        {
            WriteClientInfo(writer);
            writer.WriteString(_database);
            writer.WriteString(_user);
            writer.WriteString(_password);
        }
    }
}
