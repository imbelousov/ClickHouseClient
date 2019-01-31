namespace ClickHouseClient
{
    internal class ServerInfo
    {
        public string Name { get; set; }
        public int VersionMajor { get; set; }
        public int VersionMinor { get; set; }
        public int Build { get; set; }
        public string TimeZone { get; set; }
    }
}
