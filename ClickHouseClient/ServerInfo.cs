namespace ClickHouseClient
{
    /// <summary>
    /// Represents basic information about ClickHouse server
    /// </summary>
    internal class ServerInfo
    {
        /// <summary>
        /// Gets or sets a server name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets a major version number
        /// </summary>
        public int VersionMajor { get; set; }

        /// <summary>
        /// Gets or sets a minor version number
        /// </summary>
        public int VersionMinor { get; set; }

        /// <summary>
        /// Gets or sets a build number
        /// </summary>
        public int Build { get; set; }

        /// <summary>
        /// Gets or sets a time zone that specified on the server machine
        /// </summary>
        public string TimeZone { get; set; }
    }
}
