﻿using System;
using System.Text;

namespace ClickHouseClient
{
    /// <summary>
    /// Represents a parsed connection string
    /// </summary>
    internal class ConnectionSettings
    {
        /// <summary>
        /// Gets or sets a DNS name or an IP address of the server
        /// </summary>
        public string Host { get; set; } = "localhost";

        /// <summary>
        /// Gets or sets a port
        /// </summary>
        public int Port { get; set; } = 9000;

        /// <summary>
        /// Gets or sets a user
        /// </summary>
        public string User { get; set; } = "default";

        /// <summary>
        /// Gets or sets a password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Gets or sets a default database
        /// </summary>
        public string Database { get; set; } = "default";

        /// <summary>
        /// Gets or sets a connection timeout
        /// </summary>
        public int Timeout { get; set; } = 1000 * 10;

        public ConnectionSettings(string connectionString)
        {
            if (!string.IsNullOrWhiteSpace(connectionString))
                Parse(connectionString);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            PropertyToString(sb, "Host", Host);
            PropertyToString(sb, "Port", Port);
            PropertyToString(sb, "User", User);
            if (!string.IsNullOrEmpty(Password))
                PropertyToString(sb, "Password", Password);
            PropertyToString(sb, "Database", Database);
            PropertyToString(sb, "Timeout", Timeout);
            return sb.ToString();
        }

        private void Parse(string connectionString)
        {
            var isKey = true;
            var start = 0;
            var length = 0;
            var key = null as string;
            for (var i = 0; i < connectionString.Length + 1; i++)
            {
                var isEnd = i == connectionString.Length;
                var c = !isEnd ? connectionString[i] : (char) 0;
                if (isKey)
                {
                    if (isEnd)
                    {
                        if (length > 0 && TrimSubstring(connectionString, start, length) != string.Empty)
                            throw new ConnectionStringException("Unexpected end of connection string");
                        break;
                    }
                    if (c == '=')
                    {
                        isKey = false;
                        key = TrimSubstring(connectionString, start, length);
                        if (key.Length == 0)
                            throw new ConnectionStringException("Empty key is not allowed");
                        start += length + 1;
                        length = 0;
                    }
                    else if (c == ';')
                        throw new ConnectionStringException($"Unexpected symbol \";\" at position {start + length}");
                    else
                        length++;
                }
                else
                {
                    if (c == ';' || isEnd)
                    {
                        isKey = true;
                        var rawValue = TrimSubstring(connectionString, start, length);
                        SetProperty(key, rawValue);
                        start += length + 1;
                        length = 0;
                    }
                    else
                        length++;
                }
            }
        }

        private void PropertyToString<T>(StringBuilder sb, string key, T value)
        {
            sb.Append(key);
            sb.Append("=");
            sb.Append(value);
            sb.Append(";");
        }

        private void SetProperty(string key, string rawValue)
        {
            switch (key.ToLower())
            {
                case "host":
                    if (string.IsNullOrEmpty(rawValue))
                        throw new ConnectionStringException("Empty host is not allowed");
                    Host = rawValue;
                    break;
                case "port":
                    Port = ToInt(key, rawValue);
                    break;
                case "user":
                    if (string.IsNullOrEmpty(rawValue))
                        throw new ConnectionStringException("Empty user is not allowed");
                    User = rawValue;
                    break;
                case "password":
                    Password = rawValue;
                    break;
                case "database":
                    if (string.IsNullOrEmpty(rawValue))
                        throw new ConnectionStringException("Empty database is not allowed");
                    Database = rawValue;
                    break;
                case "timeout":
                    Timeout = ToInt(key, rawValue);
                    break;
                default:
                    throw new ConnectionStringException($"Unexpected key: {key}");
            }
        }

        private string TrimSubstring(string s, int startIndex, int length)
        {
            var trimmedStartIndex = startIndex;
            var trimmedLength = length;
            while (trimmedStartIndex < startIndex + length && char.IsWhiteSpace(s[trimmedStartIndex]))
            {
                trimmedStartIndex++;
                trimmedLength--;
            }
            while (trimmedLength > 0 && char.IsWhiteSpace(s[trimmedStartIndex + trimmedLength - 1]))
            {
                trimmedLength--;
            }
            return s.Substring(trimmedStartIndex, trimmedLength);
        }

        private int ToInt(string key, string rawValue)
        {
            if (!int.TryParse(rawValue, out var value))
                throw new ConnectionStringException($"Integer value expected for key {key}");
            return value;
        }
    }

    public class ConnectionStringException : Exception
    {
        public ConnectionStringException(string message)
            : base(message)
        {
        }
    }
}
