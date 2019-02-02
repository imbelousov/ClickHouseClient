using System;

namespace ClickHouseClient
{
    public class ClickHouseException : Exception
    {
        public int Code { get; }
        public string Name { get; }
        public new string StackTrace { get; }

        public ClickHouseException(int code, string name, string message, string stackTrace, Exception innerException)
            : base(message, innerException)
        {
            Code = code;
            Name = name;
            StackTrace = stackTrace;
        }
    }
}
