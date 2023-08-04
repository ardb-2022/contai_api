using System;
using System.Text.Json;

namespace SBWSDepositApi.Models
{
    public class ErrorDetails
    {
        public string ApiName { get; set; }
        public int StatusCode { get; set; }
        public string Message { get; set; }
        // public string StackTrace { get; set; }

        public string Source
        {
            get
            {
                return System.IO.Directory.GetCurrentDirectory();
            }
        }

        public string OccurTimeStamp
        {
            get
            {
                return DateTime.Now.ToString();
            }
        }

        public override string ToString()
        {
            return JsonSerializer.Serialize(this);
        }
    }
}
