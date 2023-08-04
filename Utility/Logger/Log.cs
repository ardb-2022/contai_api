using System;
namespace SBWSFinanceApi.Logger
{
    public sealed class Log
    {
        public string ApiName { get; set; }
        public string Message { get; set; }
        public string Source
        {
            get
            {
                return System.IO.Directory.GetCurrentDirectory();
            }
        }
        public int Counter { get; }
        public string OccurTimeStamp { get; set; }
        // public string OccurTimeStamp
        // {
        //     get
        //     {
        //         return DateTime.Now.ToString();
        //     }
        // }
        // public Severity Severity { get; }


    }

    // public enum Severity
    // {
    //     Warning = 0,
    //     Error = 1,
    //     Success = 2
    // }
}