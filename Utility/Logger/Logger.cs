using System;
using System.Collections.Generic;
using SBWSFinanceApi.Logger.Bussiness;

namespace SBWSFinanceApi.Logger
{
    public sealed class JsonFileLogger
    {
        public void Log(Log logToSave)
        {
            // todo log into a file
            LoggerBusiness obj = new LoggerBusiness();
            obj.SaveLog(logToSave);
        }

        public List<Log> GetLogs()
        {
            LoggerBusiness obj = new LoggerBusiness();
            return obj.GetLogs();
        }
    }
}
