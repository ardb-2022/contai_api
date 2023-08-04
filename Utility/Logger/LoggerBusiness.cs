using System.Collections.Generic;
using SBWSFinanceApi.Logger.DAC;
namespace SBWSFinanceApi.Logger.Bussiness
{
    internal sealed class LoggerBusiness
    {
        internal void SaveLog(Log logToSave)
        {
            LoggerDac dac = new LoggerDac();
            var logs = dac.Get();
            if (null == logs)
            {
                logs = new System.Collections.Generic.List<Log>();
            }

            logs.Add(logToSave);
            dac.Save(logs);
        }

        internal List<Log> GetLogs()
        {
            LoggerDac dac = new LoggerDac();
            return dac.Get();
        }
    }
}