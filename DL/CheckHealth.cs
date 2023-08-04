using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using SBWSAdminApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class CheckHealth
    {
        private HttpRequest __req;
        public CheckHealth(HttpRequest req)
        {
            __req = req;
        }
        string _statement;
        internal string GetOracleHealth()
        {
            string ret = null;
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format("SELECT TO_CHAR(SYSDATE, 'DD-MON-YYYY HH:MI:SS')  AS DAY FROM DUAL");
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                ret = "ORACLE: " + UtilityM.CheckNull<string>(reader["DAY"]);
                            }
                        }
                    }
                }
            }

            return ret;
        }

        internal string GetMySqlHealth()
        {
            string ret = null;
            using (var connection = MySqlDbConnection.NewConnection)
            {
                _statement = string.Format("SELECT SYSDATE()  DAY");
                using (var command = MySqlDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                ret = "MYSQL: " + UtilityM.CheckNull<DateTime>(reader["DAY"]);
                            }
                        }
                    }
                }
            }

            return ret;
        }

        internal string GetAdminConfig()
        {
            string ret = null;
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format("SELECT TO_CHAR(SYSDATE, 'DD-MON-YYYY HH:MI:SS')  AS DAY FROM DUAL");
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                ret = "ORACLE with Config: " + UtilityM.CheckNull<string>(reader["DAY"]);
                            }
                        }
                    }
                }
            }

            return ret;
        }

        internal BankConfig GetConfigNew()
        {
            // OrclDbConnection2.Init(__req);
            return OrclDbConnection2.getConfiguration();
        }

        internal string GetHdr()
        {
            if (__req.Headers.TryGetValue("bname", out var some))
            {
                return __req.Headers["bname"];
            }
            else
            {
                return some.ToString();
            }
        }

    }


}