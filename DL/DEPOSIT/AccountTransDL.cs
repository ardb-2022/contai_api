using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class AccountTransDL
    {
        string _statement;

        internal decimal GetShadowBalance(tm_deposit td)
        {
            decimal amount = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT W_F_GetShadowBalance({0},{1},{2}) AMOUNT FROM DUAL";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }
                        _statement = string.Format(_query,
                                         string.Concat("'", td.ardb_cd, "'"),
                                         string.Concat("'", td.acc_type_cd, "'"),
                                         string.Concat("'", td.acc_num, "'")
                                        );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        amount = 0;
                    }
                }
            }
            return amount;
        }
      

    }
}