using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class BalanceSheet
    {
        string _statement;
        internal List<tt_balance_sheet> PopulateBalanceSheet(p_report_param prp)
        {
            List<tt_balance_sheet> tcaRet = new List<tt_balance_sheet>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_BALANCE_SHEET";
            string _query1 = "SELECT ACC_TYPE, "
                            + " ACC_CD, "
                            + " CURR_YR, "
                            + " CURR_BAL, "
                            + " PREV_YR, "
                            + " PREV_BAL, "
                            + " TYPE, "
                            + " ACC_NAME "
                            + " FROM  TT_BALANCE_SHEET ";

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
                        _statement = string.Format(_query);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);
                            command.ExecuteNonQuery();
                            //transaction.Commit();
                        }
                        _statement = string.Format(_query1);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_balance_sheet();
                                        tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        tca.curr_yr = UtilityM.CheckNull<string>(reader["CURR_YR"]);
                                        tca.curr_bal = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                        tca.prev_yr = UtilityM.CheckNull<string>(reader["PREV_YR"]);
                                        tca.prev_bal = UtilityM.CheckNull<decimal>(reader["PREV_BAL"]);
                                        tca.type = UtilityM.CheckNull<string>(reader["TYPE"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }

        internal List<tt_balance_sheet> PopulateBalanceSheetConso(p_report_param prp)
        {
            List<tt_balance_sheet> tcaRet = new List<tt_balance_sheet>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_BALANCE_SHEET_CONSO";
            string _query1 = "SELECT ACC_TYPE, "
                            + " ACC_CD, "
                            + " CURR_YR, "
                            + " CURR_BAL, "
                            + " PREV_YR, "
                            + " PREV_BAL, "
                            + " TYPE, "
                            + " ACC_NAME "
                            + " FROM  TT_BALANCE_SHEET ";

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
                        _statement = string.Format(_query);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            command.ExecuteNonQuery();
                            //transaction.Commit();
                        }
                        _statement = string.Format(_query1);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_balance_sheet();
                                        tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        tca.curr_yr = UtilityM.CheckNull<string>(reader["CURR_YR"]);
                                        tca.curr_bal = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                        tca.prev_yr = UtilityM.CheckNull<string>(reader["PREV_YR"]);
                                        tca.prev_bal = UtilityM.CheckNull<decimal>(reader["PREV_BAL"]);
                                        tca.type = UtilityM.CheckNull<string>(reader["TYPE"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }

    }
}