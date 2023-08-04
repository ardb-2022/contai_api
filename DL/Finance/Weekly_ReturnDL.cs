using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class Weekly_ReturnDL
    {
        string _statement;
        internal List<weekly_return> PopulateWeeklyReturn(p_report_param prp)
        {
            List<weekly_return> tcaRet = new List<weekly_return>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_WEEKLY_RETURN_NEW";
            string _query1 = "SELECT TT_WEEKLY_RETURN.ARDB_CD,"
                          + " TT_WEEKLY_RETURN.BRN_CD,"
                          + " TT_WEEKLY_RETURN.SRL_NO,"
                          + " TT_WEEKLY_RETURN.DR_PARTICULARS,"
                          + " TT_WEEKLY_RETURN.DR_AMT,"
                          + " TT_WEEKLY_RETURN.CR_PARTICULARS,"
                          + " TT_WEEKLY_RETURN.CR_AMT,"
                          + " TT_WEEKLY_RETURN.DR_ACC_CD,"
                          + " TT_WEEKLY_RETURN.CR_ACC_CD"
                          + " FROM TT_WEEKLY_RETURN ORDER BY  TT_WEEKLY_RETURN.SRL_NO ";
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
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.to_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.brn_cd;
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
                                        var tca = new weekly_return();
                                        tca.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        tca.srl_no = UtilityM.CheckNull<Int32>(reader["SRL_NO"]);
                                        tca.dr_particulars = UtilityM.CheckNull<string>(reader["DR_PARTICULARS"]);
                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr_particulars = UtilityM.CheckNull<string>(reader["CR_PARTICULARS"]);
                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.dr_acc_cd = UtilityM.CheckNull<Int64>(reader["DR_ACC_CD"]);
                                        tca.cr_acc_cd = UtilityM.CheckNull<Int64>(reader["CR_ACC_CD"]);                                        
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



