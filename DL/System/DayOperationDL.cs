using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class DayOperationDL
    {
        string _statement;

internal p_gen_param W_DAY_CLOSE(p_gen_param prp)
        {
            string errMsg = "";
            int    retflg = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "W_DAY_CLOSE";
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
                        _statement = string.Format(_query );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                           command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("AS_ARDB_CD", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("AS_BRN_CD", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                                                               
                            var parm3 = new OracleParameter("gs_user_type", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.gs_user_type;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("gs_user_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.gs_user_id;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ad_prn_amt", OracleDbType.Decimal, ParameterDirection.Input);
                            parm5.Value = prp.ad_prn_amt;
                            command.Parameters.Add(parm5);

                            var parm6 = new OracleParameter("O_MSG", OracleDbType.Varchar2, ParameterDirection.Output);
                            parm6.Size=2000;
                            parm6.Value=errMsg;
                            command.Parameters.Add(parm6);

                            var parm7 = new OracleParameter("O_FLG", OracleDbType.Int32, ParameterDirection.Output);
                            parm7.Value=retflg;
                            command.Parameters.Add(parm7);
                            
                            
                            var reader = command.ExecuteNonQuery();
                            prp.output=  command.Parameters["O_MSG"].Value.ToString();
                            prp.flag=  command.Parameters["O_FLG"].Value.ToString(); 

                                    
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                    }
                    transaction.Commit();
                }
            }
            
            return prp;
        }

        internal p_gen_param W_DAY_OPEN(p_gen_param prp)
        {
            string errMsg = "";
            int    retflg = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "W_DAY_OPEN";
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
                        _statement = string.Format(_query );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                           command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("adt_tmp_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.adt_trans_dt;
                            command.Parameters.Add(parm2);


                            var parm4 = new OracleParameter("gs_user_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.gs_user_id;
                            command.Parameters.Add(parm4);


                            var parm6 = new OracleParameter("O_MSG", OracleDbType.Varchar2, ParameterDirection.Output);
                            parm6.Size=2000;
                            parm6.Value=errMsg;
                            command.Parameters.Add(parm6);

                            var parm7 = new OracleParameter("O_FLG", OracleDbType.Int32, ParameterDirection.Output);
                            parm7.Value=retflg;
                            command.Parameters.Add(parm7);
                            
                            
                            var reader = command.ExecuteNonQuery();
                            prp.output=  command.Parameters["O_MSG"].Value.ToString();
                            prp.flag=  command.Parameters["O_FLG"].Value.ToString();  
                                    
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                    }
                    transaction.Commit();
                }
            }
            return prp;
        }

           /* internal List<sd_day_operation> GetDayOperation(sd_day_operation pmc)
            {
                List<sd_day_operation> custRets = new List<sd_day_operation>();
                string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'";
                string _query = "SELECT OPERATION_DT, BRN_CD, CLS_BAL, CLS_FLAG,CLOSED_BY, CLOSED_DT  "
             + "  FROM SD_DAY_OPERATION  "
             + " WHERE    ARDB_CD = {0} AND OPERATION_DT = {1} ";
                using (var connection = OrclDbConnection.NewConnection)
                {
                    using (var command = OrclDbConnection.Command(connection, _alter))
                    {
                        command.ExecuteNonQuery();
                    }

                    _statement = string.Format(_query,
                                               string.IsNullOrWhiteSpace(pmc.ardb_cd) ? "ardb_cd" : string.Concat("'", pmc.ardb_cd, "'"),
                                               //pmc.operation_dt != null ? string.Concat("'", pmc.operation_dt.Value.ToString("dd/MM/yyyy"), "'") : "OPERATION_DT"
                                                // string.IsNullOrWhiteSpace(pmc.operation_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", pmc.operation_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                                                // string.IsNullOrWhiteSpace(pmc.operation_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", pmc.operation_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                                                string.Concat("'", pmc.operation_dt.ToString("dd/MM/yyyy"), "'")
                                                );
                    using (var command = OrclDbConnection.Command(connection, _statement))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    var mc = new sd_day_operation();
                                    mc.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    mc.operation_dt = UtilityM.CheckNull<DateTime>(reader["OPERATION_DT"]);
                                    mc.cls_bal = UtilityM.CheckNull<decimal>(reader["CLS_BAL"]);
                                    mc.cls_flg = UtilityM.CheckNull<string>(reader["CLS_FLAG"]);
                                    mc.closed_by = UtilityM.CheckNull<string>(reader["CLOSED_BY"]);
                                    mc.closed_dt = UtilityM.CheckNull<DateTime>(reader["CLOSED_DT"]);

                                    custRets.Add(mc);
                                }
                            }
                        }
                    }
                }
                return custRets;
            }*/


        internal List<sd_day_operation> GetDayOperation(sd_day_operation pmc)
        {
            List<sd_day_operation> custRets = new List<sd_day_operation>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = "SELECT OPERATION_DT, BRN_CD, CLS_BAL, CLS_FLAG,CLOSED_BY, CLOSED_DT  "
             + "  FROM SD_DAY_OPERATION  "
             + " WHERE    ARDB_CD = {0} AND OPERATION_DT = (SELECT MAX(OPERATION_DT) FROM SD_DAY_OPERATION WHERE  ARDB_CD = {1})  ";



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
                                      string.IsNullOrWhiteSpace(pmc.ardb_cd) ? "ardb_cd" : string.Concat("'", pmc.ardb_cd, "'"),
                                      string.IsNullOrWhiteSpace(pmc.ardb_cd) ? "ardb_cd" : string.Concat("'", pmc.ardb_cd, "'")
                                      );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var mc = new sd_day_operation();
                                        mc.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        mc.operation_dt = UtilityM.CheckNull<DateTime>(reader["OPERATION_DT"]);
                                        mc.cls_bal = UtilityM.CheckNull<decimal>(reader["CLS_BAL"]);
                                        mc.cls_flg = UtilityM.CheckNull<string>(reader["CLS_FLAG"]);
                                        mc.closed_by = UtilityM.CheckNull<string>(reader["CLOSED_BY"]);
                                        mc.closed_dt = UtilityM.CheckNull<DateTime>(reader["CLOSED_DT"]);

                                        custRets.Add(mc);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        custRets = null;
                    }
                }
            }
            return custRets;
        }


        internal int YearOpen(p_gen_param prp)
        {
            int _ret = 0;

            string _query = " UPDATE SM_PARAMETER SET PARAM_VALUE = {0} WHERE PARAM_CD = '207'  ";

            string _query1 = " UPDATE SM_PARAMETER SET PARAM_VALUE =  to_char(to_date(substr({0},1,10),'dd/mm/yyyy'),'dd/mm/yyyy') WHERE PARAM_CD = '210'  ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                             string.Concat("'", prp.as_acc_num, "'")                                          
                                              );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            //transaction.Commit();
                            _ret = 0;
                        }

                        _statement = string.Format(_query1,
                                             string.Concat("'", prp.adt_trans_dt, "'")
                                              );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            _ret = 0;
                        }
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _ret = -1;
                    }
                }
            }
            return _ret;
        }

    }
}