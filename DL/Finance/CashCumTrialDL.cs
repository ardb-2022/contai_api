using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class CashCumTrialDL
    {
        string _statement;
        internal List<tt_cash_cum_trial> PopulateCashCumTrial(p_report_param prp)
        {
            List<tt_cash_cum_trial> tcaRet=new List<tt_cash_cum_trial>();
            string _alter="ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query="P_CASH_CUM_TRAIL";
            string _query1="SELECT TT_CASH_CUM_TRAIL.ACC_CD,"
                          +" TT_CASH_CUM_TRAIL.ACC_NAME,"
                          +" TT_CASH_CUM_TRAIL.ACC_TYPE,"
                          +" TT_CASH_CUM_TRAIL.OPNG_DEBIT,"
                          +" TT_CASH_CUM_TRAIL.OPNG_CREDIT,"
                          +" TT_CASH_CUM_TRAIL.DEBIT,"
                          +" TT_CASH_CUM_TRAIL.CREDIT,"
                          +" TT_CASH_CUM_TRAIL.CLOS_DEBIT,"
                          +" TT_CASH_CUM_TRAIL.CLOS_CREDIT"
                          +" FROM TT_CASH_CUM_TRAIL";
            using (var connection = OrclDbConnection.NewConnection)
            {   
                using (var transaction = connection.BeginTransaction())
                {   
                    try{    
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
                                    var parm3 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm3.Value = prp.from_dt;
                                    command.Parameters.Add(parm3);
                                     var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm4.Value = prp.to_dt;
                                    command.Parameters.Add(parm4);
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
                                                var tca = new tt_cash_cum_trial();
                                                tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                                tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                                tca.opng_dr = UtilityM.CheckNull<decimal>(reader["OPNG_DEBIT"]);
                                                tca.opng_cr = UtilityM.CheckNull<decimal>(reader["OPNG_CREDIT"]);
                                                tca.dr = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                                tca.cr = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                                tca.clos_dr = UtilityM.CheckNull<decimal>(reader["CLOS_DEBIT"]);
                                                tca.clos_cr = UtilityM.CheckNull<decimal>(reader["CLOS_CREDIT"]);
                                                tcaRet.Add(tca);
                                        }
                                    }
                                }
                        }
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            tcaRet=null;
                        }
                }            
            }
            return tcaRet;
    }



        internal List<tt_cash_cum_trial> PopulateCashCumTrialConso(p_report_param prp)
        {
            List<tt_cash_cum_trial> tcaRet = new List<tt_cash_cum_trial>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_CASH_CUM_TRAIL_CONSO";
            string _query1 = "SELECT TT_CASH_CUM_TRAIL.ACC_CD,"
                          + " TT_CASH_CUM_TRAIL.ACC_NAME,"
                          + " TT_CASH_CUM_TRAIL.ACC_TYPE,"
                          + " TT_CASH_CUM_TRAIL.OPNG_DEBIT,"
                          + " TT_CASH_CUM_TRAIL.OPNG_CREDIT,"
                          + " TT_CASH_CUM_TRAIL.DEBIT,"
                          + " TT_CASH_CUM_TRAIL.CREDIT,"
                          + " TT_CASH_CUM_TRAIL.CLOS_DEBIT,"
                          + " TT_CASH_CUM_TRAIL.CLOS_CREDIT"
                          + " FROM TT_CASH_CUM_TRAIL";
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
                            var parm2 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.to_dt;
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
                                        var tca = new tt_cash_cum_trial();
                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                        tca.opng_dr = UtilityM.CheckNull<decimal>(reader["OPNG_DEBIT"]);
                                        tca.opng_cr = UtilityM.CheckNull<decimal>(reader["OPNG_CREDIT"]);
                                        tca.dr = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                        tca.cr = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                        tca.clos_dr = UtilityM.CheckNull<decimal>(reader["CLOS_DEBIT"]);
                                        tca.clos_cr = UtilityM.CheckNull<decimal>(reader["CLOS_CREDIT"]);
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


        internal List<tt_cash_cum_trial> PopulateCashCumTrialConsoNew(p_report_param prp)
        {
            List<tt_cash_cum_trial> tcaRet = new List<tt_cash_cum_trial>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_TRIAL_BOOK";
            string _query1 = "SELECT TT_TRIAL_BOOK.ACC_CD,"
                          + " TT_TRIAL_BOOK.ACC_NAME,"
                          + " TT_TRIAL_BOOK.TRIAL_TYPE,"
                          + " TT_TRIAL_BOOK.OPENING_BAL,"
                          + " TT_TRIAL_BOOK.DR_AMT,"
                          + " TT_TRIAL_BOOK.CR_AMT,"
                          + " TT_TRIAL_BOOK.CLOSING_BAL"
                          + " FROM TT_TRIAL_BOOK ORDER BY TT_TRIAL_BOOK.ACC_CD";
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
                            var parm2 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.to_dt;
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
                                        var tca = new tt_cash_cum_trial();
                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        tca.acc_type = UtilityM.CheckNull<string>(reader["TRIAL_TYPE"]);
                                        tca.opng_dr = UtilityM.CheckNull<decimal>(reader["OPENING_BAL"]);
                                        tca.dr = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.clos_dr = UtilityM.CheckNull<decimal>(reader["CLOSING_BAL"]);
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