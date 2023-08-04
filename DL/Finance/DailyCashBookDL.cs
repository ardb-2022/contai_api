using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class DailyCashBookDL
    {
        string _statement;
        string _statement1;
        string _statement2;
        //New Line
        internal List<tt_cash_account> PopulateDailyCashBook(p_report_param prp)
        {
            List<tt_cash_account> tcaRet=new List<tt_cash_account>();
            string _query="P_CASH_BOOK_REP";
            string _query1=" SELECT TT_CASH_ACCOUNT.SRL_NO,TT_CASH_ACCOUNT.DR_ACC_CD,TT_CASH_ACCOUNT.DR_PARTICULARS,TT_CASH_ACCOUNT.DR_AMT,"
                            +" TT_CASH_ACCOUNT.CR_ACC_CD,TT_CASH_ACCOUNT.CR_PARTICULARS,TT_CASH_ACCOUNT.CR_AMT,TT_CASH_ACCOUNT.CR_AMT_TR,TT_CASH_ACCOUNT.DR_AMT_TR"
                            +" FROM TT_CASH_ACCOUNT ";
            using (var connection = OrclDbConnection.NewConnection)
            {   
                using (var transaction = connection.BeginTransaction())
                {   
                    try{         
                            _statement = string.Format(_query);
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Int32, ParameterDirection.Input);
                                    parm1.Value = prp.ardb_cd;
                                    command.Parameters.Add(parm1);
                                    var parm2 = new OracleParameter("ad_cash_acc_cd", OracleDbType.Int32, ParameterDirection.Input);
                                    parm2.Value = prp.acc_cd;
                                    command.Parameters.Add(parm2);
                                    var parm3= new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm3.Value = prp.from_dt;
                                    command.Parameters.Add(parm3);
                                     var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm4.Value = prp.to_dt;
                                    command.Parameters.Add(parm4);
                                    var parm5 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm5.Value = prp.brn_cd;
                                    command.Parameters.Add(parm5);
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
                                                var tca = new tt_cash_account();
                                                tca.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);
                                                tca.dr_acc_cd = UtilityM.CheckNull<int>(reader["DR_ACC_CD"]);
                                                tca.dr_particulars = UtilityM.CheckNull<string>(reader["DR_PARTICULARS"]);
                                                tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                                tca.cr_acc_cd = UtilityM.CheckNull<int>(reader["CR_ACC_CD"]);
                                                tca.cr_particulars = UtilityM.CheckNull<string>(reader["CR_PARTICULARS"]);
                                                tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                                tca.cr_amt_tr = UtilityM.CheckNull<decimal>(reader["CR_AMT_TR"]);
                                                tca.dr_amt_tr = UtilityM.CheckNull<decimal>(reader["DR_AMT_TR"]);
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



        internal List<tt_cash_account> PopulateDailyCashBookConso(p_report_param prp)
        {
            List<tt_cash_account> tcaRet = new List<tt_cash_account>();
            string _query = "P_CASH_BOOK_CONSO_REP";
            string _query1 = " SELECT TT_CASH_ACCOUNT.SRL_NO,TT_CASH_ACCOUNT.DR_ACC_CD,TT_CASH_ACCOUNT.DR_PARTICULARS,TT_CASH_ACCOUNT.DR_AMT,"
                            + " TT_CASH_ACCOUNT.CR_ACC_CD,TT_CASH_ACCOUNT.CR_PARTICULARS,TT_CASH_ACCOUNT.CR_AMT,TT_CASH_ACCOUNT.CR_AMT_TR,TT_CASH_ACCOUNT.DR_AMT_TR"
                            + " FROM TT_CASH_ACCOUNT ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ad_cash_acc_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm2.Value = prp.acc_cd;
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
                                        var tca = new tt_cash_account();
                                        tca.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);
                                        tca.dr_acc_cd = UtilityM.CheckNull<int>(reader["DR_ACC_CD"]);
                                        tca.dr_particulars = UtilityM.CheckNull<string>(reader["DR_PARTICULARS"]);
                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr_acc_cd = UtilityM.CheckNull<int>(reader["CR_ACC_CD"]);
                                        tca.cr_particulars = UtilityM.CheckNull<string>(reader["CR_PARTICULARS"]);
                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.cr_amt_tr = UtilityM.CheckNull<decimal>(reader["CR_AMT_TR"]);
                                        tca.dr_amt_tr = UtilityM.CheckNull<decimal>(reader["DR_AMT_TR"]);
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



        internal List<tt_cash_account> PopulateDailyCashAccount(p_report_param prp)
        {
            List<tt_cash_account> tcaRet = new List<tt_cash_account>();
            string _query = "P_CASH_ACCOUNT_REP";
            string _query1 = " SELECT TT_CASH_ACCOUNT.SRL_NO,TT_CASH_ACCOUNT.DR_ACC_CD,TT_CASH_ACCOUNT.DR_PARTICULARS,TT_CASH_ACCOUNT.DR_AMT,"
                            + " TT_CASH_ACCOUNT.CR_ACC_CD,TT_CASH_ACCOUNT.CR_PARTICULARS,TT_CASH_ACCOUNT.CR_AMT,TT_CASH_ACCOUNT.CR_AMT_TR,TT_CASH_ACCOUNT.DR_AMT_TR"
                            + " FROM TT_CASH_ACCOUNT ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ad_cash_acc_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.acc_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm5.Value = prp.brn_cd;
                            command.Parameters.Add(parm5);
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
                                        var tca = new tt_cash_account();
                                        tca.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);
                                        tca.dr_acc_cd = UtilityM.CheckNull<int>(reader["DR_ACC_CD"]);
                                        tca.dr_particulars = UtilityM.CheckNull<string>(reader["DR_PARTICULARS"]);
                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr_acc_cd = UtilityM.CheckNull<int>(reader["CR_ACC_CD"]);
                                        tca.cr_particulars = UtilityM.CheckNull<string>(reader["CR_PARTICULARS"]);
                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.cr_amt_tr = UtilityM.CheckNull<decimal>(reader["CR_AMT_TR"]);
                                        tca.dr_amt_tr = UtilityM.CheckNull<decimal>(reader["DR_AMT_TR"]);
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



        internal List<tt_cash_account> PopulateDailyCashAccountConso(p_report_param prp)
        {
            List<tt_cash_account> tcaRet = new List<tt_cash_account>();
            string _query = "P_CASH_ACCOUNT_CONSO_REP";
            string _query1 = " SELECT TT_CASH_ACCOUNT.SRL_NO,TT_CASH_ACCOUNT.DR_ACC_CD,TT_CASH_ACCOUNT.DR_PARTICULARS,TT_CASH_ACCOUNT.DR_AMT,"
                            + " TT_CASH_ACCOUNT.CR_ACC_CD,TT_CASH_ACCOUNT.CR_PARTICULARS,TT_CASH_ACCOUNT.CR_AMT,TT_CASH_ACCOUNT.CR_AMT_TR,TT_CASH_ACCOUNT.DR_AMT_TR"
                            + " FROM TT_CASH_ACCOUNT ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ad_cash_acc_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.acc_cd;
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
                                        var tca = new tt_cash_account();
                                        tca.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);
                                        tca.dr_acc_cd = UtilityM.CheckNull<int>(reader["DR_ACC_CD"]);
                                        tca.dr_particulars = UtilityM.CheckNull<string>(reader["DR_PARTICULARS"]);
                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr_acc_cd = UtilityM.CheckNull<int>(reader["CR_ACC_CD"]);
                                        tca.cr_particulars = UtilityM.CheckNull<string>(reader["CR_PARTICULARS"]);
                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.cr_amt_tr = UtilityM.CheckNull<decimal>(reader["CR_AMT_TR"]);
                                        tca.dr_amt_tr = UtilityM.CheckNull<decimal>(reader["DR_AMT_TR"]);
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


        internal List<cashaccountDM> PopulateDailyCashAccountConsoNew(p_report_param prp)
        {
            List<cashaccountDM> tcaRet = new List<cashaccountDM>();

            string _query = " SELECT TT_CASH_ACCOUNT_BRANCHWISE.BRN_CD,TT_CASH_ACCOUNT_BRANCHWISE.SRL_NO,TT_CASH_ACCOUNT_BRANCHWISE.ACC_CD," 
                            + "TT_CASH_ACCOUNT_BRANCHWISE.PARTICULARS,TT_CASH_ACCOUNT_BRANCHWISE.DR_AMT,"
                            + " TT_CASH_ACCOUNT_BRANCHWISE.CR_AMT "
                            + " FROM TT_CASH_ACCOUNT_BRANCHWISE "
                            + " WHERE TT_CASH_ACCOUNT_BRANCHWISE.ACC_CD = {0} AND TT_CASH_ACCOUNT_BRANCHWISE.PARTICULARS = {1}"; 

            string _query1 = " Select DISTINCT ACC_CD, PARTICULARS "
                            + " FROM TT_CASH_ACCOUNT_BRANCHWISE ORDER BY ACC_CD ";

            string _query2 = "P_CASH_ACCOUNT_REP_BRANCHWISE";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement2 = string.Format(_query2);
                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                        {
                            command2.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command2.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ad_cash_acc_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.ad_cash_acc_cd;
                            command2.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command2.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command2.Parameters.Add(parm4);

                            command2.ExecuteNonQuery();
                            //transaction.Commit();
                        }

                        _statement1 = string.Format(_query1);

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        cashaccountDM tcaRet1 = new cashaccountDM();
                                        //var tca1 = new trail_type();
                                        var tca1 = new accwisecashaccount();

                                        tca1.acc_cd = UtilityM.CheckNull<int>(reader1["ACC_CD"]);
                                        tca1.acc_name = UtilityM.CheckNull<string>(reader1["PARTICULARS"]);

                                        
                                        _statement = string.Format(_query,
                                        UtilityM.CheckNull<int>(reader1["ACC_CD"]),
                                        string.Concat("'", UtilityM.CheckNull<string>(reader1["PARTICULARS"]), "'")
                                        );

                                       // tcaRet1.acc_type = tca1;


                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_cash_account();
                                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                        tca.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);
                                                        tca.dr_acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                                        tca.dr_particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);

                                                        tca1.tot_dr_amt = tca1.tot_dr_amt + UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                                        tca1.tot_cr_amt = tca1.tot_cr_amt + UtilityM.CheckNull<decimal>(reader["CR_AMT"]);


                                                        tcaRet1.cashaccount.Add(tca);

                                                        
                                                    }
                                                }
                                            }
                                        }

                                        tcaRet1.acccashaccount = tca1;

                                        tcaRet.Add(tcaRet1);
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