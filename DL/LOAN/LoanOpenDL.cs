using System;
using System.Text.Json;
using System.Collections.Generic;
using System.IO;
using SBWSFinanceApi.Models;
using System.Data.Common;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;
using SBWSDepositApi.Models;
using SBWSDepositApi.Deposit;
using Oracle.ManagedDataAccess.Client;
using System.Data;



namespace SBWSFinanceApi.DL
{
    public class LoanOpenDL
    {
        string _statement;
        AccountOpenDL _dac = new AccountOpenDL();

        internal p_loan_param CalculateLoanInterest(p_loan_param prp)
        {
            p_loan_param tcaRet = new p_loan_param();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = "P_LOAN_DAY_PRODUCT";
            string _query2 = "P_CAL_LOAN_INTT";
            string _query3 = "p_recovery";
            string _query4 = "Select Nvl(curr_prn, 0) curr_prn,"
				            +" Nvl(ovd_prn, 0) ovd_prn,"
				            +" Nvl(curr_intt, 0) curr_intt,"
				            +" Nvl(ovd_intt, 0) ovd_intt,"
                            + " Nvl(penal_intt, 0) penal_intt"
                            + " From   tm_loan_all where ardb_cd = {0} and loan_id = {1} and del_flag= 'N' ";

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
                        _statement = string.Format(_query1);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.loan_id;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.intt_dt;
                            command.Parameters.Add(parm3);
                            command.ExecuteNonQuery();
                        }
                        _statement = string.Format(_query2);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.loan_id;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.intt_dt;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.brn_cd;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("as_user", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm5.Value = prp.gs_user_id;
                            command.Parameters.Add(parm5);
                            command.ExecuteNonQuery();

                        }
                         if (prp.commit_roll_flag == 2)
                         {
                        _statement = string.Format(_query3);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.loan_id;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("ad_recov_amt", OracleDbType.Decimal, ParameterDirection.Input);
                            parm3.Value = prp.recov_amt;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("as_curr_intt_rate", OracleDbType.Decimal, ParameterDirection.Input);
                            parm4.Value = prp.curr_intt_rate;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("ad_curr_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm5);
                            var parm6 = new OracleParameter("ad_ovd_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm6);
                            var parm7 = new OracleParameter("ad_curr_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm7);
                            var parm8 = new OracleParameter("ad_ovd_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm8);
                            var parm9 = new OracleParameter("ad_penal_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm9);
                            var parm10 = new OracleParameter("ad_adv_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm10);
                                command.ExecuteNonQuery();
                            tcaRet.curr_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm5.Value.ToString()));
                            tcaRet.ovd_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm6.Value.ToString()));
                            tcaRet.curr_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm7.Value.ToString()));
                            tcaRet.ovd_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm8.Value.ToString()));
                            tcaRet.penal_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm9.Value.ToString()));
                            tcaRet.adv_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm10.Value.ToString()));
                            }
                         }
                         else
                         {
                        _statement = string.Format(_query4,
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.loan_id, "'"));
                         using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        tcaRet.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["curr_prn"]);
                                        tcaRet.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["ovd_prn"]);
                                        tcaRet.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["curr_intt"]);
                                        tcaRet.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["ovd_intt"]);
                                        tcaRet.penal_intt_recov= UtilityM.CheckNull<decimal>(reader["penal_intt"]);
                                        }
                                }
                            }
                        }
                        }
                        if (prp.commit_roll_flag == 1)
                            transaction.Commit();
                        else
                            transaction.Rollback();


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



        internal p_loan_param CalculateLoanInterestYearend(p_loan_param prp)
        {
            p_loan_param tcaRet = new p_loan_param();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            //string _query1 = "P_LOAN_DAY_PRODUCT";
            string _query2 = "P_CAL_LOAN_INTT_YEAREND";
            string _query3 = "p_recovery";
            string _query4 = "Select Nvl(curr_prn, 0) curr_prn,"
                            + " Nvl(ovd_prn, 0) ovd_prn,"
                            + " Nvl(curr_intt, 0) curr_intt,"
                            + " Nvl(ovd_intt, 0) ovd_intt,"
                            + " Nvl(penal_intt, 0) penal_intt"
                            + " From   tm_loan_all where ardb_cd = {0} and loan_id = {1} and del_flag= 'N' ";

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
                       /* _statement = string.Format(_query1);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.loan_id;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.intt_dt;
                            command.Parameters.Add(parm3);
                            command.ExecuteNonQuery();
                        }*/
                        _statement = string.Format(_query2);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.loan_id;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.intt_dt;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.brn_cd;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("as_user", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm5.Value = prp.gs_user_id;
                            command.Parameters.Add(parm5);
                            command.ExecuteNonQuery();

                        }
                        if (prp.commit_roll_flag == 2)
                        {
                            _statement = string.Format(_query3);
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm1.Value = prp.ardb_cd;
                                command.Parameters.Add(parm1);
                                var parm2 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm2.Value = prp.loan_id;
                                command.Parameters.Add(parm2);
                                var parm3 = new OracleParameter("ad_recov_amt", OracleDbType.Decimal, ParameterDirection.Input);
                                parm3.Value = prp.recov_amt;
                                command.Parameters.Add(parm3);
                                var parm4 = new OracleParameter("as_curr_intt_rate", OracleDbType.Decimal, ParameterDirection.Input);
                                parm4.Value = prp.curr_intt_rate;
                                command.Parameters.Add(parm4);
                                var parm5 = new OracleParameter("ad_curr_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm5);
                                var parm6 = new OracleParameter("ad_ovd_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm6);
                                var parm7 = new OracleParameter("ad_curr_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm7);
                                var parm8 = new OracleParameter("ad_ovd_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm8);
                                var parm9 = new OracleParameter("ad_penal_intt_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm9);
                                var parm10 = new OracleParameter("ad_adv_prn_recov", OracleDbType.Decimal, ParameterDirection.Output);
                                command.Parameters.Add(parm10);
                                command.ExecuteNonQuery();
                                tcaRet.curr_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm5.Value.ToString()));
                                tcaRet.ovd_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm6.Value.ToString()));
                                tcaRet.curr_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm7.Value.ToString()));
                                tcaRet.ovd_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm8.Value.ToString()));
                                tcaRet.penal_intt_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm9.Value.ToString()));
                                tcaRet.adv_prn_recov = UtilityM.CheckNull<Decimal>(Convert.ToDecimal(parm10.Value.ToString()));
                            }
                        }
                        else
                        {
                            _statement = string.Format(_query4,
                                                        string.Concat("'", prp.ardb_cd, "'"),
                                                        string.Concat("'", prp.loan_id, "'"));
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        while (reader.Read())
                                        {
                                            tcaRet.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["curr_prn"]);
                                            tcaRet.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["ovd_prn"]);
                                            tcaRet.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["curr_intt"]);
                                            tcaRet.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["ovd_intt"]);
                                            tcaRet.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["penal_intt"]);
                                        }
                                    }
                                }
                            }
                        }
                        if (prp.commit_roll_flag == 1)
                            transaction.Commit();
                        else
                            transaction.Rollback();


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




        internal List<p_loan_param> CalculateLoanAccWiseInterest(List<p_loan_param> prp)
        {
            List<p_loan_param> tcaRetList = new List<p_loan_param>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = "p_calc_loanaccwise_intt";


            using (var connection = OrclDbConnection.NewConnection)
            {
                for (int i = 0; i < prp.Count; i++)
                {
                    var tcaRet = new p_loan_param();
                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            using (var command = OrclDbConnection.Command(connection, _alter))
                            {
                                command.ExecuteNonQuery();
                            }
                            _statement = string.Format(_query1);
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm1.Value = prp[i].ardb_cd;
                                command.Parameters.Add(parm1);
                                var parm2 = new OracleParameter("ad_acc_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm2.Value = prp[i].acc_cd;
                                command.Parameters.Add(parm2);
                                var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                                parm3.Value = prp[i].intt_dt;
                                command.Parameters.Add(parm3);
                                command.ExecuteNonQuery();
                            }
                            transaction.Commit();
                            tcaRet.acc_cd = prp[i].acc_cd;
                            tcaRet.status = 0;
                            tcaRetList.Add(tcaRet);
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            tcaRet.acc_cd = prp[i].acc_cd;
                            tcaRet.status = 1;
                            tcaRetList.Add(tcaRet);
                        }
                    }
                }
            }
            return tcaRetList;
        }

        internal p_loan_param PopulateCropAmtDueDt(p_loan_param prp)
        {
            p_loan_param tcaRet = new p_loan_param();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = "W_Pop_CropAmtDueDt";


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
                        _statement = string.Format(_query1);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("ls_crop_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.crop_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ls_party_cd", OracleDbType.Int64, ParameterDirection.Input);
                            parm2.Value = prp.cust_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("ad_ldt_due_dt", OracleDbType.Date, ParameterDirection.Output);
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("ad_sanc_amt", OracleDbType.Decimal, ParameterDirection.Output);
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("ad_status", OracleDbType.Int64, ParameterDirection.Output);
                            command.Parameters.Add(parm5);
                            command.ExecuteNonQuery();
                            tcaRet.due_dt = (parm3.Status == OracleParameterStatus.NullFetched) ? (DateTime?)null : Convert.ToDateTime(parm3.Value.ToString());
                            tcaRet.recov_amt = (parm4.Status == OracleParameterStatus.NullFetched) ? (Int32)0 : Convert.ToInt32(parm4.Value.ToString());
                            tcaRet.status = (parm5.Status == OracleParameterStatus.NullFetched) ? (Int32)0 : Convert.ToInt32(parm5.Value.ToString());
                        }
                        if (prp.commit_roll_flag == 1)
                            transaction.Commit();
                        else
                            transaction.Rollback();


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

        internal decimal F_GET_EFF_INTT_RT(p_loan_param prp)
        {
            decimal intt_rt = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT W_F_GET_EFF_INTT_RT({0},{1},{2}) INTT_RT FROM DUAL";
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
                                         string.Concat("'", prp.loan_id, "'"),
                                         string.Concat("'", prp.acc_type_cd, "'"),
                                         string.IsNullOrWhiteSpace(prp.intt_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", prp.intt_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                                        );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        intt_rt = 0;
                    }
                }
            }
            return intt_rt;
        }

        internal string PopulateLoanAccountNumber(p_gen_param prp)
        {
            string accNum = "";

            string _query = "Select lpad(nvl(max(to_number(substr(loan_id,4))) + 1, 1),7,'0') ACC_NUM "
                           + " From  TM_LOAN_ALL "
                           + " Where ardb_cd={0} and brn_cd = {1} ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement = string.Format(_query,
                                        string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.brn_cd, "'")
                                        );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        accNum = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        accNum = "";
                    }
                }
            }
            return prp.brn_cd + accNum;
        }
        internal LoanOpenDM GetLoanData(tm_loan_all loan)
        {
            LoanOpenDM AccOpenDMRet = new LoanOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        tm_deposit td = new tm_deposit();
                        td.brn_cd = loan.brn_cd;
                        td.acc_num = loan.loan_id;
                        td.acc_type_cd = loan.acc_cd;
                        AccOpenDMRet.tmloanall = GetLoanAll(connection, loan);
                        AccOpenDMRet.tmguaranter = GetGuaranter(connection, loan.ardb_cd, loan.loan_id);
                        AccOpenDMRet.tmlaonsanction = GetLoanSanction(connection, loan.ardb_cd,loan.loan_id);
                        AccOpenDMRet.tdaccholder = GetAccholderTemp(connection, loan.ardb_cd, loan.brn_cd, loan.loan_id, loan.acc_cd);
                        AccOpenDMRet.tmlaonsanctiondtls = GetLoanSanctionDtls(connection, loan.ardb_cd, loan.loan_id);
                        AccOpenDMRet.tddeftrans = _dac.GetDepTrans(connection, td);
                        if (!String.IsNullOrWhiteSpace(AccOpenDMRet.tddeftrans.trans_cd.ToString()) && AccOpenDMRet.tddeftrans.trans_cd > 0)
                        {
                            td.trans_cd = AccOpenDMRet.tddeftrans.trans_cd;
                            td.trans_dt = AccOpenDMRet.tddeftrans.trans_dt;
                            AccOpenDMRet.tmdenominationtrans = _dac.GetDenominationDtls(connection, td);
                            AccOpenDMRet.tmtransfer = _dac.GetTransfer(connection, td);
                            AccOpenDMRet.tddeftranstrf = _dac.GetDepTransTrf(connection, td);
                        }

                        AccOpenDMRet.tdloansancsetlist = GetTdLoanSanctionDtls(connection, loan.ardb_cd, loan.loan_id , AccOpenDMRet.tmloanall.acc_cd);

                        return AccOpenDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }
        internal string InsertLoanTransactionData(LoanOpenDM acc)
        {
            string _section = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section = "GetTransCDMaxId";
                        int maxTransCD = _dac.GetTransCDMaxId(connection, acc.tddeftrans);
                        //_section = "InsertDenominationDtls";
                        //if (acc.tmdenominationtrans.Count > 0)
                        //    _dac.InsertDenominationDtls(connection, acc.tmdenominationtrans, maxTransCD);
                        _section = "InsertTransfer";
                        if (acc.tmtransfer.Count > 0)
                            _dac.InsertTransfer(connection, acc.tmtransfer, maxTransCD);
                        _section = "InsertDepTransTrf";
                        if (acc.tddeftranstrf.Count > 0)
                            _dac.InsertDepTransTrf(connection, acc.tddeftranstrf, maxTransCD);
                        _section = "InsertDepTrans";
                        if (!String.IsNullOrWhiteSpace(maxTransCD.ToString()) && maxTransCD != 0)
                            _dac.InsertDepTrans(connection, acc.tddeftrans, maxTransCD);
                        transaction.Commit();
                        return maxTransCD.ToString();
                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return _section + " : " + ex.Message;
                    }

                }
            }
        }



        internal int InsertSubsidyData(tm_subsidy prp)
        {
            
            int _ret = 0;

            tm_subsidy prprets = new tm_subsidy();

            string _query = "INSERT INTO TM_SUBSIDY (ARDB_CD,BRN_CD,LOAN_CASE_NO,LOAN_ID,START_DT,DISTRIBUTION_DT,SUBSIDY_AMT,SUBSIDY_TYPE,MODIFIED_BY,MODIFIED_DT,SUBSIDY,DEL_FLAG)"
                        + " VALUES ({0},{1},{2},{3},to_date({4},'dd-mm-yyyy'),to_date({5},'dd-mm-yyyy'),{6},{7},{8},Sysdate,{9},'N') ";

            using (var connection = OrclDbConnection.NewConnection)
            {

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                                 _statement = string.Format(_query,
                                         string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.brn_cd, "'"),
                                         string.Concat("'",prp.loan_acc_no,"'"),
                                         string.Concat("'",prp.loan_id,"'"),
                                         string.Concat("'",prp.start_dt.Value.ToString("dd/MM/yyyy"),"'"),
                                         string.Concat("'", prp.distribution_dt.Value.ToString("dd/MM/yyyy"),"'"),
                                         string.Concat("'", prp.subsidy_amt, "'"),
                                         string.Concat("'", prp.subsidy_type, "'"),
                                         string.Concat("'", prp.modified_by, "'"),
                                         string.Concat("'", prp.subsidy,"'")                                         
                                         );

                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();
                                transaction.Commit();
                                _ret = 0;
                            }
                      
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


        internal int UpdateSubsidyData(tm_subsidy prp)
        {
            int _ret = 0;

            string _query = "UPDATE TM_SUBSIDY SET "
         + " LOAN_CASE_NO     =NVL({0},LOAN_CASE_NO       ),"
         + " START_DT         =NVL(to_date('{1}','dd-mm-yyyy'),START_DT       ),"
         + " DISTRIBUTION_DT  =NVL(to_date('{2}','dd-mm-yyyy'),DISTRIBUTION_DT       ),"
         + " SUBSIDY_AMT      =NVL({3},SUBSIDY_AMT    ),"
         + " MODIFIED_BY      =NVL({4},MODIFIED_BY        ),"
         + " MODIFIED_DT      =Sysdate,"
         + " SUBSIDY_TYPE      =NVL({5},SUBSIDY_TYPE        ),"
         + " SUBSIDY          =NVL({6},SUBSIDY )"
         + " WHERE (ARDB_CD = {7}) AND (BRN_CD = {8}) AND "
         + " (LOAN_ID = {9} ) AND  "
         + " (DEL_FLAG = 'N') ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                            _statement = string.Format(_query,
                                         string.Concat(prp.loan_acc_no),
                                         string.Concat(prp.start_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat(prp.distribution_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", prp.subsidy_amt, "'"),
                                         string.Concat("'", prp.modified_by, "'"),
                                         string.Concat("'", prp.subsidy_type, "'"),
                                         string.Concat("'", prp.subsidy, "'"),
                                         string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.brn_cd, "'"),
                                         string.Concat("'", prp.loan_id, "'")                                        
                                         );
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();
                                transaction.Commit();
                                _ret = 0;
                            }                        
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


        internal int DeleteSubsidyData(tm_subsidy prp)
        {
            int _ret = 0;

            string _query = "UPDATE TM_SUBSIDY SET "
         + " DEL_FLAG     = 'Y'  "
         + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
         + " (LOAN_ID = {2} ) AND  "
         + " (DEL_FLAG = 'N') ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                     string.Concat("'", prp.ardb_cd, "'"),
                                     string.Concat("'", prp.brn_cd, "'"),
                                     string.Concat("'", prp.loan_id, "'")
                                     );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            _ret = 0;
                        }
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


        internal tm_subsidy GetSubsidyData(tm_subsidy prp)
        {
            tm_subsidy prpRets = new tm_subsidy();

            string _query = "SELECT  LOAN_ID,"
            + " LOAN_CASE_NO,"
            + " START_DT,"
            + " DISTRIBUTION_DT,"
            + " SUBSIDY_AMT,"
            + " SUBSIDY_TYPE,"
            + " SUBSIDY,"
            + " ARDB_CD,"
            + " BRN_CD,"
            + " DEL_FLAG"
            + " FROM TM_SUBSIDY"
            + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) "
            + " AND (  LOAN_ID = {2} )  "
            + " AND (DEL_FLAG='N' ) ";

            using (var connection = OrclDbConnection.NewConnection)            {
                
                    _statement = string.Format(_query,
                    string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                    string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                    string.Concat("'", prp.loan_id, "'")
                    );
                
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var ppr = new tm_subsidy();
                                ppr.loan_id = UtilityM.CheckNull<Int64>(reader["LOAN_ID"]);
                                ppr.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_CASE_NO"]);
                                ppr.start_dt = UtilityM.CheckNull<DateTime>(reader["START_DT"]);
                                ppr.distribution_dt = UtilityM.CheckNull<DateTime>(reader["DISTRIBUTION_DT"]);
                                ppr.subsidy_amt = UtilityM.CheckNull<decimal>(reader["SUBSIDY_AMT"]);
                                ppr.subsidy_type = UtilityM.CheckNull<string>(reader["SUBSIDY_TYPE"]);
                                ppr.subsidy = UtilityM.CheckNull<Int64>(reader["SUBSIDY"]);
                                ppr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                ppr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                ppr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                prpRets = ppr;
                            }
                        }
                    }
                }
            }
            return prpRets;
        }



        internal String GetHostName1()
        {
            string hostname = System.Environment.MachineName;

            return hostname;
        }



        internal String InsertLoanAccountOpeningData(LoanOpenDM acc)
        {
            string _section = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section = "UpdateLoanAll";
                        if (!String.IsNullOrWhiteSpace(acc.tmloanall.loan_id))
                            UpdateLoanAll(connection, acc.tmloanall);
                        _section = "UpdateGuaranter";
                        if (!String.IsNullOrWhiteSpace(acc.tmguaranter.loan_id))
                            UpdateGuaranter(connection, acc.tmguaranter);
                        _section = "UpdateLoanSanction";
                        if (acc.tmlaonsanction.Count > 0)
                            UpdateLoanSanction(connection, acc.tmlaonsanction);
                        _section = "UpdateAccholder";
                        if (acc.tdaccholder.Count > 0)
                            UpdateAccholder(connection, acc.tdaccholder);
                        _section = "UpdateLoanSanctionDtls";
                        if (acc.tmlaonsanctiondtls.Count > 0)
                            UpdateLoanSanctionDtls(connection, acc.tmlaonsanctiondtls);

                        if(acc.tdloansancsetlist.Count > 0)
                        {
                            var td_loan_sanc_list =
                             SerializeEntireLoanSancList(acc.tdloansancsetlist);
                             InsertLoanSecurityDtls(connection, td_loan_sanc_list);
                        }

                        transaction.Commit();
                        return "0" ;
                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return _section + " : " + ex.Message;
                    }

                }
            }
        }

        internal int UpdateLoanAccountOpeningData(LoanOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.tmloanall.loan_id))
                            UpdateLoanAll(connection, acc.tmloanall);
                        if (!String.IsNullOrWhiteSpace(acc.tmguaranter.loan_id))
                            UpdateGuaranter(connection, acc.tmguaranter);
                        if (acc.tmlaonsanction.Count > 0)
                            UpdateLoanSanction(connection, acc.tmlaonsanction);
                        if (acc.tdaccholder.Count > 0)
                            UpdateAccholder(connection, acc.tdaccholder);
                        if (acc.tmlaonsanctiondtls.Count > 0)
                            UpdateLoanSanctionDtls(connection, acc.tmlaonsanctiondtls);
                        if (acc.tmdenominationtrans.Count > 0)
                            _dac.UpdateDenominationDtls(connection, acc.tmdenominationtrans);
                        if (acc.tmtransfer.Count > 0)
                            _dac.UpdateTransfer(connection, acc.tmtransfer);
                        if (acc.tddeftranstrf.Count > 0)
                            _dac.UpdateDepTransTrf(connection, acc.tddeftranstrf);
                        if (!String.IsNullOrWhiteSpace(acc.tddeftrans.trans_cd.ToString()))
                            _dac.UpdateDepTrans(connection, acc.tddeftrans);
                        transaction.Commit();
                        return 0;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }

                }
            }
        }

        internal tm_loan_all GetLoanAll(DbConnection connection, tm_loan_all loan)
        {
            tm_loan_all loanRet = new tm_loan_all();
            string _query = " SELECT TL.ARDB_CD,MC.BRN_CD,TL.PARTY_CD,TL.ACC_CD,TL.LOAN_ID,TL.LOAN_ACC_NO,TL.PRN_LIMIT,TL.DISB_AMT,TL.DISB_DT, "
                     + " TL.CURR_PRN,TL.OVD_PRN,TL.CURR_INTT,TL.OVD_INTT,TL.PENAL_INTT,TL.PRE_EMI_INTT,TL.OTHER_CHARGES,TL.CURR_INTT_RATE,TL.OVD_INTT_RATE,TL.DISB_STATUS,TL.PIRIODICITY,TL.TENURE_MONTH,   "
                     + " TL.INSTL_START_DT,TL.CREATED_BY,TL.CREATED_DT,TL.MODIFIED_BY,TL.MODIFIED_DT,TL.LAST_INTT_CALC_DT,TL.OVD_TRF_DT,TL.APPROVAL_STATUS,TL.CC_FLAG,TL.CHEQUE_FACILITY,TL.INTT_CALC_TYPE, "
                     + " TL.EMI_FORMULA_NO,TL.REP_SCH_FLAG,TL.LOAN_CLOSE_DT,TL.LOAN_STATUS,TL.INSTL_AMT,TL.INSTL_NO,ACTIVITY_CD,ACTIVITY_DTLS,SECTOR_CD,FUND_TYPE,COMP_UNIT_NO "
                     + " ,MC.CUST_NAME , (Select sum(tot_share_holding) From   tm_party_share Where  party_cd = TL.PARTY_CD ) tot_share_holding   "
                     + " FROM TM_LOAN_ALL TL,MM_CUSTOMER MC  "
                     + " WHERE TL.ARDB_CD={0} AND TL.LOAN_ID={1} AND TL.ACC_CD ={2}  "
                     + " AND  TL.PARTY_CD=MC.CUST_CD(+) AND TL.ARDB_CD=MC.ARDB_CD(+)  AND TL.DEL_FLAG='N'  ";

            /*string _query = " SELECT BRN_CD,PARTY_CD,ACC_CD,LOAN_ID,LOAN_ACC_NO,PRN_LIMIT,DISB_AMT,DISB_DT,"
                           + "CURR_PRN,OVD_PRN,CURR_INTT,OVD_INTT,PRE_EMI_INTT,OTHER_CHARGES,CURR_INTT_RATE,OVD_INTT_RATE,DISB_STATUS,PIRIODICITY,TENURE_MONTH,"
                           + "INSTL_START_DT,CREATED_BY,CREATED_DT,MODIFIED_BY,MODIFIED_DT,LAST_INTT_CALC_DT,OVD_TRF_DT,APPROVAL_STATUS,CC_FLAG,CHEQUE_FACILITY,INTT_CALC_TYPE,"
                           + "EMI_FORMULA_NO,REP_SCH_FLAG,LOAN_CLOSE_DT,LOAN_STATUS,INSTL_AMT,INSTL_NO,ACTIVITY_CD,ACTIVITY_DTLS,SECTOR_CD,FUND_TYPE,COMP_UNIT_NO"	 
                           + " FROM TM_LOAN_ALL WHERE BRN_CD={0} AND LOAN_ID={1} ";*/

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(loan.ardb_cd) ? string.Concat("'", loan.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(loan.loan_id) ? string.Concat("'", loan.loan_id, "'") : "LOAN_ID",
                                          loan.acc_cd>0 ? loan.acc_cd.ToString() : "ACC_CD"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new tm_loan_all();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                d.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                d.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                d.prn_limit = UtilityM.CheckNull<decimal>(reader["PRN_LIMIT"]);
                                d.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                d.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                d.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                d.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                d.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                d.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                d.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                d.pre_emi_intt = UtilityM.CheckNull<decimal>(reader["PRE_EMI_INTT"]);
                                d.other_charges = UtilityM.CheckNull<decimal>(reader["OTHER_CHARGES"]);
                                d.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                                d.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                                d.disb_status = UtilityM.CheckNull<string>(reader["DISB_STATUS"]);
                                d.piriodicity = UtilityM.CheckNull<string>(reader["PIRIODICITY"]);
                                d.tenure_month = UtilityM.CheckNull<int>(reader["TENURE_MONTH"]);
                                d.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                                d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                d.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                d.ovd_trf_dt = UtilityM.CheckNull<DateTime>(reader["OVD_TRF_DT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.cc_flag = UtilityM.CheckNull<string>(reader["CC_FLAG"]);
                                d.cheque_facility = UtilityM.CheckNull<string>(reader["CHEQUE_FACILITY"]);
                                d.intt_calc_type = UtilityM.CheckNull<string>(reader["INTT_CALC_TYPE"]);
                                d.emi_formula_no = UtilityM.CheckNull<int>(reader["EMI_FORMULA_NO"]);
                                d.rep_sch_flag = UtilityM.CheckNull<string>(reader["REP_SCH_FLAG"]);
                                d.loan_close_dt = UtilityM.CheckNull<DateTime>(reader["LOAN_CLOSE_DT"]);
                                d.loan_status = UtilityM.CheckNull<string>(reader["LOAN_STATUS"]);
                                d.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                d.instl_no = UtilityM.CheckNull<int>(reader["INSTL_NO"]);
                                d.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                d.activity_dtls = UtilityM.CheckNull<string>(reader["ACTIVITY_DTLS"]);
                                d.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                                d.fund_type = UtilityM.CheckNull<string>(reader["FUND_TYPE"]);
                                d.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                                d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                d.tot_share_holding = UtilityM.CheckNull<decimal>(reader["tot_share_holding"]);
                                loanRet = d;
                            }
                        }
                    }
                }
            }
            return loanRet;
        }
        internal tm_guaranter GetGuaranter(DbConnection connection, string ardb_cd, string loan_id)
        {
            tm_guaranter loanRet = new tm_guaranter();
            string _query = " SELECT ARDB_CD,LOAN_ID,ACC_CD,GUA_TYPE,GUA_ID,GUA_NAME,GUA_ADD,OFFICE_NAME, "
                            + " SHARE_ACC_NUM,SHARE_TYPE,OPENING_DT,SHARE_BAL,DEPART,DESIG,  "
                            + " SALARY,SEC_58,MOBILE,SRL_NO "
                            + " FROM TM_GUARANTER WHERE ARDB_CD = {0} AND  LOAN_ID = {1} AND DEL_FLAG= 'N' ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(ardb_cd) ? string.Concat("'", ardb_cd, "'") : "ARDB_CD", 
                                          !string.IsNullOrWhiteSpace(loan_id) ? string.Concat("'", loan_id, "'") : "LOAN_ID"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new tm_guaranter();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                d.acc_cd = UtilityM.CheckNull<Decimal>(reader["ACC_CD"]);
                                d.gua_type = UtilityM.CheckNull<string>(reader["GUA_TYPE"]);
                                d.gua_id = UtilityM.CheckNull<string>(reader["GUA_ID"]);
                                d.gua_name = UtilityM.CheckNull<string>(reader["GUA_NAME"]);
                                d.gua_add = UtilityM.CheckNull<string>(reader["GUA_ADD"]);
                                d.office_name = UtilityM.CheckNull<string>(reader["OFFICE_NAME"]);
                                d.share_acc_num = UtilityM.CheckNull<string>(reader["SHARE_ACC_NUM"]);
                                d.share_type = UtilityM.CheckNull<Int64>(reader["SHARE_TYPE"]);
                                d.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                d.share_bal = UtilityM.CheckNull<decimal>(reader["SHARE_BAL"]);
                                d.depart = UtilityM.CheckNull<string>(reader["DEPART"]);
                                d.desig = UtilityM.CheckNull<string>(reader["DESIG"]);
                                d.salary = UtilityM.CheckNull<decimal>(reader["SALARY"]);
                                d.sec_58 = UtilityM.CheckNull<string>(reader["SEC_58"]);
                                d.mobile = UtilityM.CheckNull<string>(reader["MOBILE"]);
                                d.srl_no = UtilityM.CheckNull<Int64>(reader["SRL_NO"]);
                                loanRet = d;
                            }
                        }
                    }
                }
            }
            return loanRet;
        }
        internal List<tm_loan_sanction> GetLoanSanction(DbConnection connection,string ardb_cd, string loan_id)
        {
            List<tm_loan_sanction> loanRet = new List<tm_loan_sanction>();
            string _query = " SELECT     ARDB_CD,LOAN_ID , SANC_NO ,   SANC_DT , CREATED_BY , "
                            + " CREATED_DT ,  MODIFIED_BY ,  MODIFIED_DT , "
                            + " APPROVAL_STATUS ,   APPROVED_BY , APPROVED_DT , "
                            + " MEMO_NO    FROM  TM_LOAN_SANCTION   "
                            + " WHERE  ARDB_CD={0} and  LOAN_ID  = {1} ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(ardb_cd) ? string.Concat("'", ardb_cd, "'") : "ARDB_CD",
                                          !string.IsNullOrWhiteSpace(loan_id) ? string.Concat("'", loan_id, "'") : "LOAN_ID"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new tm_loan_sanction();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                d.sanc_no = UtilityM.CheckNull<decimal>(reader["SANC_NO"]);
                                d.sanc_dt = UtilityM.CheckNull<DateTime>(reader["SANC_DT"]);
                                d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                d.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                d.memo_no = UtilityM.CheckNull<string>(reader["MEMO_NO"]);
                                loanRet.Add(d);
                            }
                        }
                    }
                }
            }
            return loanRet;
        }
        internal List<td_accholder> GetAccholderTemp(DbConnection connection, string ardb_cd, string brn_cd, string acc_num, Int32 acc_type_cd)
        {
            List<td_accholder> accList = new List<td_accholder>();

            dynamic _query = " SELECT TA.ARDB_CD,TA.BRN_CD,TA.ACC_TYPE_CD,TA.ACC_NUM,TA.ACC_HOLDER,"
                  + " TA.RELATION,TA.CUST_CD,MC.CUST_NAME                      "
                  + " FROM TD_ACCHOLDER TA,MM_CUSTOMER MC                      "
                  + " WHERE TA.ARDB_CD = {0} AND TA.BRN_CD = {1}                                  "
                  + " AND   ACC_NUM = {2}                              "
                  + " AND   TA.CUST_CD=MC.CUST_CD (+)                          "
                  + " AND   TA.BRN_CD=MC.BRN_CD (+)                            "
                  + " AND   ACC_TYPE_CD = {3}                                  ";

            var v1 = !string.IsNullOrWhiteSpace(ardb_cd) ? string.Concat("'", ardb_cd, "'") : "ardb_cd";
            var v2 = !string.IsNullOrWhiteSpace(brn_cd) ? string.Concat("'", brn_cd, "'") : "brn_cd";
            var v3 = !string.IsNullOrWhiteSpace(acc_num) ? string.Concat("'", acc_num, "'") : "acc_num";
            dynamic v4 = (acc_type_cd > 0) ? acc_type_cd.ToString() : "ACC_TYPE_CD";
            _statement = string.Format(_query, v1, v2, v3,v4);


            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var a = new td_accholder();
                                a.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                a.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                a.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                a.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                a.acc_holder = UtilityM.CheckNull<string>(reader["ACC_HOLDER"]);
                                a.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                a.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                a.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                accList.Add(a);
                            }
                        }
                    }
                }
            }
            return accList;
        }
        internal List<tm_loan_sanction_dtls> GetLoanSanctionDtls(DbConnection connection, string ardb_cd, string loan_id)
        {
            List<tm_loan_sanction_dtls> loanRet = new List<tm_loan_sanction_dtls>();
            string _query = " SELECT  LS.ARDB_CD,LS.SECTOR_CD ,  LS.ACTIVITY_CD ,   LS.CROP_CD ,  SANC_AMT ,      "
                     + " DUE_DT ,  LOAN_ID , SANC_NO , SANC_STATUS ,                              "
                     + " SRL_NO ,  APPROVAL_STATUS ,MC.CROP_DESC,MA.ACTIVITY_DESC,MS.SECTOR_DESC  "
                     + " FROM  TM_LOAN_SANCTION_DTLS   LS,                                        "
                     + " MM_CROP MC,MM_ACTIVITY  MA,                                              "
                     + " MM_SECTOR MS                                                             "
                     + " WHERE LS.SECTOR_CD=MS.SECTOR_CD(+) AND LS.CROP_CD=MC.CROP_CD(+)          "
                     + " AND LS.ACTIVITY_CD=MA.ACTIVITY_CD(+)                                     "
                     + " AND MA.SECTOR_CD=MS.SECTOR_CD AND LS.ARDB_CD = {0} AND  LS.LOAN_ID  = {1}  AND LS.DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(ardb_cd) ? string.Concat("'", ardb_cd, "'") : "ARDB_CD",
                                          !string.IsNullOrWhiteSpace(loan_id) ? string.Concat("'", loan_id, "'") : "LOAN_ID"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new tm_loan_sanction_dtls();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                d.sanc_no = UtilityM.CheckNull<decimal>(reader["SANC_NO"]);
                                d.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                                d.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                d.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                                d.sanc_amt = UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                d.due_dt = UtilityM.CheckNull<DateTime>(reader["DUE_DT"]);
                                d.sanc_status = UtilityM.CheckNull<string>(reader["SANC_STATUS"]);
                                d.srl_no = UtilityM.CheckNull<decimal>(reader["SRL_NO"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.crop_desc = UtilityM.CheckNull<string>(reader["CROP_DESC"]);
                                d.activity_desc = UtilityM.CheckNull<string>(reader["ACTIVITY_DESC"]);
                                d.sector_desc = UtilityM.CheckNull<string>(reader["SECTOR_DESC"]);
                                loanRet.Add(d);
                            }
                        }
                    }
                }
            }
            return loanRet;
        }

      internal List<td_loan_sanc_set> GetTdLoanSanctionDtls(DbConnection connection, string ardb_cd,string loan_id , int acc_cd)
        {
            List<td_loan_sanc_set> tdLoanSancSetList = new List<td_loan_sanc_set>();
            List<td_loan_sanc> tdLoanSancList = new List<td_loan_sanc>();
            td_loan_sanc_set loanSancSet = new td_loan_sanc_set();

            int prevDataSet = 1;

            string _query = " SELECT T.ARDB_CD,T.LOAN_ID , T.SANC_NO , T.PARAM_CD , T.PARAM_VALUE , UPPER(T.PARAM_TYPE) PARAM_TYPE , T.DATASET_NO , T.FIELD_NAME , S.PARAM_DESC"
                           + " FROM TD_LOAN_SANC  T, SM_LOAN_SANCTION S "
                           + " WHERE T.ARDB_CD = {0} AND T.LOAN_ID = {1}            "
                           +" AND S.PARAM_CD = T.PARAM_CD     "
                           +" AND S.FIELD_NAME = T.FIELD_NAME "
                           +" AND S.ACC_CD = {2} AND T.DEL_FLAG = 'N'             "
                           +" ORDER BY DATASET_NO , PARAM_CD ";

            _statement = string.Format(_query,
                                            !string.IsNullOrWhiteSpace(ardb_cd) ? string.Concat("'", ardb_cd, "'") : "1" ,
                                          !string.IsNullOrWhiteSpace(loan_id) ? string.Concat("'", loan_id, "'") : "1" ,
                                           string.Concat("'", acc_cd , "'") 
                                           );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new td_loan_sanc();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]).ToString();
                                d.sanc_no = UtilityM.CheckNull<decimal>(reader["SANC_NO"]);                                
                                d.param_cd = UtilityM.CheckNull<string>(reader["PARAM_CD"]);
                                d.param_value = UtilityM.CheckNull<string>(reader["PARAM_VALUE"]);
                                d.param_type = UtilityM.CheckNull<string>(reader["PARAM_TYPE"]);
                                // if (d.param_type == "DATE")
                                // {
                                //     d.param_value_dt =  UtilityM.CheckNull<DateTime>(reader["PARAM_VALUE_DT"]);
                                // }
                                // else
                                // d.param_value_dt =null;
                                
                                d.dataset_no = UtilityM.CheckNull<Int32>(reader["DATASET_NO"]);
                                d.field_name = UtilityM.CheckNull<string>(reader["FIELD_NAME"]);
                                d.param_desc = UtilityM.CheckNull<string>(reader["PARAM_DESC"]);                                

                                if (prevDataSet != d.dataset_no)
                                {
                                    prevDataSet = d.dataset_no;
                                    loanSancSet.tdloansancset = tdLoanSancList;
                                    tdLoanSancSetList.Add( loanSancSet);

                                    loanSancSet = new td_loan_sanc_set();
                                    tdLoanSancList = new List<td_loan_sanc>();
                                }

                                tdLoanSancList.Add(d);
                            }

                            if(tdLoanSancList.Count > 0)
                            {
                                // td_loan_sanc_set loanSancSet = new td_loan_sanc_set();
                                // loanSancSet.tdloansancset = tdLoanSancList;
                                // tdLoanSancSetList.Add( loanSancSet);

                                loanSancSet.tdloansancset = tdLoanSancList;
                                tdLoanSancSetList.Add( loanSancSet);
                            }

                        }
                    }
                }
            }

            return tdLoanSancSetList;
        }





        internal bool InsertLoanAll(DbConnection connection, tm_loan_all loan)
        {
            string _query = "INSERT INTO TM_LOAN_ALL (BRN_CD, PARTY_CD, ACC_CD, LOAN_ID, LOAN_ACC_NO, PRN_LIMIT, DISB_AMT, DISB_DT,   "
                            + "CURR_PRN, OVD_PRN, CURR_INTT, OVD_INTT, PRE_EMI_INTT, OTHER_CHARGES, CURR_INTT_RATE, OVD_INTT_RATE,    "
                            + "DISB_STATUS, PIRIODICITY, TENURE_MONTH, INSTL_START_DT, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,"
                            + "LAST_INTT_CALC_DT, OVD_TRF_DT, APPROVAL_STATUS, CC_FLAG, CHEQUE_FACILITY, INTT_CALC_TYPE, EMI_FORMULA_NO,  "
                            + "REP_SCH_FLAG, LOAN_CLOSE_DT, LOAN_STATUS, INSTL_AMT, INSTL_NO, ACTIVITY_CD, ACTIVITY_DTLS, SECTOR_CD,  "
                            + "FUND_TYPE, COMP_UNIT_NO,ARDB_CD,DEL_FLAG,PENAL_INTT)    "
                            + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                            + " {15},{16}, {17}, {18},{19},{20},SYSDATE,{21},SYSDATE, "
                            + " {22},{23},{24},{25},{26},{27},{28},{29}, "
                            + " {30},{31},{32},{33},{34}, {35},{36},{37},"
                            + " {38},{39},'N',{40})   ";

            _statement = string.Format(_query,
               string.Concat("'", loan.brn_cd, "'"),
                string.Concat("'", loan.party_cd, "'"),
                string.Concat("'", loan.acc_cd, "'"),
                string.Concat("'", loan.loan_id, "'"),
                string.Concat("'", loan.loan_acc_no, "'"),
                string.Concat("'", loan.prn_limit, "'"),
                string.Concat("'", loan.disb_amt, "'"),
                string.IsNullOrWhiteSpace(loan.disb_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.disb_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                string.Concat("'", loan.curr_prn, "'"),
                string.Concat("'", loan.ovd_prn, "'"),
                string.Concat("'", loan.curr_intt, "'"),
                string.Concat("'", loan.ovd_intt, "'"),
                string.Concat("'", loan.pre_emi_intt, "'"),
                string.Concat("'", loan.other_charges, "'"),
                string.Concat("'", loan.curr_intt_rate, "'"),
                string.Concat("'", loan.ovd_intt_rate, "'"),
                string.Concat("'", loan.disb_status, "'"),
                string.Concat("'", loan.piriodicity, "'"),
                string.Concat("'", loan.tenure_month, "'"),
                string.IsNullOrWhiteSpace(loan.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                string.Concat("'", loan.created_by, "'"),
                string.Concat("'", loan.modified_by, "'"),
                string.IsNullOrWhiteSpace(loan.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                string.IsNullOrWhiteSpace(loan.ovd_trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.ovd_trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                string.Concat("'", loan.approval_status, "'"),
                string.Concat("'", loan.cc_flag, "'"),
                string.Concat("'", loan.cheque_facility, "'"),
                string.Concat("'", loan.intt_calc_type, "'"),
                string.Concat("'", loan.emi_formula_no, "'"),
                string.Concat("'", loan.rep_sch_flag, "'"),
                string.IsNullOrWhiteSpace(loan.loan_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.loan_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                string.Concat("'", loan.loan_status, "'"),
                string.Concat("'", loan.instl_amt, "'"),
                string.Concat("'", loan.instl_no, "'"),
                string.Concat("'", loan.activity_cd, "'"),
                string.Concat("'", loan.activity_dtls, "'"),
                string.Concat("'", loan.sector_cd, "'"),
                string.Concat("'", loan.fund_type, "'"),
                string.Concat("'", loan.comp_unit_no, "'"),
                string.Concat("'", loan.ardb_cd, "'"),
                string.Concat("'", loan.penal_intt, "'")
                );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool Insertguaranter(DbConnection connection, tm_guaranter loan)
        {
            string _query = "INSERT INTO TM_GUARANTER (LOAN_ID, ACC_CD, GUA_TYPE, GUA_ID, GUA_NAME, GUA_ADD, OFFICE_NAME, "
                           + " SHARE_ACC_NUM, SHARE_TYPE, OPENING_DT, SHARE_BAL, DEPART, DESIG, SALARY, SEC_58, MOBILE, SRL_NO,ARDB_CD,DEL_FLAG) "
                           + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                           + " {15},{16},{17},'N' ) ";

            _statement = string.Format(_query,
                String.Concat("'", loan.loan_id, "'"),
                String.Concat("'", loan.acc_cd, "'"),
                String.Concat("'", loan.gua_type, "'"),
                String.Concat("'", loan.gua_id, "'"),
                String.Concat("'", loan.gua_name, "'"),
                String.Concat("'", loan.gua_add, "'"),
                String.Concat("'", loan.office_name, "'"),
                String.Concat("'", loan.share_acc_num, "'"),
                String.Concat("'", loan.share_type, "'"),
                String.Concat("'", loan.opening_dt, "'"),
                String.Concat("'", loan.share_bal, "'"),
                String.Concat("'", loan.depart, "'"),
                String.Concat("'", loan.desig, "'"),
                String.Concat("'", loan.salary, "'"),
                String.Concat("'", loan.sec_58, "'"),
                String.Concat("'", loan.mobile, "'"),
                String.Concat("'", loan.srl_no, "'"),
                String.Concat("'", loan.ardb_cd,"'")
                );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool InsertAccholder(DbConnection connection, td_accholder acc)
        {
            string _query = "INSERT INTO TD_ACCHOLDER ( brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,ardb_cd,del_flag ) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N' ) ";

            _statement = string.Format(_query,
                                                   string.Concat("'", acc.brn_cd, "'"),
                                                   acc.acc_type_cd,
                                                   string.Concat("'", acc.acc_num, "'"),
                                                   string.Concat("'", acc.acc_holder, "'"),
                                                   string.Concat("'", acc.relation, "'"),
                                                   acc.cust_cd,
                                                   string.Concat("'", acc.ardb_cd, "'")
                                                    );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool InsertLoanSanction(DbConnection connection, tm_loan_sanction acc)
        {

            string _query = "INSERT INTO TM_LOAN_SANCTION (LOAN_ID, SANC_NO, SANC_DT, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, MEMO_NO,ARDB_CD,DEL_FLAG) "
                            + "VALUES ({0}, {1}, {2}, {3},SYSDATE, {4}, SYSDATE,{5}, {6}, {7}, {8},{9},'N')";

            _statement = string.Format(_query,
                         string.Concat("'", acc.loan_id, "'"),
                         string.Concat("'", acc.sanc_no, "'"),
                         string.IsNullOrWhiteSpace(acc.sanc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc.sanc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                         string.Concat("'", acc.created_by, "'"),
                         string.Concat("'", acc.modified_by, "'"),
                         string.Concat("'", acc.approval_status, "'"),
                         string.Concat("'", acc.approved_by, "'"),
                         string.IsNullOrWhiteSpace(acc.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                         string.Concat("'", acc.memo_no, "'"),
                         string.Concat("'", acc.ardb_cd, "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool InsertLoanSanctionDtls(DbConnection connection, tm_loan_sanction_dtls acc)
        {

            string _query = "INSERT INTO TM_LOAN_SANCTION_DTLS (LOAN_ID, SANC_NO, SECTOR_CD, ACTIVITY_CD, CROP_CD, SANC_AMT, DUE_DT, SANC_STATUS, SRL_NO, APPROVAL_STATUS,ARDB_CD,DEL_FLAG)"
                            + " VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9},{10},'N')";


            _statement = string.Format(_query,
      string.Concat("'", acc.loan_id, "'"),
      string.Concat("'", acc.sanc_no, "'"),
      string.Concat("'", acc.sector_cd, "'"),
      string.Concat("'", acc.activity_cd, "'"),
      string.Concat("'", acc.crop_cd, "'"),
      string.Concat("'", acc.sanc_amt, "'"),
      string.IsNullOrWhiteSpace(acc.due_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc.due_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
      string.Concat("'", acc.sanc_status, "'"),
      string.Concat("'", acc.srl_no, "'"),
      string.Concat("'", acc.approval_status, "'"),
      string.Concat("'", acc.ardb_cd, "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }

        internal bool UpdateLoanAll(DbConnection connection, tm_loan_all loan)
        {
            string _query = " UPDATE TM_LOAN_ALL "
+ " SET   PARTY_CD          = NVL({1},  PARTY_CD         )"
+ ", ACC_CD            = NVL({2},  ACC_CD           )"
+ ", LOAN_ID           = NVL({3},  LOAN_ID          )"
+ ", LOAN_ACC_NO       = NVL({4},  LOAN_ACC_NO      )"
+ ", PRN_LIMIT         = NVL({5},  PRN_LIMIT        )"
+ ", DISB_AMT          = NVL({6},  DISB_AMT         )"
+ ", DISB_DT           = NVL({7},  DISB_DT          )"
+ ", CURR_PRN          = NVL({8},  CURR_PRN         )"
+ ", OVD_PRN           = NVL({9}, OVD_PRN          )"
+ ", CURR_INTT         = NVL({10}, CURR_INTT        )"
+ ", OVD_INTT          = NVL({11}, OVD_INTT         )"
+ ", PRE_EMI_INTT      = NVL({12}, PRE_EMI_INTT     )"
+ ", OTHER_CHARGES     = NVL({13}, OTHER_CHARGES    )"
+ ", CURR_INTT_RATE    = NVL({14}, CURR_INTT_RATE   )"
+ ", OVD_INTT_RATE     = NVL({15}, OVD_INTT_RATE    )"
+ ", DISB_STATUS       = NVL({16}, DISB_STATUS      )"
+ ", PIRIODICITY       = NVL({17}, PIRIODICITY      )"
+ ", TENURE_MONTH      = NVL({18}, TENURE_MONTH     )"
+ ", INSTL_START_DT    = NVL({19}, INSTL_START_DT   )"
+ ", MODIFIED_BY       = NVL({20}, MODIFIED_BY      )"
+ ", MODIFIED_DT       = SYSDATE                     "
+ ", LAST_INTT_CALC_DT = NVL({21}, LAST_INTT_CALC_DT)"
+ ", OVD_TRF_DT        = NVL({22}, OVD_TRF_DT       )"
+ ", APPROVAL_STATUS   = NVL({23}, APPROVAL_STATUS  )"
+ ", CC_FLAG           = NVL({24}, CC_FLAG          )"
+ ", CHEQUE_FACILITY   = NVL({25}, CHEQUE_FACILITY  )"
+ ", INTT_CALC_TYPE    = NVL({26}, INTT_CALC_TYPE   )"
+ ", EMI_FORMULA_NO    = NVL({27}, EMI_FORMULA_NO   )"
+ ", REP_SCH_FLAG      = NVL({28}, REP_SCH_FLAG     )"
+ ", LOAN_CLOSE_DT     = NVL({29}, LOAN_CLOSE_DT    )"
+ ", LOAN_STATUS       = NVL({30}, LOAN_STATUS      )"
+ ", INSTL_AMT         = NVL({31}, INSTL_AMT        )"
+ ", INSTL_NO          = NVL({32}, INSTL_NO         )"
+ ", ACTIVITY_CD       = NVL({33}, ACTIVITY_CD      )"
+ ", ACTIVITY_DTLS     = NVL({34}, ACTIVITY_DTLS    )"
+ ", SECTOR_CD         = NVL({35}, SECTOR_CD        )"
+ ", FUND_TYPE         = NVL({36}, FUND_TYPE        )"
+ ", COMP_UNIT_NO      = NVL({37}, COMP_UNIT_NO     )"
+ ", PENAL_INTT        = NVL({38}, PENAL_INTT        )"
+ " WHERE LOAN_ID           = {39} "
+ " AND ARDB_CD = {41} ";

            _statement = string.Format(_query,
             string.Concat("'", loan.brn_cd, "'"),
              string.Concat("'", loan.party_cd, "'"),
              string.Concat("'", loan.acc_cd, "'"),
              string.Concat("'", loan.loan_id, "'"),
              string.Concat("'", loan.loan_acc_no, "'"),
              string.Concat("'", loan.prn_limit, "'"),
              string.Concat("'", loan.disb_amt, "'"),
              string.IsNullOrWhiteSpace(loan.disb_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.disb_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
              string.Concat("'", loan.curr_prn, "'"),
              string.Concat("'", loan.ovd_prn, "'"),
              string.Concat("'", loan.curr_intt, "'"),
              string.Concat("'", loan.ovd_intt, "'"),
              string.Concat("'", loan.pre_emi_intt, "'"),
              string.Concat("'", loan.other_charges, "'"),
              string.Concat("'", loan.curr_intt_rate, "'"),
              string.Concat("'", loan.ovd_intt_rate, "'"),
              string.Concat("'", loan.disb_status, "'"),
              string.Concat("'", loan.piriodicity, "'"),
              string.Concat("'", loan.tenure_month, "'"),
              string.IsNullOrWhiteSpace(loan.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
              string.Concat("'", loan.modified_by, "'"),
              string.IsNullOrWhiteSpace(loan.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
              string.IsNullOrWhiteSpace(loan.ovd_trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.ovd_trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
              string.Concat("'", loan.approval_status, "'"),
              string.Concat("'", loan.cc_flag, "'"),
              string.Concat("'", loan.cheque_facility, "'"),
              string.Concat("'", loan.intt_calc_type, "'"),
              string.Concat("'", loan.emi_formula_no, "'"),
              string.Concat("'", loan.rep_sch_flag, "'"),
              string.IsNullOrWhiteSpace(loan.loan_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", loan.loan_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
              string.Concat("'", loan.loan_status, "'"),
              string.Concat("'", loan.instl_amt, "'"),
              string.Concat("'", loan.instl_no, "'"),
              string.Concat("'", loan.activity_cd, "'"),
              string.Concat("'", loan.activity_dtls, "'"),
              string.Concat("'", loan.sector_cd, "'"),
              string.Concat("'", loan.fund_type, "'"),
              string.Concat("'", loan.comp_unit_no, "'"),
              string.Concat("'", loan.penal_intt, "'"),
               string.Concat("'", loan.loan_id, "'"),
              string.Concat("'", loan.brn_cd, "'"),
              string.Concat("'", loan.ardb_cd, "'")
              );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                int count = command.ExecuteNonQuery();
                if (count.Equals(0))
                    InsertLoanAll(connection, loan);
            }
            return true;
        }

        internal bool UpdateGuaranter(DbConnection connection, tm_guaranter loan)
        {
            string _query = " UPDATE TM_GUARANTER "
+ " SET   LOAN_ID        = NVL({0}, LOAN_ID      )"
+ ", ACC_CD         = NVL({1}, ACC_CD       )"
+ ", GUA_TYPE       = NVL({2}, GUA_TYPE     )"
+ ", GUA_ID         = NVL({3}, GUA_ID       )"
+ ", GUA_NAME       = NVL({4}, GUA_NAME     )"
+ ", GUA_ADD        = NVL({5}, GUA_ADD      )"
+ ", OFFICE_NAME    = NVL({6}, OFFICE_NAME  )"
+ ", SHARE_ACC_NUM  = NVL({7}, SHARE_ACC_NUM)"
+ ", SHARE_TYPE     = NVL({8}, SHARE_TYPE   )"
+ ", OPENING_DT     = NVL({9}, OPENING_DT   )"
+ ", SHARE_BAL      = NVL({10}, SHARE_BAL    )"
+ ", DEPART         = NVL({11}, DEPART       )"
+ ", DESIG          = NVL({12}, DESIG        )"
+ ", SALARY         = NVL({13}, SALARY       )"
+ ", SEC_58         = NVL({14}, SEC_58       )"
+ ", MOBILE         = NVL({15}, MOBILE       )"
+ ", SRL_NO         = NVL({16}, SRL_NO       )"
+ " WHERE LOAN_ID = {17} AND ACC_CD = {18} AND ARDB_CD={19}     "
+ " AND SRL_NO = 1 ";
            _statement = string.Format(_query,
               String.Concat("'", loan.loan_id, "'"),
               String.Concat("'", loan.acc_cd, "'"),
               String.Concat("'", loan.gua_type, "'"),
               String.Concat("'", loan.gua_id, "'"),
               String.Concat("'", loan.gua_name, "'"),
               String.Concat("'", loan.gua_add, "'"),
               String.Concat("'", loan.office_name, "'"),
               String.Concat("'", loan.share_acc_num, "'"),
               String.Concat("'", loan.share_type, "'"),
               String.Concat("'", loan.opening_dt, "'"),
               String.Concat("'", loan.share_bal, "'"),
               String.Concat("'", loan.depart, "'"),
               String.Concat("'", loan.desig, "'"),
               String.Concat("'", loan.salary, "'"),
               String.Concat("'", loan.sec_58, "'"),
               String.Concat("'", loan.mobile, "'"),
               String.Concat("'", loan.srl_no, "'"),
               String.Concat("'", loan.loan_id, "'"),
               String.Concat("'", loan.acc_cd, "'"),
               String.Concat("'", loan.ardb_cd, "'")
               );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                int count = command.ExecuteNonQuery();
                if (count.Equals(0))
                    Insertguaranter(connection, loan);
            }
            return true;

        }

        internal bool UpdateLoanSanction(DbConnection connection, List<tm_loan_sanction> acc)
        {
            string _query = " UPDATE TM_LOAN_SANCTION               "
+ " SET   LOAN_ID           = NVL({0},LOAN_ID         )"
+ ", SANC_NO           = NVL({1},SANC_NO         )"
+ ", SANC_DT           = NVL({2},SANC_DT         )"
+ ", MODIFIED_BY       = NVL({3},MODIFIED_BY     )"
+ ", MODIFIED_DT       = SYSDATE                  "
+ ", APPROVAL_STATUS   = NVL({4},APPROVAL_STATUS )"
+ ", APPROVED_BY       = NVL({5},APPROVED_BY     )"
+ ", APPROVED_DT       = NVL({6},APPROVED_DT     )"
+ ", MEMO_NO           = NVL({7},MEMO_NO         )"
+ " WHERE LOAN_ID = {8} AND SANC_NO = {9} AND ARDB_CD={10}";
            for (int i = 0; i < acc.Count; i++)
            {
                _statement = string.Format(_query,
                             string.Concat("'", acc[i].loan_id, "'"),
                             string.Concat("'", acc[i].sanc_no, "'"),
                             string.IsNullOrWhiteSpace(acc[i].sanc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc[i].sanc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                              string.Concat("'", acc[i].modified_by, "'"),
                             string.Concat("'", acc[i].approval_status, "'"),
                             string.Concat("'", acc[i].approved_by, "'"),
                             string.IsNullOrWhiteSpace(acc[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", acc[i].memo_no, "'"),
                              string.Concat("'", acc[i].loan_id, "'"),
                             string.Concat("'", acc[i].sanc_no, "'"),
                             string.Concat("'", acc[i].ardb_cd, "'"));

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    int count = command.ExecuteNonQuery();
                    if (count.Equals(0))
                        InsertLoanSanction(connection, acc[i]);
                }
            }
            return true;

        }

        internal bool UpdateAccholder(DbConnection connection, List<td_accholder> acc)
        {
            string _query = " UPDATE td_accholder   "
                 + " SET brn_cd     = {0}, "
                 + " acc_type_cd    = {1}, "
                 + " acc_num        = {2}, "
                 + " acc_holder     = {3}, "
                 + " relation       = {4} "
                 + " WHERE cust_cd   = {5} and  brn_cd = {6} AND acc_num = {7} AND acc_type_cd=NVL({8},  acc_type_cd ) and ardb_cd={9}  ";

            for (int i = 0; i < acc.Count; i++)
            {
                _statement = string.Format(_query,
                                 !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                 !string.IsNullOrWhiteSpace(acc[i].acc_type_cd.ToString()) ? string.Concat("'", acc[i].acc_type_cd, "'") : "acc_type_cd",
                                 !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                 !string.IsNullOrWhiteSpace(acc[i].acc_holder) ? string.Concat("'", acc[i].acc_holder, "'") : "acc_holder",
                                 !string.IsNullOrWhiteSpace(acc[i].relation) ? string.Concat("'", acc[i].relation, "'") : "relation",
                                 !string.IsNullOrWhiteSpace(acc[i].cust_cd.ToString()) ? string.Concat("'", acc[i].cust_cd, "'") : "cust_cd",
                                 !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                 !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                 string.Concat("'", acc[i].acc_type_cd, "'"),
                                 !string.IsNullOrWhiteSpace(acc[i].ardb_cd) ? string.Concat("'", acc[i].ardb_cd, "'") : "ardb_cd"

                                 );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    int count = command.ExecuteNonQuery();
                    if (count.Equals(0))
                        InsertAccholder(connection, acc[i]);

                }
            }
            return true;
        }

        internal bool UpdateLoanSanctionDtls(DbConnection connection, List<tm_loan_sanction_dtls> acc)
        {
            string _query = " UPDATE TM_LOAN_SANCTION_DTLS           "
+ " SET   LOAN_ID         = NVL({0}, LOAN_ID        )"
+ ", SANC_NO         = NVL({1}, SANC_NO        )"
+ ", SECTOR_CD       = NVL({2}, SECTOR_CD      )"
+ ", ACTIVITY_CD     = NVL({3}, ACTIVITY_CD    )"
+ ", CROP_CD         = NVL({4}, CROP_CD        )"
+ ", SANC_AMT        = NVL({5}, SANC_AMT       )"
+ ", DUE_DT          = NVL({6}, DUE_DT         )"
+ ", SANC_STATUS     = NVL({7}, SANC_STATUS    )"
+ ", SRL_NO          = NVL({8}, SRL_NO         )"
+ ", APPROVAL_STATUS = NVL({9}, APPROVAL_STATUS)"
+ " WHERE LOAN_ID = {10} AND SANC_NO = {11} AND SRL_NO = {12} AND ARDB_CD={13} ";
            for (int i = 0; i < acc.Count; i++)
            {
                _statement = string.Format(_query,
          string.Concat("'", acc[i].loan_id, "'"),
          string.Concat("'", acc[i].sanc_no, "'"),
          string.Concat("'", acc[i].sector_cd, "'"),
          string.Concat("'", acc[i].activity_cd, "'"),
          string.Concat("'", acc[i].crop_cd, "'"),
          string.Concat("'", acc[i].sanc_amt, "'"),
          string.IsNullOrWhiteSpace(acc[i].due_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", acc[i].due_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
          string.Concat("'", acc[i].sanc_status, "'"),
          string.Concat("'", acc[i].srl_no, "'"),
          string.Concat("'", acc[i].approval_status, "'"),
          string.Concat("'", acc[i].loan_id, "'"),
          string.Concat("'", acc[i].sanc_no, "'"),
         string.Concat("'", acc[i].srl_no, "'"),
         string.Concat("'", acc[i].ardb_cd, "'")
          );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    int count = command.ExecuteNonQuery();
                    if (count.Equals(0))
                        InsertLoanSanctionDtls(connection, acc[i]);
                }
            }
            return true;

        }


        internal tm_loan_all GetLoanAllWithChild(tm_loan_all loan)
        {
            tm_loan_all loanRet = new tm_loan_all();
            string _query = " SELECT TL.ARDB_CD,MC.BRN_CD,TL.PARTY_CD,TL.ACC_CD,TL.LOAN_ID,TL.LOAN_ACC_NO,TL.PRN_LIMIT,TL.DISB_AMT,TL.DISB_DT, "
                          + "  TL.CURR_PRN,TL.OVD_PRN,TL.CURR_INTT,TL.OVD_INTT,TL.PENAL_INTT,TL.PRE_EMI_INTT,TL.OTHER_CHARGES,TL.CURR_INTT_RATE,TL.OVD_INTT_RATE,TL.DISB_STATUS,TL.PIRIODICITY,TL.TENURE_MONTH, "
                          + " TL.INSTL_START_DT,TL.CREATED_BY,TL.CREATED_DT,TL.MODIFIED_BY,TL.MODIFIED_DT,TL.LAST_INTT_CALC_DT,TL.OVD_TRF_DT,TL.APPROVAL_STATUS,TL.CC_FLAG,TL.CHEQUE_FACILITY,TL.INTT_CALC_TYPE, "
                          + " TL.EMI_FORMULA_NO,TL.REP_SCH_FLAG,TL.LOAN_CLOSE_DT,TL.LOAN_STATUS,TL.INSTL_AMT,TL.INSTL_NO,ACTIVITY_CD,ACTIVITY_DTLS,SECTOR_CD,FUND_TYPE,COMP_UNIT_NO	 "
                          + " ,MC.CUST_NAME "
                          + " FROM TM_LOAN_ALL TL,MM_CUSTOMER MC WHERE TL.LOAN_ID={0} AND TL.ARDB_CD = {1} "
                          + " AND  TL.PARTY_CD=MC.CUST_CD AND TL.ARDB_CD=MC.ARDB_CD";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(loan.loan_id) ? string.Concat("'", loan.loan_id, "'") : "LOAN_ID",
                                          !string.IsNullOrWhiteSpace(loan.ardb_cd) ? string.Concat("'", loan.ardb_cd, "'") : "ardb_cd"
                                           );
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new tm_loan_all();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    d.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                    d.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                    d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                    d.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                    d.prn_limit = UtilityM.CheckNull<decimal>(reader["PRN_LIMIT"]);
                                    d.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                    d.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                    d.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                    d.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                    d.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                    d.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                    d.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                    d.pre_emi_intt = UtilityM.CheckNull<decimal>(reader["PRE_EMI_INTT"]);
                                    d.other_charges = UtilityM.CheckNull<decimal>(reader["OTHER_CHARGES"]);
                                    d.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                                    d.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                                    d.disb_status = UtilityM.CheckNull<string>(reader["DISB_STATUS"]);
                                    d.piriodicity = UtilityM.CheckNull<string>(reader["PIRIODICITY"]);
                                    d.tenure_month = UtilityM.CheckNull<int>(reader["TENURE_MONTH"]);
                                    d.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                                    d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                    d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                    d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                    d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                    d.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                    d.ovd_trf_dt = UtilityM.CheckNull<DateTime>(reader["OVD_TRF_DT"]);
                                    d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                    d.cc_flag = UtilityM.CheckNull<string>(reader["CC_FLAG"]);
                                    d.cheque_facility = UtilityM.CheckNull<string>(reader["CHEQUE_FACILITY"]);
                                    d.intt_calc_type = UtilityM.CheckNull<string>(reader["INTT_CALC_TYPE"]);
                                    d.emi_formula_no = UtilityM.CheckNull<int>(reader["EMI_FORMULA_NO"]);
                                    d.rep_sch_flag = UtilityM.CheckNull<string>(reader["REP_SCH_FLAG"]);
                                    d.loan_close_dt = UtilityM.CheckNull<DateTime>(reader["LOAN_CLOSE_DT"]);
                                    d.loan_status = UtilityM.CheckNull<string>(reader["LOAN_STATUS"]);
                                    d.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                    d.instl_no = UtilityM.CheckNull<int>(reader["INSTL_NO"]);
                                    d.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                    d.activity_dtls = UtilityM.CheckNull<string>(reader["ACTIVITY_DTLS"]);
                                    d.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                                    d.fund_type = UtilityM.CheckNull<string>(reader["FUND_TYPE"]);
                                    d.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                                    d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                    loanRet = d;
                                }
                            }
                        }
                    }
                }
            }
            return loanRet;
        }


        internal List<sm_kcc_param> GetSmKccParam()
        {
            List<sm_kcc_param> mamRets = new List<sm_kcc_param>();
            string _query = " Select PARAM_CD,PARAM_DESC,PARAM_VALUE   from  SM_KCC_PARAMS";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query);
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var mam = new sm_kcc_param();
                                mam.param_cd = UtilityM.CheckNull<string>(reader["PARAM_CD"]);
                                mam.param_desc = UtilityM.CheckNull<string>(reader["PARAM_DESC"]).ToString();
                                mam.param_value = UtilityM.CheckNull<string>(reader["PARAM_VALUE"]);
                                mamRets.Add(mam);
                            }
                        }
                    }
                }

            }
            return mamRets;
        }

        internal string ApproveLoanAccountTranaction(p_gen_param pgp)
        {
            string _ret = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var updateTdDepTransSuccess = 0;
                        string _query1 = "UPDATE TD_DEP_TRANS SET "
                                               + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                               + " MODIFIED_DT            =SYSDATE,"
                                               + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                               + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                               + " APPROVED_DT            =SYSDATE"
                                               + " WHERE (BRN_CD = {3}) AND "
                                               + " (TRANS_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                               + " (  TRANS_CD = {5} ) AND ( ARDB_CD = {6}) ";
                        _statement = string.Format(_query1,
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", "A", "'"),
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", pgp.brn_cd, "'"),
                                string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                string.Concat(pgp.ad_trans_cd),
                                string.Concat(pgp.ardb_cd)
                                );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            updateTdDepTransSuccess = command.ExecuteNonQuery();
                        }
                        if (updateTdDepTransSuccess > 0)
                        {
                            DepTransactionDL _dl1 = new DepTransactionDL();
                            _ret = _dl1.P_TD_DEP_TRANS_LOAN(connection, pgp);
                            if (_ret == "0")
                            {
                                DenominationDL _dl2 = new DenominationDL();
                                _ret = _dl2.P_UPDATE_DENOMINATION(connection, pgp);
                                if (_ret == "0")
                                {
                                    string _query2 = "UPDATE TD_DEP_TRANS_TRF SET "
                                              + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                              + " MODIFIED_DT            =SYSDATE,"
                                              + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                              + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                              + " APPROVED_DT            =SYSDATE"
                                              + " WHERE (BRN_CD = {3}) AND "
                                              + " (TRANS_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                              + " (  TRANS_CD = {5} ) AND (  ARDB_CD = {6} )";
                                    _statement = string.Format(_query2,
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", "A", "'"),
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", pgp.brn_cd, "'"),
                                            string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                            string.Concat(pgp.ad_trans_cd),
                                            string.Concat("'", pgp.ardb_cd, "'")
                                            );
                                    using (var command = OrclDbConnection.Command(connection, _statement))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    string _query = "UPDATE TM_TRANSFER SET "
                                              + " APPROVAL_STATUS        =NVL({0},APPROVAL_STATUS),"
                                              + " APPROVED_BY            =NVL({1},APPROVED_BY    ),"
                                              + " APPROVED_DT            =SYSDATE"
                                              + " WHERE (BRN_CD = {2}) AND "
                                              + " (TRF_DT = to_date('{3}','dd-mm-yyyy' )) AND  "
                                              + " (  TRANS_CD = {4} ) AND (  ARDB_CD = {5} ) ";
                                    _statement = string.Format(_query,
                                            string.Concat("'", "A", "'"),
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", pgp.brn_cd, "'"),
                                            string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                            string.Concat(pgp.ad_trans_cd),
                                            string.Concat("'", pgp.ardb_cd, "'")
                                            );
                                    using (var command = OrclDbConnection.Command(connection, _statement))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    transaction.Commit();
                                    return "0";

                                }
                                else
                                {
                                    transaction.Rollback();
                                    return _ret;
                                }
                            }
                            else
                            {
                                transaction.Rollback();
                                return _ret;
                            }
                        }
                        else
                        {
                            transaction.Rollback();
                            return _ret;
                        }

                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return ex.Message.ToString();
                    }

                }
            }
        }

        internal List<sm_loan_sanction> GetSmLoanSanctionList()
        {
            List<sm_loan_sanction> smLoanSancList =new List<sm_loan_sanction>();

            string _query=" SELECT ACC_CD , PARAM_CD , PARAM_DESC, PARAM_TYPE, FIELD_NAME , DATASET_NO FROM SM_LOAN_SANCTION ORDER BY PARAM_CD";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                _statement = string.Format(_query);
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                               var smLoanSanc = new sm_loan_sanction();

                               smLoanSanc.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                               smLoanSanc.param_cd = UtilityM.CheckNull<string>(reader["PARAM_CD"]);
                               smLoanSanc.param_desc = UtilityM.CheckNull<string>(reader["PARAM_DESC"]);
                               smLoanSanc.param_type = UtilityM.CheckNull<string>(reader["PARAM_TYPE"]);
                               smLoanSanc.field_name = UtilityM.CheckNull<string>(reader["FIELD_NAME"]);
                               smLoanSanc.dataset_no = UtilityM.CheckNull<Int32>(reader["DATASET_NO"]);

                               smLoanSancList.Add(smLoanSanc);
                            }
                        }
                    }
                }
            
            }
            return smLoanSancList;
        }


        internal List<td_loan_sanc> SerializeEntireLoanSancList(List<td_loan_sanc_set> tdLoanSacnSetList) 
        {

            List<td_loan_sanc> tdLoanSancList = new List<td_loan_sanc>();
            td_loan_sanc tdLoanSanc = new td_loan_sanc();

            for (int i = 0; i < tdLoanSacnSetList.Count; i++)
            {
                 tdLoanSancList.AddRange(tdLoanSacnSetList[i].tdloansancset);
            }

            return tdLoanSancList;

            
        }

        internal bool InsertLoanSecurityDtls(DbConnection connection, List<td_loan_sanc> tdLoanSancList )
             {
                     string _queryD=" DELETE FROM TD_LOAN_SANC WHERE ARDB_CD = {0} AND  LOAN_ID = {1}";
                    _statement = string.Format(_queryD,
                                                !string.IsNullOrWhiteSpace(tdLoanSancList[0].ardb_cd) ? string.Concat("'", tdLoanSancList[0].ardb_cd, "'") : "-1",
            !string.IsNullOrWhiteSpace( tdLoanSancList[0].loan_id ) ? string.Concat("'", tdLoanSancList[0].loan_id, "'") : "-1");
                 
                     using (var command = OrclDbConnection.Command(connection, _statement))
                          {                   
                             command.ExecuteNonQuery();
                           }                        
                     
                     string _queryI = "INSERT INTO TD_LOAN_SANC( LOAN_ID , SANC_NO , PARAM_CD , PARAM_VALUE , PARAM_TYPE , DATASET_NO , FIELD_NAME,ARDB_CD,DEL_FLAG)"
                                    + " VALUES ( {0}, {1}, {2}, {3}, {4}, {5}, {6},{7},'N' )";
                    
                     for (int i = 0; i < tdLoanSancList.Count; i++)
                     {
                       _statement = string.Format(_queryI,
                                                  string.Concat("'", tdLoanSancList[i].loan_id, "'"),
                                                  tdLoanSancList[i].sanc_no ,
                                                  string.Concat("'", tdLoanSancList[i].param_cd , "'"),
                                                  string.Concat("'", tdLoanSancList[i].param_value , "'"),
                                                  string.Concat("'", tdLoanSancList[i].param_type , "'"),
                                                  string.Concat("'", tdLoanSancList[i].dataset_no , "'"),
                                                  string.Concat("'", tdLoanSancList[i].field_name , "'"),
                                                  string.Concat("'", tdLoanSancList[i].ardb_cd, "'"));
                 
                         using (var command = OrclDbConnection.Command(connection, _statement))
                         {
                             command.ExecuteNonQuery();
                 
                         }
                     }
                 
                     return true;
                 }

internal List<AccDtlsLov> GetLoanDtls(p_gen_param prm)
        {
            List<AccDtlsLov> accDtlsLovs = new List<AccDtlsLov>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GET_LOAN_DTLS"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm1.Value = prm.ardb_cd;
                    command.Parameters.Add(parm1);
                    var parm2 = new OracleParameter("ad_loan_type", OracleDbType.Decimal, ParameterDirection.Input);
                    parm2.Value = prm.ad_acc_type_cd;
                    command.Parameters.Add(parm2);
                    var parm3 = new OracleParameter("as_cust_name", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm3.Value = prm.as_cust_name;
                    command.Parameters.Add(parm3);
                    var parm4 = new OracleParameter("p_loan_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm4);
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var accDtlsLov = new AccDtlsLov();
                                        accDtlsLov.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        accDtlsLov.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        accDtlsLov.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        accDtlsLov.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        accDtlsLov.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        accDtlsLov.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);

                                        accDtlsLovs.Add(accDtlsLov);
                                    }
                                }
                            }

                        }
                        catch (Exception ex)
                        {

                        }
                    }
                }
            }

            return accDtlsLovs;
        }



        internal List<AccDtlsLov> GetLoanDtls1(p_gen_param prm)
        {
            List<AccDtlsLov> accDtlsLovs = new List<AccDtlsLov>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GET_LOAN_DTLS1"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm1.Value = prm.ardb_cd;
                    command.Parameters.Add(parm1);
                    var parm2 = new OracleParameter("as_cust_name", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm2.Value = prm.as_cust_name;
                    command.Parameters.Add(parm2);
                    var parm3 = new OracleParameter("p_loan_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm3);
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var accDtlsLov = new AccDtlsLov();
                                        accDtlsLov.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        accDtlsLov.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        accDtlsLov.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        accDtlsLov.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        accDtlsLov.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        accDtlsLov.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        accDtlsLov.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);

                                        accDtlsLovs.Add(accDtlsLov);
                                    }
                                }
                            }

                        }
                        catch (Exception ex)
                        {

                        }
                    }
                }
            }

            return accDtlsLovs;
        }





        internal List<AccDtlsLov> GetLoanDtlsByID(p_gen_param prm)
        {
            List<AccDtlsLov> accDtlsLovs = new List<AccDtlsLov>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GET_LOAN_ID_DTLS"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm1.Value = prm.ardb_cd;
                    command.Parameters.Add(parm1);
                    var parm2 = new OracleParameter("as_cust_name", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm2.Value = prm.as_cust_name;
                    command.Parameters.Add(parm2);
                    var parm3 = new OracleParameter("p_loan_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm3);
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var accDtlsLov = new AccDtlsLov();
                                        accDtlsLov.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        accDtlsLov.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        accDtlsLov.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        accDtlsLov.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        accDtlsLov.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        accDtlsLov.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);

                                        accDtlsLovs.Add(accDtlsLov);
                                    }
                                }
                            }

                        }
                        catch (Exception ex)
                        {

                        }
                    }
                }
            }

            return accDtlsLovs;
        }

     internal List<tt_rep_sch> PopulateLoanRepSch(p_loan_param prp)
        {
            List<tt_rep_sch> loanrepschList = new List<tt_rep_sch>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_generate_schedule";
            string _query = " SELECT LOAN_ID,REP_ID,DUE_DT,INSTL_PRN,INSTL_PAID,STATUS "
                            +" FROM TT_REP_SCH WHERE  ARDB_CD = {0} AND LOAN_ID={1} ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prp.ardb_cd;
                            command.Parameters.Add(parm);

                            var parm1 = new OracleParameter("as_loan_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.loan_id;
                            command.Parameters.Add(parm1);

                           
                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query,
                         string.Concat("'", prp.ardb_cd, "'"),
                         string.Concat("'", prp.loan_id, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanrepsch = new tt_rep_sch();

                                        loanrepsch.rep_id = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["REP_ID"]));
                                        loanrepsch.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanrepsch.due_dt = UtilityM.CheckNull<DateTime>(reader["DUE_DT"]);
                                        loanrepsch.instl_prn = UtilityM.CheckNull<decimal>(reader["INSTL_PRN"]);
                                        loanrepsch.instl_paid = UtilityM.CheckNull<decimal>(reader["INSTL_PAID"]);
                                        loanrepsch.status = UtilityM.CheckNull<string>(reader["STATUS"]);
                                        loanrepschList.Add(loanrepsch);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanrepschList = null;
                    }
                }
            }
            return loanrepschList;
        }

        internal List<tt_detailed_list_loan> PopulateLoanDetailedList(p_report_param prp)
        {
            List<tt_detailed_list_loan> loanDtlList = new List<tt_detailed_list_loan>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_detailed_list_loan";
            string _query = "   SELECT TT_DETAILED_LIST_LOAN.ACC_CD,           "
                             + "   TT_DETAILED_LIST_LOAN.PARTY_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN.CURR_INTT_RATE,       "
                             + "   TT_DETAILED_LIST_LOAN.OVD_INTT_RATE,        "
                             + "   TT_DETAILED_LIST_LOAN.CURR_PRN,             "
                             + "   TT_DETAILED_LIST_LOAN.OVD_PRN,              "
                             + "   TT_DETAILED_LIST_LOAN.CURR_INTT,            "
                             + "   TT_DETAILED_LIST_LOAN.OVD_INTT,             "
                             + "   TT_DETAILED_LIST_LOAN.PENAL_INTT,           "
                             + "   TT_DETAILED_LIST_LOAN.ACC_NAME,             "
                             + "   TT_DETAILED_LIST_LOAN.ACC_NUM,              "
                             + "   TT_DETAILED_LIST_LOAN.BLOCK_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN.COMPUTED_TILL_DT,     "
                             + "   TT_DETAILED_LIST_LOAN.LIST_DT               "
                             + "   FROM  TT_DETAILED_LIST_LOAN                 "
                             + "   WHERE TT_DETAILED_LIST_LOAN.ARDB_CD = {0} AND TT_DETAILED_LIST_LOAN.ACC_CD = {1}    "
                             + "   AND  ( TT_DETAILED_LIST_LOAN.CURR_PRN + TT_DETAILED_LIST_LOAN.OVD_PRN ) > 0 ";



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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("ad_acc_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm3.Value = prp.acc_cd;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.adt_dt;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query,
                                                   string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                                   prp.acc_cd != 0 ? Convert.ToString(prp.acc_cd) : "ACC_CD");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_detailed_list_loan();

                                        loanDtl.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.curr_intt_rate = UtilityM.CheckNull<float>(reader["CURR_INTT_RATE"]);
                                        loanDtl.ovd_intt_rate = UtilityM.CheckNull<float>(reader["OVD_INTT_RATE"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanDtl.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.computed_till_dt = UtilityM.CheckNull<DateTime>(reader["COMPUTED_TILL_DT"]);
                                        loanDtl.list_dt = UtilityM.CheckNull<DateTime>(reader["LIST_DT"]);
                                        loanDtlList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }


        internal List<tt_int_subsidy> PopulateInterestSubsidy(p_report_param prp)
        {
            List<tt_int_subsidy> loanDtlList = new List<tt_int_subsidy>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_interest_subsidy";
            string _query = "   SELECT TT_INT_SUBSIDY.LOAN_ID,           "
                             + "   TT_INT_SUBSIDY.PARTY_NAME,           "
                             + "   TT_INT_SUBSIDY.DISB_AMT,       "
                             + "   TT_INT_SUBSIDY.LOAN_BALANCE,        "
                             + "   TT_INT_SUBSIDY.CURR_INTT_RATE,             "
                             + "   TT_INT_SUBSIDY.LOAN_RECOV,              "
                             + "   TT_INT_SUBSIDY.CURR_INTT_RECOV, TT_INT_SUBSIDY.LS_BLOCK,           "
                             + "   TT_INT_SUBSIDY.SUBSIDY_AMT             "
                             + "   FROM  TT_INT_SUBSIDY                 " ;



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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_frdt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_todt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_int_subsidy();

                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.curr_intt_rate = UtilityM.CheckNull<float>(reader["CURR_INTT_RATE"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanDtl.loan_balance = UtilityM.CheckNull<decimal>(reader["LOAN_BALANCE"]);
                                        loanDtl.prn_recov = UtilityM.CheckNull<decimal>(reader["LOAN_RECOV"]);
                                        loanDtl.intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["LS_BLOCK"]);
                                        loanDtl.subsidy_amt = UtilityM.CheckNull<decimal>(reader["SUBSIDY_AMT"]);
                                        
                                        loanDtlList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }


        internal List<tt_detailed_list_loan> PopulateLoanDetailedListAll(p_report_param prp)
        {
            List<tt_detailed_list_loan> loanDtlList = new List<tt_detailed_list_loan>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_detailed_list_loan_all";
            string _query = "   SELECT (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD= TT_DETAILED_LIST_LOAN_ALL.ACC_CD) ACC_TYPE_DESC,           "
                             + "   TT_DETAILED_LIST_LOAN_ALL.PARTY_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN_ALL.CURR_INTT_RATE,       "
                             + "   TT_DETAILED_LIST_LOAN_ALL.OVD_INTT_RATE,        "
                             + "   TT_DETAILED_LIST_LOAN_ALL.CURR_PRN,             "
                             + "   TT_DETAILED_LIST_LOAN_ALL.OVD_PRN,              "
                             + "   TT_DETAILED_LIST_LOAN_ALL.CURR_INTT,            "
                             + "   TT_DETAILED_LIST_LOAN_ALL.OVD_INTT,             "
                             + "   TT_DETAILED_LIST_LOAN_ALL.PENAL_INTT,           "
                             + "   TT_DETAILED_LIST_LOAN_ALL.ACC_NAME,             "
                             + "   TT_DETAILED_LIST_LOAN_ALL.ACC_NUM,              "
                             + "   TT_DETAILED_LIST_LOAN_ALL.BLOCK_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN_ALL.COMPUTED_TILL_DT,     "
                             + "   TT_DETAILED_LIST_LOAN_ALL.LIST_DT,               "
                             + "   TM_LOAN_ALL.LOAN_ACC_NO               "
                             + "   FROM  TT_DETAILED_LIST_LOAN_ALL ,TM_LOAN_ALL                "
                             + "   WHERE TT_DETAILED_LIST_LOAN_ALL.ARDB_CD = {0}   "
                             + "   AND  ( TT_DETAILED_LIST_LOAN_ALL.CURR_PRN + TT_DETAILED_LIST_LOAN_ALL.OVD_PRN ) > 0 "
                             + "   AND  TT_DETAILED_LIST_LOAN_ALL.ARDB_CD = TM_LOAN_ALL.ARDB_CD "
                             + "   AND  TT_DETAILED_LIST_LOAN_ALL.ACC_NUM = TM_LOAN_ALL.LOAN_ID ";



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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                           var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.adt_dt;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query,
                                                   string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_detailed_list_loan();

                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_TYPE_DESC"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.curr_intt_rate = UtilityM.CheckNull<float>(reader["CURR_INTT_RATE"]);
                                        loanDtl.ovd_intt_rate = UtilityM.CheckNull<float>(reader["OVD_INTT_RATE"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.activity_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanDtl.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.computed_till_dt = UtilityM.CheckNull<DateTime>(reader["COMPUTED_TILL_DT"]);
                                        loanDtl.list_dt = UtilityM.CheckNull<DateTime>(reader["LIST_DT"]);
                                        loanDtl.ledger_folio_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                        loanDtlList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }


        internal List<blockwisedisb_type> PopulateLoanDisburseReg(p_report_param prp)
        {
            List<blockwisedisb_type> loanDisReg = new List<blockwisedisb_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.LOAN_ID,   "
                            + "(SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD = A.ACC_CD ) ACC_NAME,                                         "
                            + " B.TRANS_DT,       "
                            + " A.PARTY_CD,       "
                            + " C.CUST_NAME,       "
                            + " A.FUND_TYPE,       "
                            + " A.BRN_CD,         "
                            + "(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD ) BLOCK_NAME, "
                            + "(SELECT ACTIVITY_DESC FROM MM_ACTIVITY WHERE ACTIVITY_CD = A.ACTIVITY_CD ) ACTIVITY_NAME,"
                            + " SUM(B.DISB_AMT) DISB_AMT   "
                            + " FROM TM_LOAN_ALL A , GM_LOAN_TRANS B , MM_CUSTOMER C "
                            + " WHERE   A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID           "
                            + " AND A.PARTY_CD  = C.CUST_CD           "
                            + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy') "
                            + " AND B.TRANS_TYPE = 'B'                "
                            + " AND A.BRN_CD = {2}                    "
                            + " AND A.ARDB_CD = {3}     "
                            + " AND C.BLOCK_CD = {4}     "
                            + " GROUP BY A.LOAN_ID, A.ACC_CD,C.BLOCK_CD, B.TRANS_DT,A.ACTIVITY_CD, A.PARTY_CD, C.CUST_NAME,A.FUND_TYPE, A.BRN_CD  ORDER BY A.ACC_CD,B.TRANS_DT";


            string _query1 = " SELECT DISTINCT C.BLOCK_CD,(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD AND ARDB_CD = A.ARDB_CD) BLOCK_NAME "
                            + " FROM TM_LOAN_ALL A , GM_LOAN_TRANS B , MM_CUSTOMER C "
                            + " WHERE   A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID           "
                            + " AND A.PARTY_CD  = C.CUST_CD           "
                            + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy') "
                            + " AND B.TRANS_TYPE = 'B'                "
                            + " AND A.BRN_CD = {2}                    "
                            + " AND A.ARDB_CD = {3}     " ;





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

                        string _statement1 = string.Format(_query1,
                                         prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        blockwisedisb_type tcaRet1 = new blockwisedisb_type();

                                        var tca = new block_type();
                                        tca.block_cd = UtilityM.CheckNull<string>(reader1["BLOCK_CD"]);
                                        tca.block_name = UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]);


                                        _statement = string.Format(_query,
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                     UtilityM.CheckNull<String>(reader1["BLOCK_CD"])
                                      );

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var loanDis = new tm_loan_all();

                                                        loanDis.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                        loanDis.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        loanDis.acc_desc = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                        loanDis.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                        loanDis.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                                        loanDis.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                        loanDis.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                                        loanDis.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                                        loanDis.fund_type = UtilityM.CheckNull<string>(reader["FUND_TYPE"]);
                                                        loanDis.activity_name = UtilityM.CheckNull<string>(reader["ACTIVITY_NAME"]);
                                                        tcaRet1.tmloanall.Add(loanDis);

                                                        tca.tot_block_curr_prn_recov = tca.tot_block_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                                        tca.tot__block_ovd_prn_recov = tca.tot__block_ovd_prn_recov + 1;

                                                        tcaRet1.blocktype = tca;


                                                       
                                                    }
                                                }
                                            }
                                        }
                                        loanDisReg.Add(tcaRet1);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDisReg = null;
                    }
                }
            }
            return loanDisReg;
        }



        internal List<accwiserecovery_type> PopulateRecoveryRegister(p_report_param prp)
        {
            List<accwiserecovery_type> loanRecoList = new List<accwiserecovery_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.BRN_CD,                       "
               + " A.LOAN_ID,                                        "
               + " (SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD) ACC_NAME,                                         "
               + " A.PARTY_CD,                                       "
               + " C.CUST_NAME,                                      "
               + " A.LAST_INTT_CALC_DT,                              "
               + " B.TRANS_DT,                                       "
               + "(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD) BLOCK_NAME, "
               + " SUM(B.RECOV_AMT) RECOV_AMT,                       "
               + " SUM(B.CURR_PRN_RECOV) CURR_PRN_RECOV,             "
               + " SUM(B.ADV_PRN_RECOV) ADV_PRN_RECOV,             "
               + " SUM(B.OVD_PRN_RECOV) OVD_PRN_RECOV,               "
               + " SUM(B.CURR_INTT_RECOV) CURR_INTT_RECOV,           "
               + " SUM(B.OVD_INTT_RECOV) OVD_INTT_RECOV,              "
               + " SUM(B.PENAL_INTT_RECOV) PENAL_INTT_RECOV          "
               + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
               + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID                       "
               + " AND A.PARTY_CD  = C.CUST_CD                        "
               + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy')"
               + " AND B.TRANS_TYPE = 'R'                            "
               + " AND A.BRN_CD = {2}                                "
               + " AND A.ARDB_CD = {3}                                "
               + " AND A.ACC_CD = {4}                                "
               + " AND C.BLOCK_CD = {5}                                "
               + " GROUP BY A.BRN_CD, A.LOAN_ID, A.ACC_CD, C.CUST_NAME,"
               + " A.PARTY_CD, A.LAST_INTT_CALC_DT, B.TRANS_DT,C.BLOCK_CD     ORDER BY  B.TRANS_DT  ";


            string _query1 = " SELECT DISTINCT A.ACC_CD,(SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD AND ARDB_CD = A.ARDB_CD) ACC_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} ";


            string _query2 = " SELECT DISTINCT C.BLOCK_CD,(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD AND ARDB_CD = A.ARDB_CD) BLOCK_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} "
                             + "  AND A.ACC_CD = {4} ";


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

                        string _statement1 = string.Format(_query1,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        accwiserecovery_type tcaRet1 = new accwiserecovery_type();

                                        var tca = new acc_type();
                                        tca.acc_cd = UtilityM.CheckNull<Int32>(reader1["ACC_CD"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader1["ACC_NAME"]);

                                        tcaRet1.acctype = tca;

                                        string _statement2 = string.Format(_query2,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                         prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                        string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                        UtilityM.CheckNull<Int32>(reader1["ACC_CD"]));

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {

                                                        blockwiserecovery_type tcaRet2 = new blockwiserecovery_type();

                                                        var tca1 = new block_type();
                                                        tca1.block_cd = UtilityM.CheckNull<string>(reader2["BLOCK_CD"]);
                                                        tca1.block_name = UtilityM.CheckNull<string>(reader2["BLOCK_NAME"]);



                                                        _statement = string.Format(_query,
                                                                     prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                                                     string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                                                     UtilityM.CheckNull<Int32>(reader1["ACC_CD"]),
                                                                     UtilityM.CheckNull<String>(reader2["BLOCK_CD"])
                                                                  );

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanReco = new gm_loan_trans();


                                                                        loanReco.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                                        loanReco.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanReco.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                                        loanReco.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                                                        loanReco.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                                        loanReco.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                                                        loanReco.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                                        loanReco.recov_amt = UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        loanReco.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        loanReco.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        loanReco.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        loanReco.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        loanReco.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        loanReco.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);

                                                                        tca1.tot_block_curr_prn_recov = tca1.tot_block_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        tca1.tot__block_ovd_prn_recov = tca1.tot__block_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        tca1.tot__block_adv_prn_recov = tca1.tot__block_adv_prn_recov + UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        tca1.tot_block_curr_intt_recov = tca1.tot_block_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        tca1.tot_block_ovd_intt_recov = tca1.tot_block_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        tca1.tot_block_penal_intt_recov = tca1.tot_block_penal_intt_recov + UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                                                        tca1.tot_block_recov = tca1.tot_block_recov + UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        tca1.tot_count_block = tca1.tot_count_block + 1;
                                                                        tcaRet2.gmloantrans.Add(loanReco);
                                                                        //loanRecoList.Add(loanReco);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        tcaRet2.blocktype = tca1;
                                                        tcaRet1.blockwiserecovery.Add(tcaRet2);//loanRecoList.Add(tcaRet1);
                                                        tca.tot_acc_curr_prn_recov = tca.tot_acc_curr_prn_recov + tca1.tot_block_curr_prn_recov;
                                                        tca.tot_acc_ovd_prn_recov = tca.tot_acc_ovd_prn_recov + tca1.tot__block_ovd_prn_recov;
                                                        tca.tot_acc_adv_prn_recov = tca.tot_acc_adv_prn_recov + tca1.tot__block_adv_prn_recov;
                                                        tca.tot_acc_curr_intt_recov = tca.tot_acc_curr_intt_recov + tca1.tot_block_curr_intt_recov;
                                                        tca.tot_acc_ovd_intt_recov = tca.tot_acc_ovd_intt_recov + tca1.tot_block_ovd_intt_recov;
                                                        tca.tot_acc_penal_intt_recov = tca.tot_acc_penal_intt_recov + tca1.tot_block_penal_intt_recov;
                                                        tca.tot_acc_recov = tca.tot_acc_recov + tca1.tot_block_recov;
                                                        tca.tot_count_acc = tca.tot_count_acc + 1;
                                                    }


                                                }
                                            }
                                        }
                                        loanRecoList.Add(tcaRet1);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRecoList = null;
                    }
                }
            }
            return loanRecoList;
        }



        internal List<accwiserecovery_type> PopulateRecoveryRegisterVillWise(p_report_param prp)
        {
            List<accwiserecovery_type> loanRecoList = new List<accwiserecovery_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.BRN_CD,                       "
               + " A.LOAN_ID,                                        "
               + " (SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD) ACC_NAME,                                         "
               + " A.PARTY_CD,                                       "
               + " C.CUST_NAME,                                      "
               + " A.LAST_INTT_CALC_DT,                              "
               + " B.TRANS_DT,                                       "
               + "(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD) BLOCK_NAME, "
               + " SUM(B.RECOV_AMT) RECOV_AMT,                       "
               + " SUM(B.CURR_PRN_RECOV) CURR_PRN_RECOV,             "
               + " SUM(B.ADV_PRN_RECOV) ADV_PRN_RECOV,             "
               + " SUM(B.OVD_PRN_RECOV) OVD_PRN_RECOV,               "
               + " SUM(B.CURR_INTT_RECOV) CURR_INTT_RECOV,           "
               + " SUM(B.OVD_INTT_RECOV) OVD_INTT_RECOV,              "
               + " SUM(B.PENAL_INTT_RECOV) PENAL_INTT_RECOV          "
               + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
               + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID                       "
               + " AND A.PARTY_CD  = C.CUST_CD                        "
               + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy')"
               + " AND B.TRANS_TYPE = 'R'                            "
               + " AND A.BRN_CD = {2}                                "
               + " AND A.ARDB_CD = {3}                                "
               + " AND A.ACC_CD = {4}                                "
               + " AND C.VILL_CD = {5}                                "
               + " GROUP BY A.BRN_CD, A.LOAN_ID, A.ACC_CD, C.CUST_NAME,"
               + " A.PARTY_CD, A.LAST_INTT_CALC_DT, B.TRANS_DT,C.BLOCK_CD     ORDER BY  B.TRANS_DT  ";


            string _query1 = " SELECT DISTINCT A.ACC_CD,(SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD AND ARDB_CD = A.ARDB_CD) ACC_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} ";


            string _query2 = " SELECT DISTINCT C.VILL_CD,(SELECT min(VILL_NAME) FROM MM_VILL WHERE VILL_CD = C.VILL_CD AND ARDB_CD = A.ARDB_CD) VILL_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} "
                             + "  AND A.ACC_CD = {4} ";


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

                        string _statement1 = string.Format(_query1,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        accwiserecovery_type tcaRet1 = new accwiserecovery_type();

                                        var tca = new acc_type();
                                        tca.acc_cd = UtilityM.CheckNull<Int32>(reader1["ACC_CD"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader1["ACC_NAME"]);

                                        tcaRet1.acctype = tca;

                                        string _statement2 = string.Format(_query2,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                         prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                        string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                        UtilityM.CheckNull<Int32>(reader1["ACC_CD"]));

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {

                                                        blockwiserecovery_type tcaRet2 = new blockwiserecovery_type();

                                                        var tca1 = new block_type();
                                                        tca1.block_cd = UtilityM.CheckNull<string>(reader2["VILL_CD"]);
                                                        tca1.block_name = UtilityM.CheckNull<string>(reader2["VILL_NAME"]);



                                                        _statement = string.Format(_query,
                                                                     prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                                                     string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                                                     UtilityM.CheckNull<Int32>(reader1["ACC_CD"]),
                                                                     UtilityM.CheckNull<String>(reader2["VILL_CD"])
                                                                  );

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanReco = new gm_loan_trans();


                                                                        loanReco.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                                        loanReco.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanReco.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                                        loanReco.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                                                        loanReco.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                                        loanReco.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                                                        loanReco.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                                        loanReco.recov_amt = UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        loanReco.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        loanReco.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        loanReco.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        loanReco.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        loanReco.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        loanReco.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);

                                                                        tca1.tot_block_curr_prn_recov = tca1.tot_block_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        tca1.tot__block_ovd_prn_recov = tca1.tot__block_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        tca1.tot__block_adv_prn_recov = tca1.tot__block_adv_prn_recov + UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        tca1.tot_block_curr_intt_recov = tca1.tot_block_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        tca1.tot_block_ovd_intt_recov = tca1.tot_block_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        tca1.tot_block_penal_intt_recov = tca1.tot_block_penal_intt_recov + UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                                                        tca1.tot_block_recov = tca1.tot_block_recov + UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        tca1.tot_count_block = tca1.tot_count_block + 1;
                                                                        tcaRet2.gmloantrans.Add(loanReco);
                                                                        //loanRecoList.Add(loanReco);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        tcaRet2.blocktype = tca1;
                                                        tcaRet1.blockwiserecovery.Add(tcaRet2);//loanRecoList.Add(tcaRet1);
                                                        tca.tot_acc_curr_prn_recov = tca.tot_acc_curr_prn_recov + tca1.tot_block_curr_prn_recov;
                                                        tca.tot_acc_ovd_prn_recov = tca.tot_acc_ovd_prn_recov + tca1.tot__block_ovd_prn_recov;
                                                        tca.tot_acc_adv_prn_recov = tca.tot_acc_adv_prn_recov + tca1.tot__block_adv_prn_recov;
                                                        tca.tot_acc_curr_intt_recov = tca.tot_acc_curr_intt_recov + tca1.tot_block_curr_intt_recov;
                                                        tca.tot_acc_ovd_intt_recov = tca.tot_acc_ovd_intt_recov + tca1.tot_block_ovd_intt_recov;
                                                        tca.tot_acc_penal_intt_recov = tca.tot_acc_penal_intt_recov + tca1.tot_block_penal_intt_recov;
                                                        tca.tot_acc_recov = tca.tot_acc_recov + tca1.tot_block_recov;
                                                        tca.tot_count_acc = tca.tot_count_acc + 1;
                                                    }


                                                }
                                            }
                                        }
                                        loanRecoList.Add(tcaRet1);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRecoList = null;
                    }
                }
            }
            return loanRecoList;
        }



        internal List<accwiserecovery_type> PopulateRecoveryRegisterFundwise(p_report_param prp)
        {
            List<accwiserecovery_type> loanRecoList = new List<accwiserecovery_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.BRN_CD,                       "
               + " A.LOAN_ID,                                        "
               + " (SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD) ACC_NAME,                                         "
               + " A.PARTY_CD,                                       "
               + " C.CUST_NAME,                                      "
               + " A.LAST_INTT_CALC_DT,                              "
               + " B.TRANS_DT,                                       "
               + "(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD) BLOCK_NAME, "
               + " SUM(B.RECOV_AMT) RECOV_AMT,                       "
               + " SUM(B.CURR_PRN_RECOV) CURR_PRN_RECOV,             "
               + " SUM(B.ADV_PRN_RECOV) ADV_PRN_RECOV,             "
               + " SUM(B.OVD_PRN_RECOV) OVD_PRN_RECOV,               "
               + " SUM(B.CURR_INTT_RECOV) CURR_INTT_RECOV,           "
               + " SUM(B.OVD_INTT_RECOV) OVD_INTT_RECOV,              "
               + " SUM(B.PENAL_INTT_RECOV) PENAL_INTT_RECOV          "
               + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
               + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID                       "
               + " AND A.PARTY_CD  = C.CUST_CD                        "
               + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy')"
               + " AND B.TRANS_TYPE = 'R'                            "
               + " AND A.BRN_CD = {2}                                "
               + " AND A.ARDB_CD = {3}                                "
               + " AND A.ACC_CD = {4}                                "
               + " AND C.BLOCK_CD = {5}                                "
               + " AND A.FUND_TYPE = {6}                                "
               + " GROUP BY A.BRN_CD, A.LOAN_ID, A.ACC_CD, C.CUST_NAME,"
               + " A.PARTY_CD, A.LAST_INTT_CALC_DT, B.TRANS_DT,C.BLOCK_CD     ORDER BY  B.TRANS_DT  ";


            string _query1 = " SELECT DISTINCT A.ACC_CD,(SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD AND ARDB_CD = A.ARDB_CD) ACC_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} "
                             + "  AND A.FUND_TYPE = {4} ";


            string _query2 = " SELECT DISTINCT C.BLOCK_CD,(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD AND ARDB_CD = A.ARDB_CD) BLOCK_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} "
                             + "  AND A.ACC_CD = {4} "
                             + "  AND A.FUND_TYPE = {5} " ;


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

                        string _statement1 = string.Format(_query1,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.fund_type) ? "FUND_TYPE" : string.Concat("'", prp.fund_type, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        accwiserecovery_type tcaRet1 = new accwiserecovery_type();

                                        var tca = new acc_type();
                                        tca.acc_cd = UtilityM.CheckNull<Int32>(reader1["ACC_CD"]);
                                        tca.acc_name = UtilityM.CheckNull<string>(reader1["ACC_NAME"]);

                                        tcaRet1.acctype = tca;

                                        string _statement2 = string.Format(_query2,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                         prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                        string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                        UtilityM.CheckNull<Int32>(reader1["ACC_CD"]),
                                       string.IsNullOrWhiteSpace(prp.fund_type) ? "FUND_TYPE" : string.Concat("'", prp.fund_type, "'"));

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {

                                                        blockwiserecovery_type tcaRet2 = new blockwiserecovery_type();

                                                        var tca1 = new block_type();
                                                        tca1.block_cd = UtilityM.CheckNull<string>(reader2["BLOCK_CD"]);
                                                        tca1.block_name = UtilityM.CheckNull<string>(reader2["BLOCK_NAME"]);



                                                        _statement = string.Format(_query,
                                                                     prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                                                     string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                                                     UtilityM.CheckNull<Int32>(reader1["ACC_CD"]),
                                                                     UtilityM.CheckNull<String>(reader2["BLOCK_CD"]),
                                                                     string.IsNullOrWhiteSpace(prp.fund_type) ? "FUND_TYPE" : string.Concat("'", prp.fund_type, "'")
                                                                  );

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanReco = new gm_loan_trans();


                                                                        loanReco.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                                        loanReco.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanReco.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                                        loanReco.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                                                        loanReco.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                                        loanReco.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                                                        loanReco.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                                        loanReco.recov_amt = UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        loanReco.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        loanReco.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        loanReco.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        loanReco.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        loanReco.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        loanReco.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);

                                                                        tca1.tot_block_curr_prn_recov = tca1.tot_block_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        tca1.tot__block_ovd_prn_recov = tca1.tot__block_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        tca1.tot__block_adv_prn_recov = tca1.tot__block_adv_prn_recov + UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        tca1.tot_block_curr_intt_recov = tca1.tot_block_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        tca1.tot_block_ovd_intt_recov = tca1.tot_block_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        tca1.tot_block_penal_intt_recov = tca1.tot_block_penal_intt_recov + UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                                                        tca1.tot_block_recov = tca1.tot_block_recov + UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        tca1.tot_count_block = tca1.tot_count_block + 1;
                                                                        tcaRet2.gmloantrans.Add(loanReco);
                                                                        //loanRecoList.Add(loanReco);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        tcaRet2.blocktype = tca1;
                                                        tcaRet1.blockwiserecovery.Add(tcaRet2);//loanRecoList.Add(tcaRet1);
                                                        tca.tot_acc_curr_prn_recov = tca.tot_acc_curr_prn_recov + tca1.tot_block_curr_prn_recov;
                                                        tca.tot_acc_ovd_prn_recov = tca.tot_acc_ovd_prn_recov + tca1.tot__block_ovd_prn_recov;
                                                        tca.tot_acc_adv_prn_recov = tca.tot_acc_adv_prn_recov + tca1.tot__block_adv_prn_recov;
                                                        tca.tot_acc_curr_intt_recov = tca.tot_acc_curr_intt_recov + tca1.tot_block_curr_intt_recov;
                                                        tca.tot_acc_ovd_intt_recov = tca.tot_acc_ovd_intt_recov + tca1.tot_block_ovd_intt_recov;
                                                        tca.tot_acc_penal_intt_recov = tca.tot_acc_penal_intt_recov + tca1.tot_block_penal_intt_recov;
                                                        tca.tot_acc_recov = tca.tot_acc_recov + tca1.tot_block_recov;
                                                        tca.tot_count_acc = tca.tot_count_acc + 1;
                                                    }


                                                }
                                            }
                                        }
                                        loanRecoList.Add(tcaRet1);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRecoList = null;
                    }
                }
            }
            return loanRecoList;
        }




        internal List<blockwiserecovery_type> PopulateRecoveryRegisterFundwiseBlockwise(p_report_param prp)
        {
            List<blockwiserecovery_type> loanRecoList = new List<blockwiserecovery_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.BRN_CD,                       "
               + " A.LOAN_ID,                                        "
               + " (SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD) ACC_NAME,                                         "
               + " A.PARTY_CD,                                       "
               + " C.CUST_NAME,                                      "
               + " A.LAST_INTT_CALC_DT,                              "
               + " B.TRANS_DT,                                       "
               + "(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD) BLOCK_NAME, "
               + " SUM(B.RECOV_AMT) RECOV_AMT,                       "
               + " SUM(B.CURR_PRN_RECOV) CURR_PRN_RECOV,             "
               + " SUM(B.ADV_PRN_RECOV) ADV_PRN_RECOV,             "
               + " SUM(B.OVD_PRN_RECOV) OVD_PRN_RECOV,               "
               + " SUM(B.CURR_INTT_RECOV) CURR_INTT_RECOV,           "
               + " SUM(B.OVD_INTT_RECOV) OVD_INTT_RECOV,              "
               + " SUM(B.PENAL_INTT_RECOV) PENAL_INTT_RECOV          "
               + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
               + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID                       "
               + " AND A.PARTY_CD  = C.CUST_CD                        "
               + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy')"
               + " AND B.TRANS_TYPE = 'R'                            "
               + " AND A.BRN_CD = {2}                                "
               + " AND A.ARDB_CD = {3}                                "
               + " AND C.BLOCK_CD = {4}                                "
               + " AND A.FUND_TYPE = {5}                                "
               + " GROUP BY A.BRN_CD, A.LOAN_ID, A.ACC_CD, C.CUST_NAME,"
               + " A.PARTY_CD, A.LAST_INTT_CALC_DT, B.TRANS_DT,C.BLOCK_CD     ORDER BY  B.TRANS_DT  ";


            string _query2 = " SELECT DISTINCT C.BLOCK_CD,(SELECT BLOCK_NAME FROM MM_BLOCK WHERE BLOCK_CD = C.BLOCK_CD AND ARDB_CD = A.ARDB_CD) BLOCK_NAME "
                             + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
                             + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID "
                             + "  AND A.PARTY_CD = C.CUST_CD "
                             + "  AND B.TRANS_DT BETWEEN to_date({0}, 'dd-mm-yyyy') AND to_date({1}, 'dd-mm-yyyy') "
                             + "  AND B.TRANS_TYPE = 'R' "
                             + "  AND A.BRN_CD = {2} "
                             + "  AND A.ARDB_CD = {3} "
                             + "  AND A.FUND_TYPE = {4} ";


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
                        
                                        string _statement2 = string.Format(_query2,
                                         prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                         prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "TRANS_DT",
                                        string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.fund_type) ? "FUND_TYPE" : string.Concat("'", prp.fund_type, "'"));

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {

                                                        blockwiserecovery_type tcaRet2 = new blockwiserecovery_type();

                                                        var tca1 = new block_type();
                                                        tca1.block_cd = UtilityM.CheckNull<string>(reader2["BLOCK_CD"]);
                                                        tca1.block_name = UtilityM.CheckNull<string>(reader2["BLOCK_NAME"]);



                                                        _statement = string.Format(_query,
                                                                     prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                                                     string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                                                     string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                                                     UtilityM.CheckNull<String>(reader2["BLOCK_CD"]),
                                                                     string.IsNullOrWhiteSpace(prp.fund_type) ? "FUND_TYPE" : string.Concat("'", prp.fund_type, "'")
                                                                  );

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanReco = new gm_loan_trans();


                                                                        loanReco.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                                        loanReco.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanReco.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                                        loanReco.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                                                        loanReco.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                                        loanReco.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                                                        loanReco.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                                        loanReco.recov_amt = UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        loanReco.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        loanReco.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        loanReco.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        loanReco.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        loanReco.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        loanReco.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);

                                                                        tca1.tot_block_curr_prn_recov = tca1.tot_block_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                                                        tca1.tot__block_ovd_prn_recov = tca1.tot__block_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                                                        tca1.tot__block_adv_prn_recov = tca1.tot__block_adv_prn_recov + UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                                                        tca1.tot_block_curr_intt_recov = tca1.tot_block_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                                                        tca1.tot_block_ovd_intt_recov = tca1.tot_block_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                                                        tca1.tot_block_penal_intt_recov = tca1.tot_block_penal_intt_recov + UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                                                        tca1.tot_block_recov = tca1.tot_block_recov + UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                                                        tca1.tot_count_block = tca1.tot_count_block + 1;
                                                                        tcaRet2.gmloantrans.Add(loanReco);
                                                                        //loanRecoList.Add(loanReco);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        tcaRet2.blocktype = tca1;
                                                        loanRecoList.Add(tcaRet2);
                                                    }
                                                }
                                            }
                                        }                                   

                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRecoList = null;
                    }
                }
            }
            return loanRecoList;
        }




        internal List<tm_loan_all> PopulateLoanDisburseRegAccwise(p_report_param prp)
        {
            List<tm_loan_all> loanDisReg = new List<tm_loan_all>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.LOAN_ID,   "
                            + " A.ACC_CD,         "
                            + " B.TRANS_DT,       "
                            + " A.PARTY_CD,       "
                            + " C.CUST_NAME,       "
                            + " A.BRN_CD,         "
                            + " SUM(B.DISB_AMT) DISB_AMT   "
                            + " FROM TM_LOAN_ALL A , GM_LOAN_TRANS B , MM_CUSTOMER C "
                            + " WHERE   A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID           "
                            + " AND A.PARTY_CD  = C.CUST_CD           "
                            + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy') "
                            + " AND B.TRANS_TYPE = 'B'                "
                            + " AND A.BRN_CD = {2}                    "
                            + " AND A.ARDB_CD = {3}     "
                            + " AND A.ACC_CD = {4}     "
                            + " GROUP BY A.LOAN_ID, A.ACC_CD, B.TRANS_DT, A.PARTY_CD, C.CUST_NAME, A.BRN_CD  ORDER BY B.TRANS_DT";



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
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                      prp.acc_cd != 0 ? Convert.ToString(prp.acc_cd) : "ACC_CD");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDis = new tm_loan_all();

                                        loanDis.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDis.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanDis.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        loanDis.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanDis.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                        loanDis.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        loanDis.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanDisReg.Add(loanDis);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDisReg = null;
                    }
                }
            }
            return loanDisReg;
        }



        


        internal List<gm_loan_trans> PopulateRecoveryRegisterAccwise(p_report_param prp)
        {
            List<gm_loan_trans> loanRecoList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.BRN_CD,                       "
               + " A.LOAN_ID,                                        "
               + " (SELECT ACC_NAME FROM M_ACC_MASTER WHERE ACC_CD =A.ACC_CD) ACC_NAME,                                         "
               + " A.PARTY_CD,                                       "
               + " C.CUST_NAME,                                      "
               + " A.LAST_INTT_CALC_DT,                              "
               + " B.TRANS_DT,                                       "
               + " SUM(B.RECOV_AMT) RECOV_AMT,                       "
               + " SUM(B.CURR_PRN_RECOV) CURR_PRN_RECOV,             "
               + " SUM(B.ADV_PRN_RECOV) ADV_PRN_RECOV,             "
               + " SUM(B.OVD_PRN_RECOV) OVD_PRN_RECOV,               "
               + " SUM(B.CURR_INTT_RECOV) CURR_INTT_RECOV,           "
               + " SUM(B.OVD_INTT_RECOV) OVD_INTT_RECOV,              "
               + " SUM(B.PENAL_INTT_RECOV) PENAL_INTT_RECOV          "
               + " FROM TM_LOAN_ALL A, GM_LOAN_TRANS B, MM_CUSTOMER C "
               + " WHERE A.ARDB_CD = B.ARDB_CD AND A.ARDB_CD = C.ARDB_CD AND A.LOAN_ID = B.LOAN_ID                       "
               + " AND A.PARTY_CD  = C.CUST_CD                        "
               + " AND B.TRANS_DT BETWEEN to_date('{0}', 'dd-mm-yyyy') AND to_date('{1}', 'dd-mm-yyyy')"
               + " AND B.TRANS_TYPE = 'R'                            "
               + " AND A.BRN_CD = {2}                                "
               + " AND A.ARDB_CD = {3}                                "
               + " AND A.ACC_CD = {4}                                "
               + " GROUP BY A.BRN_CD, A.LOAN_ID, A.ACC_CD, C.CUST_NAME,"
               + " A.PARTY_CD, A.LAST_INTT_CALC_DT, B.TRANS_DT   ORDER BY  B.TRANS_DT   ";

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
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                      prp.acc_cd != 0 ? Convert.ToString(prp.acc_cd) : "ACC_CD");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanReco = new gm_loan_trans();


                                        loanReco.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        loanReco.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanReco.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanReco.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                        loanReco.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanReco.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                        loanReco.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanReco.recov_amt = UtilityM.CheckNull<decimal>(reader["RECOV_AMT"]);
                                        loanReco.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        loanReco.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                        loanReco.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        loanReco.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanReco.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanReco.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);

                                        loanRecoList.Add(loanReco);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRecoList = null;
                    }
                }
            }
            return loanRecoList;
        }        



        internal List<gm_loan_trans> PopulateLoanStatement(p_report_param prp)
        {
            List<gm_loan_trans> loanStmtList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = "SELECT GM_LOAN_TRANS.LOAN_ID, "
                + " GM_LOAN_TRANS.TRANS_DT,             "
                + " GM_LOAN_TRANS.TRANS_CD,             "
                + " GM_LOAN_TRANS.DISB_AMT,             "
                + " GM_LOAN_TRANS.CURR_PRN_RECOV,       "
                + " GM_LOAN_TRANS.ADV_PRN_RECOV,       "
                + " GM_LOAN_TRANS.OVD_PRN_RECOV,        "
                + " GM_LOAN_TRANS.CURR_INTT_RECOV,      "
                + " GM_LOAN_TRANS.OVD_INTT_RECOV,       "
                + " GM_LOAN_TRANS.PENAL_INTT_RECOV,       "
                + " GM_LOAN_TRANS.LAST_INTT_CALC_DT,    "
                + " GM_LOAN_TRANS.CURR_INTT_CALCULATED, "
                + " GM_LOAN_TRANS.OVD_INTT_CALCULATED,  "
                + " GM_LOAN_TRANS.PENAL_INTT_CALCULATED,  "
                + " GM_LOAN_TRANS.PRN_TRF,              "
                + " GM_LOAN_TRANS.INTT_TRF,             "
                + " GM_LOAN_TRANS.PRN_TRF_REVERT,       "
                + " GM_LOAN_TRANS.INTT_TRF_REVERT,      "
                + " GM_LOAN_TRANS.CURR_PRN,             "
                + " GM_LOAN_TRANS.OVD_PRN,              "
                + " GM_LOAN_TRANS.CURR_INTT,            "
                + " GM_LOAN_TRANS.OVD_INTT ,            "
                + " GM_LOAN_TRANS.PENAL_INTT ,            "
                + " C.CUST_NAME                         "
                + " FROM GM_LOAN_TRANS , TM_LOAN_ALL A ,  MM_CUSTOMER C "
                + " WHERE GM_LOAN_TRANS.ARDB_CD = {0}   "
                + " AND GM_LOAN_TRANS.LOAN_ID = {1}     "
                + " AND (GM_LOAN_TRANS.TRANS_TYPE) IN ('B', 'R', 'I','O') "
                + " AND GM_LOAN_TRANS.TRANS_DT BETWEEN to_date('{2}', 'dd-mm-yyyy') AND to_date('{3}', 'dd-mm-yyyy') "
                + " AND GM_LOAN_TRANS.ARDB_CD = A.ARDB_CD "
                + " AND GM_LOAN_TRANS.ARDB_CD = C.ARDB_CD "
                + " AND GM_LOAN_TRANS.LOAN_ID = A.LOAN_ID "
                + " AND A.PARTY_CD = C.CUST_CD  ORDER BY GM_LOAN_TRANS.TRANS_DT,GM_LOAN_TRANS.TRANS_CD";


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
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.loan_id) ? "LOAN_ID" : string.Concat("'", prp.loan_id, "'"),
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanStmt = new gm_loan_trans();

                                        loanStmt.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanStmt.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanStmt.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanStmt.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                        loanStmt.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanStmt.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        loanStmt.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                        loanStmt.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        loanStmt.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanStmt.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanStmt.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        loanStmt.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                        loanStmt.curr_intt_calculated = UtilityM.CheckNull<decimal>(reader["CURR_INTT_CALCULATED"]);
                                        loanStmt.ovd_intt_calculated = UtilityM.CheckNull<decimal>(reader["OVD_INTT_CALCULATED"]);
                                        loanStmt.penal_intt_calculated = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_CALCULATED"]);
                                        loanStmt.prn_trf = UtilityM.CheckNull<decimal>(reader["PRN_TRF"]);
                                        loanStmt.intt_trf = UtilityM.CheckNull<decimal>(reader["INTT_TRF"]);
                                        loanStmt.prn_trf_revert = UtilityM.CheckNull<decimal>(reader["PRN_TRF_REVERT"]);
                                        loanStmt.intt_trf_revert = UtilityM.CheckNull<decimal>(reader["INTT_TRF_REVERT"]);
                                        loanStmt.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanStmt.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanStmt.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanStmt.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanStmt.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);

                                        loanStmtList.Add(loanStmt);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanStmtList = null;
                    }
                }
            }
            return loanStmtList;
        }


        internal List<gm_loan_trans> PopulateLoanStatementBmardb(p_report_param prp)
        {
            List<gm_loan_trans> loanStmtList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = "SELECT a.TRANS_DT,"
                            + " a.ISSUE_AMT, "
                            + "a.CURR_PRN_RECOV,"
                            + "a.OVD_PRN_RECOV,"
                            + "a.ADV_PRN_RECOV,"
                            + "a.CURR_INTT_RECOV,"
                            + "a.OVD_INTT_RECOV,"
                            + "a.PENAL_INTT_RECOV,"
                            + "a.CURR_PRN_BAL + a.OVD_PRN_BAL PRN_BAL,"
                            + "a.CURR_INTT_BAL + a.OVD_INTT_BAL + a.PENAL_INTT_BAL INTT_BAL,"
                            + "a.PARTICULARS "
                            + "FROM MD_LOAN_PASSBOOK a"
                            + " WHERE a.ARDB_CD = {0} "
                            + "AND a.LOAN_ID = {1} "
                            + "AND a.TRANS_DT BETWEEN to_date('{2}', 'dd-mm-yyyy') AND to_date('{3}', 'dd-mm-yyyy') "
                            + "ORDER BY a.TRANS_DT,a.TRANS_CD";

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
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.loan_id) ? "LOAN_ID" : string.Concat("'", prp.loan_id, "'"),
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanStmt = new gm_loan_trans();

                                        loanStmt.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanStmt.disb_amt = UtilityM.CheckNull<decimal>(reader["ISSUE_AMT"]);                                       
                                        loanStmt.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        loanStmt.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                        loanStmt.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        loanStmt.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanStmt.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanStmt.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        loanStmt.curr_prn = UtilityM.CheckNull<decimal>(reader["PRN_BAL"]);
                                        loanStmt.curr_intt = UtilityM.CheckNull<decimal>(reader["INTT_BAL"]);
                                        loanStmt.acc_typ_dsc = UtilityM.CheckNull<string>(reader["PARTICULARS"]);

                                        loanStmtList.Add(loanStmt);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanStmtList = null;
                    }
                }
            }
            return loanStmtList;
        }


        internal List<gm_loan_trans> PopulateOvdTrfDtls(p_report_param prp)
        {
            List<gm_loan_trans> loanStmtList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = "SELECT GM_LOAN_TRANS.LOAN_ID, "
                + " GM_LOAN_TRANS.TRANS_DT,             "
                + " GM_LOAN_TRANS.PRN_TRF,              "
                + " GM_LOAN_TRANS.INTT_TRF,             "
                + " GM_LOAN_TRANS.CURR_PRN,             "
                + " GM_LOAN_TRANS.OVD_PRN,              "
                + " GM_LOAN_TRANS.CURR_INTT,            "
                + " GM_LOAN_TRANS.OVD_INTT ,            "
                + " GM_LOAN_TRANS.PENAL_INTT ,            "
                + " C.CUST_NAME                         "
                + " FROM GM_LOAN_TRANS , TM_LOAN_ALL A ,  MM_CUSTOMER C "
                + " WHERE GM_LOAN_TRANS.ARDB_CD = {0}   "
                + " AND GM_LOAN_TRANS.BRN_CD = {1}     "
                + " AND GM_LOAN_TRANS.TRANS_TYPE = 'O' "
                + " AND GM_LOAN_TRANS.TRANS_DT BETWEEN to_date('{2}', 'dd-mm-yyyy') AND to_date('{3}', 'dd-mm-yyyy') "
                + " AND GM_LOAN_TRANS.ARDB_CD = A.ARDB_CD "
                + " AND GM_LOAN_TRANS.ARDB_CD = C.ARDB_CD "
                + " AND GM_LOAN_TRANS.LOAN_ID = A.LOAN_ID "
                + " AND A.PARTY_CD = C.CUST_CD  ORDER BY GM_LOAN_TRANS.TRANS_DT,GM_LOAN_TRANS.TRANS_CD";


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
                                      string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ARDB_CD" : string.Concat("'", prp.ardb_cd, "'"),
                                      string.IsNullOrWhiteSpace(prp.brn_cd) ? "BRN_CD" : string.Concat("'", prp.brn_cd, "'"),
                                      prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "TRANS_DT",
                                      prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "TRANS_DT");

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanStmt = new gm_loan_trans();

                                        loanStmt.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanStmt.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanStmt.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanStmt.prn_trf = UtilityM.CheckNull<decimal>(reader["PRN_TRF"]);
                                        loanStmt.intt_trf = UtilityM.CheckNull<decimal>(reader["INTT_TRF"]);
                                        loanStmt.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanStmt.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanStmt.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanStmt.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanStmt.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);

                                        loanStmtList.Add(loanStmt);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanStmtList = null;
                    }
                }
            }
            return loanStmtList;
        }

        internal List<accwiseloansubcashbook> PopulateLoanSubCashBook(p_report_param prp)
        {
            List<accwiseloansubcashbook> loanSubCashBookList = new List<accwiseloansubcashbook>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_sub_csh_bk_loan";
            string _query = " SELECT DISTINCT TT_LOAN_SUB_CASH_BOOK.ACC_TYPE_CD,"
                +" TT_LOAN_SUB_CASH_BOOK.ACC_DESC, "
               + " TT_LOAN_SUB_CASH_BOOK.ACC_NUM,  "
               + " TT_LOAN_SUB_CASH_BOOK.CASH_DR,  "
               + " TT_LOAN_SUB_CASH_BOOK.TRF_DR,   "
               + " TT_LOAN_SUB_CASH_BOOK.CASH_CR,  "
               + " TT_LOAN_SUB_CASH_BOOK.TRF_CR,   "
               + " TT_LOAN_SUB_CASH_BOOK.CUST_NAME "
               + " FROM TT_LOAN_SUB_CASH_BOOK   "
               + " WHERE TT_LOAN_SUB_CASH_BOOK.ACC_TYPE_CD = {0}";

            string _query2 = " SELECT DISTINCT TT_LOAN_SUB_CASH_BOOK.ACC_TYPE_CD,"
                + " TT_LOAN_SUB_CASH_BOOK.ACC_DESC "
                + " FROM TT_LOAN_SUB_CASH_BOOK   ORDER BY TT_LOAN_SUB_CASH_BOOK.ACC_TYPE_CD   ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_as_on_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.adt_as_on_dt;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();

                        }

                        string _statement1 = string.Format(_query2);

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        accwiseloansubcashbook tcaRet1 = new accwiseloansubcashbook();

                                        var tca1 = new acc_type();
                                        tca1.acc_type_cd = UtilityM.CheckNull<int>(reader1["ACC_TYPE_CD"]);
                                        tca1.acc_name = UtilityM.CheckNull<string>(reader1["ACC_DESC"]);


                                        _statement = string.Format(_query, UtilityM.CheckNull<int>(reader1["ACC_TYPE_CD"]));

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var loanSubCashBook = new tt_loan_sub_cash_book();

                                                        loanSubCashBook.acc_type_cd = Convert.ToInt32(UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]));
                                                        loanSubCashBook.acc_typ_dsc = UtilityM.CheckNull<string>(reader["ACC_DESC"]);
                                                        loanSubCashBook.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        loanSubCashBook.cash_dr = UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        loanSubCashBook.trf_dr = UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        loanSubCashBook.cash_cr = UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        loanSubCashBook.trf_cr = UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        loanSubCashBook.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);

                                                        tca1.tot_acc_curr_prn_recov = tca1.tot_acc_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        tca1.tot_acc_curr_intt_recov = tca1.tot_acc_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        tca1.tot_acc_ovd_prn_recov = tca1.tot_acc_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        tca1.tot_acc_ovd_intt_recov = tca1.tot_acc_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        tca1.tot_count_acc = tca1.tot_count_acc + 1;

                                                        tcaRet1.ttloansubcashbook.Add(loanSubCashBook);

                                                    }
                                                    //transaction.Commit();                                                
                                                }
                                            }
                                        }
                                     tcaRet1.acctype = tca1;
                                    loanSubCashBookList.Add(tcaRet1);
                                    }
                                    
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanSubCashBookList = null;
                    }
                }
            }
            return loanSubCashBookList;
        }

        internal List<demand_notice> GetDemandNotice(p_report_param prp)
        {
            List<demand_notice> loanDtlList = new List<demand_notice>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "P_DUE_NOTICE";
            string _query = " SELECT TT_NOTIS1.LOAN_ID,           "
                             + "   TT_NOTIS1.CUST_NAME,           "
                             + "   TT_NOTIS1.CUST_ADDRESS,           "
                             + "   TT_NOTIS1.BLOCK_NAME,           "
                             + "   TT_NOTIS1.ACTIVITY_NAME,           "
                             + "   TT_NOTIS1.CURR_INTT_RATE,       "
                             + "   TT_NOTIS1.OVD_INTT_RATE,        "
                             + "   TT_NOTIS1.DUE_PRN,             "
                             + "   TT_NOTIS1.CURR_PRN,             "
                             + "   TT_NOTIS1.OVD_PRN,              "
                             + "   TT_NOTIS1.CURR_INTT,            "
                             + "   TT_NOTIS1.OVD_INTT,             "
                             + "   TT_NOTIS1.PENAL_INTT,           "
                             + "   TT_NOTIS1.LEDGER_NO,             "
                             + "   TT_NOTIS1.SANC_AMT             "
                           + " FROM TT_NOTIS1                      ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.to_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_dt", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.loan_id;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new demand_notice();

                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanDtl.cust_address = UtilityM.CheckNull<string>(reader["CUST_ADDRESS"]);
                                        loanDtl.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                                        loanDtl.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.activity_name = UtilityM.CheckNull<string>(reader["ACTIVITY_NAME"]);
                                        loanDtl.due_prn = UtilityM.CheckNull<decimal>(reader["DUE_PRN"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.loan_case_no = UtilityM.CheckNull<string>(reader["LEDGER_NO"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);

                                        loanDtlList.Add(loanDtl);
                                    }
                                }

                                transaction.Commit();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }


        internal List<tt_detailed_list_loan> GetDefaultList(p_report_param prp)
        {
            List<tt_detailed_list_loan> loanDtlList = new List<tt_detailed_list_loan>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_detailed_list_loan";
            string _query = " SELECT TT_DETAILED_LIST_LOAN.ACC_CD,           "
                             + "   TT_DETAILED_LIST_LOAN.PARTY_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN.CURR_INTT_RATE,       "
                             + "   TT_DETAILED_LIST_LOAN.OVD_INTT_RATE,        "
                             + "   TT_DETAILED_LIST_LOAN.CURR_PRN,             "
                             + "   TT_DETAILED_LIST_LOAN.OVD_PRN,              "
                             + "   TT_DETAILED_LIST_LOAN.CURR_INTT,            "
                             + "   TT_DETAILED_LIST_LOAN.OVD_INTT,             "
                             + "   TT_DETAILED_LIST_LOAN.PENAL_INTT,           "
                             + "   TT_DETAILED_LIST_LOAN.ACC_NAME,             "
                             + "   TT_DETAILED_LIST_LOAN.ACC_NUM,              "
                             + "   TT_DETAILED_LIST_LOAN.BLOCK_NAME,           "
                             + "   TT_DETAILED_LIST_LOAN.COMPUTED_TILL_DT,     "
                             + "   TT_DETAILED_LIST_LOAN.LIST_DT               "
                           + " FROM TT_DETAILED_LIST_LOAN                      "
                           + " WHERE   ( TT_DETAILED_LIST_LOAN.ACC_CD = {0} )  "
                           + " AND     ( TT_DETAILED_LIST_LOAN.OVD_PRN > 0  )  ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("ad_acc_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm3.Value = prp.acc_cd;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.adt_dt;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                          string.Concat("'", prp.acc_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_detailed_list_loan();

                                        loanDtl.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.curr_intt_rate = UtilityM.CheckNull<float>(reader["CURR_INTT_RATE"]);
                                        loanDtl.ovd_intt_rate = UtilityM.CheckNull<float>(reader["OVD_INTT_RATE"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanDtl.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.computed_till_dt = UtilityM.CheckNull<DateTime>(reader["COMPUTED_TILL_DT"]);
                                        loanDtl.list_dt = UtilityM.CheckNull<DateTime>(reader["LIST_DT"]);
                                        loanDtlList.Add(loanDtl);                                       
                                    }                                    
                                }

                                transaction.Commit();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }



        internal List<tt_npa> PopulateNPAList(p_report_param prp)
        {
            List<tt_npa> loanDtlList = new List<tt_npa>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_npa_dl";
            string _query = " SELECT TT_NPA_LOANWISE.ACC_CD,           "
                             + "  TT_NPA_LOANWISE.LOAN_ID,           "
                             + "  TT_NPA_LOANWISE.CASE_NO,      "
                             + "  TT_NPA_LOANWISE.PARTY_NAME,       "
                             + "  TT_NPA_LOANWISE.INT_DUE,              "
                             + "  TT_NPA_LOANWISE.STAN_PRN,               "
                             + "  TT_NPA_LOANWISE.SUBSTAN_PRN,            "
                             + "  TT_NPA_LOANWISE.D1_PRN,            "
                             + "  TT_NPA_LOANWISE.D2_PRN,             "
                             + "  TT_NPA_LOANWISE.D3_PRN,              "
                             + "  TT_NPA_LOANWISE.NPA_DT,           "
                             + "  TT_NPA_LOANWISE.DEFAULT_NO,           "
                             + "  TT_NPA_LOANWISE.BLOCK_NAME,    "
                             + "  TM_LOAN_ALL.DISB_DT,           "
                             + "  TM_LOAN_ALL.DISB_AMT,       "
                             + "  Round( TM_LOAN_ALL.DISB_AMT / TM_LOAN_ALL.INSTL_NO)       INSTL_AMT1,      "
                             + "  TM_LOAN_ALL.PIRIODICITY,           "
                             + "  TT_NPA_LOANWISE.OVD_PRN,           "
                             + "  TT_NPA_LOANWISE.OVD_INTT,           "
                             + "  TT_NPA_LOANWISE.PENAL_INTT,           "
                             + "  TT_NPA_LOANWISE.PRN_DUE,           "
                             + "  TT_NPA_LOANWISE.ACTIVITY,           "
                             + "  TT_NPA_DL_LIST.PROVISION,           "
                             + "  TT_NPA_DL_LIST.RECOV_DT           "
                              + " FROM TT_NPA_LOANWISE,TM_LOAN_ALL,TT_NPA_DL_LIST         "
                           + "  WHERE	TM_LOAN_ALL.LOAN_ID =  TT_NPA_LOANWISE.LOAN_ID        "
                           + "  AND     TM_LOAN_ALL.LOAN_ID = TT_NPA_DL_LIST.LOAN_ID "
                           + "  AND		 TM_LOAN_ALL.FUND_TYPE= {0}   "
                           + "  AND 	TM_LOAN_ALL.ARDB_CD = {1}   "
                           + "  AND 	TM_LOAN_ALL.BRN_CD = {2}   ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("ad_acc_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm3.Value = prp.acc_cd;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.adt_dt;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                          string.Concat("'", prp.fund_type, "'"),
                          string.Concat("'", prp.ardb_cd, "'"),
                          string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_npa();

                                        loanDtl.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.case_no = UtilityM.CheckNull<string>(reader["CASE_NO"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.intt_due = UtilityM.CheckNull<decimal>(reader["INT_DUE"]);
                                        loanDtl.stan_prn = UtilityM.CheckNull<decimal>(reader["STAN_PRN"]);
                                        loanDtl.substan_prn = UtilityM.CheckNull<decimal>(reader["SUBSTAN_PRN"]);
                                        loanDtl.d1_prn = UtilityM.CheckNull<decimal>(reader["D1_PRN"]);
                                        loanDtl.d2_prn = UtilityM.CheckNull<decimal>(reader["D2_PRN"]);
                                        loanDtl.d3_prn = UtilityM.CheckNull<decimal>(reader["D3_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.npa_dt = UtilityM.CheckNull<DateTime>(reader["NPA_DT"]);
                                        loanDtl.default_no = UtilityM.CheckNull<Int64>(reader["DEFAULT_NO"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.activity = UtilityM.CheckNull<string>(reader["ACTIVITY"]);
                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanDtl.instl_amt1 = UtilityM.CheckNull<decimal>(reader["INSTL_AMT1"]);
                                        loanDtl.periodicity = UtilityM.CheckNull<string>(reader["PIRIODICITY"]);
                                        loanDtl.prn_due = UtilityM.CheckNull<decimal>(reader["PRN_DUE"]);
                                        loanDtl.provision = UtilityM.CheckNull<decimal>(reader["PROVISION"]);
                                        loanDtl.recov_dt = UtilityM.CheckNull<DateTime>(reader["RECOV_DT"]);
                                        loanDtlList.Add(loanDtl);
                                    }
                                }

                                transaction.Commit();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }



        internal List<tt_npa> PopulateNPAListAll(p_report_param prp)
        {
            List<tt_npa> loanDtlList = new List<tt_npa>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_npa_dl_all";
            string _query = " SELECT (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD= TT_NPA_LOANWISE_ALL.ACC_CD) ACC_CD,           "
                             + "  TT_NPA_LOANWISE_ALL.LOAN_ID,           "
                             + "  TT_NPA_LOANWISE_ALL.CASE_NO,      "
                             + "  TT_NPA_LOANWISE_ALL.PARTY_NAME,       "
                             + "  TT_NPA_LOANWISE_ALL.INT_DUE,              "
                             + "  TT_NPA_LOANWISE_ALL.STAN_PRN,               "
                             + "  TT_NPA_LOANWISE_ALL.SUBSTAN_PRN,            "
                             + "  TT_NPA_LOANWISE_ALL.D1_PRN,            "
                             + "  TT_NPA_LOANWISE_ALL.D2_PRN,             "
                             + "  TT_NPA_LOANWISE_ALL.D3_PRN,              "
                             + "  TT_NPA_LOANWISE_ALL.NPA_DT,           "
                             + "  TT_NPA_LOANWISE_ALL.DEFAULT_NO,           "
                             + "  TT_NPA_LOANWISE_ALL.BLOCK_NAME,    "
                             + "  TM_LOAN_ALL.DISB_DT,           "
                             + "  TM_LOAN_ALL.DISB_AMT,       "
                             + "  Round( TM_LOAN_ALL.DISB_AMT / TM_LOAN_ALL.INSTL_NO)       INSTL_AMT1,      "
                             + "  TM_LOAN_ALL.PIRIODICITY,           "
                             + "  TT_NPA_LOANWISE_ALL.OVD_PRN,           "
                             + "  TT_NPA_LOANWISE_ALL.OVD_INTT,           "
                             + "  TT_NPA_LOANWISE_ALL.PENAL_INTT,           "
                             + "  TT_NPA_LOANWISE_ALL.PRN_DUE,           "
                             + "  TT_NPA_LOANWISE_ALL.ACTIVITY,           "
                             + "  TT_NPA_DL_LIST.PROVISION,           "
                             + "  TT_NPA_DL_LIST.RECOV_DT,           "
                             + "  TT_NPA_LOANWISE_ALL.ACTIVITY           "
                              + " FROM TT_NPA_LOANWISE_ALL,TM_LOAN_ALL,TT_NPA_DL_LIST         "
                           + "  WHERE	TM_LOAN_ALL.LOAN_ID =  TT_NPA_LOANWISE_ALL.LOAN_ID        "
                           + "  AND     TM_LOAN_ALL.LOAN_ID = TT_NPA_DL_LIST.LOAN_ID "
                           + "  AND		 TM_LOAN_ALL.FUND_TYPE= {0}   "
                           + "  AND 	TM_LOAN_ALL.ARDB_CD = {1}   "
                           + "  AND 	TM_LOAN_ALL.BRN_CD = {2}   ";

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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.adt_dt;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                          string.Concat("'", prp.fund_type, "'"),
                          string.Concat("'", prp.ardb_cd, "'"),
                          string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_npa();

                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_CD"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.case_no = UtilityM.CheckNull<string>(reader["CASE_NO"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.intt_due = UtilityM.CheckNull<decimal>(reader["INT_DUE"]);
                                        loanDtl.stan_prn = UtilityM.CheckNull<decimal>(reader["STAN_PRN"]);
                                        loanDtl.substan_prn = UtilityM.CheckNull<decimal>(reader["SUBSTAN_PRN"]);
                                        loanDtl.d1_prn = UtilityM.CheckNull<decimal>(reader["D1_PRN"]);
                                        loanDtl.d2_prn = UtilityM.CheckNull<decimal>(reader["D2_PRN"]);
                                        loanDtl.d3_prn = UtilityM.CheckNull<decimal>(reader["D3_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.npa_dt = UtilityM.CheckNull<DateTime>(reader["NPA_DT"]);
                                        loanDtl.default_no = UtilityM.CheckNull<Int64>(reader["DEFAULT_NO"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.activity = UtilityM.CheckNull<string>(reader["ACTIVITY"]);
                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanDtl.instl_amt1 = UtilityM.CheckNull<decimal>(reader["INSTL_AMT1"]);
                                        loanDtl.periodicity = UtilityM.CheckNull<string>(reader["PIRIODICITY"]);
                                        loanDtl.prn_due = UtilityM.CheckNull<decimal>(reader["PRN_DUE"]);
                                        loanDtl.provision = UtilityM.CheckNull<decimal>(reader["PROVISION"]);
                                        loanDtl.recov_dt = UtilityM.CheckNull<DateTime>(reader["RECOV_DT"]);
                                        loanDtl.activity = UtilityM.CheckNull<string>(reader["ACTIVITY"]);
                                        loanDtlList.Add(loanDtl);
                                    }
                                }

                                transaction.Commit();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDtlList = null;
                    }
                }
            }
            return loanDtlList;
        }



        internal List<demand_list> GetDemandListSingle(p_report_param prp)
        {
            List<demand_list> loanDfltList = new List<demand_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand_single";
            string _query = " SELECT DISTINCT TT_BLOCK_ACTI_DEMAND.ARDB_CD,TT_BLOCK_ACTI_DEMAND.LOAN_ID,             "
                           + " TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,           "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_PRN,            "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_PRN,                 "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_INTT,                  "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_INTT,                "
                           + " TT_BLOCK_ACTI_DEMAND.PENAL_INTT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.DISB_DT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN,                  "
                           + " TT_BLOCK_ACTI_DEMAND.UPTO_1,               "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_1,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_2,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_3,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_4,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_5,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_6,          "
                           + " TT_BLOCK_ACTI_DEMAND.PARTY_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.SERVICE_AREA_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.VILL_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.MONTH,          "
                           + " TT_BLOCK_ACTI_DEMAND.ACC_CD,          "
                            + " TT_BLOCK_ACTI_DEMAND.LOAN_ACC_NO,          "
                             + " TT_BLOCK_ACTI_DEMAND.ACC_DESC          "
                           + " FROM TT_BLOCK_ACTI_DEMAND               ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            var parm6 = new OracleParameter("ls_loan_id", OracleDbType.Char, ParameterDirection.Input);
                            parm6.Value = prp.loan_id;
                            command.Parameters.Add(parm6);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new demand_list();

                                        loanDtl.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.service_area_name = UtilityM.CheckNull<string>(reader["SERVICE_AREA_NAME"]);
                                        loanDtl.vill_name = UtilityM.CheckNull<string>(reader["VILL_NAME"]);
                                        loanDtl.month = UtilityM.CheckNull<Int64>(reader["MONTH"]);
                                        loanDtl.acc_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["ACC_CD"]));
                                        loanDtl.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_DESC"]);

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }




        internal List<demand_list> GetDemandList(p_report_param prp)
        {
            List<demand_list> loanDfltList = new List<demand_list>();           

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT DISTINCT TT_BLOCK_ACTI_DEMAND.ARDB_CD,TT_BLOCK_ACTI_DEMAND.LOAN_ID,             "
                           + " TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,           "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_PRN,            "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_PRN,                 "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_INTT,                  "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_INTT,                "
                           + " TT_BLOCK_ACTI_DEMAND.PENAL_INTT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.DISB_DT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN,                  "
                           + " TT_BLOCK_ACTI_DEMAND.UPTO_1,               "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_1,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_2,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_3,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_4,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_5,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_6,          "
                           + " TT_BLOCK_ACTI_DEMAND.PARTY_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.SERVICE_AREA_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.VILL_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.MONTH,          "
                           + " TT_BLOCK_ACTI_DEMAND.ACC_CD,          "
                            + " TT_BLOCK_ACTI_DEMAND.LOAN_ACC_NO,          "
                             + " TT_BLOCK_ACTI_DEMAND.ACC_DESC          "
                           + " FROM TT_BLOCK_ACTI_DEMAND               ";

            
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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new demand_list();

                                        loanDtl.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.service_area_name = UtilityM.CheckNull<string>(reader["SERVICE_AREA_NAME"]);
                                        loanDtl.vill_name = UtilityM.CheckNull<string>(reader["VILL_NAME"]);
                                        loanDtl.month = UtilityM.CheckNull<Int64>(reader["MONTH"]);
                                        loanDtl.acc_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["ACC_CD"]));
                                        loanDtl.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_DESC"]);

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }



        internal List<demandDM> GetDemandListMemberwise(p_report_param prp)
        {
            List<demandDM> loanDfltList = new List<demandDM>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT DISTINCT TT_BLOCK_ACTI_DEMAND.ARDB_CD,TT_BLOCK_ACTI_DEMAND.LOAN_ID,             "
                           + " TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,           "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_PRN,            "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_PRN,                 "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_INTT,                  "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_INTT,                "
                           + " TT_BLOCK_ACTI_DEMAND.PENAL_INTT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.DISB_DT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN,                  "
                           + " TT_BLOCK_ACTI_DEMAND.UPTO_1,               "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_1,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_2,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_3,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_4,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_5,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_6,          "
                           + " TT_BLOCK_ACTI_DEMAND.PARTY_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.SERVICE_AREA_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.VILL_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.MONTH,          "
                           + " TT_BLOCK_ACTI_DEMAND.ACC_CD,          "
                           + " TT_BLOCK_ACTI_DEMAND.LOAN_ACC_NO          "
                           + " FROM TT_BLOCK_ACTI_DEMAND  " 
                           + " WHERE TT_BLOCK_ACTI_DEMAND.ARDB_CD = {0}  "
                           + " AND TT_BLOCK_ACTI_DEMAND.BLOCK_NAME = {1}  "
                           + " AND TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD = {2}  " ;

            string _query1 = " SELECT DISTINCT block_name "
                             +  "  FROM TT_BLOCK_ACTI_DEMAND "
                             +  "  WHERE ARDB_CD = {0} " ;

            string _query2 = " SELECT DISTINCT activity_cd  "
                             + "  FROM TT_BLOCK_ACTI_DEMAND "
                             + "  WHERE ARDB_CD = {0} AND BLOCK_NAME = {1} ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                       string _statement1 = string.Format(_query1,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'")
                                        );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        demandDM tcaRet1 = new demandDM();

                                        var tca = new demandblock_type();
                                        tca.block = UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]);

                                        string _statement2 = string.Format(_query2,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                        string.Concat("'", UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]), "'")
                                        );

                                        tcaRet1.demandblock = tca;

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {
                                                        var tca1 = new activitywise_type();
                                                        tca1.activitytype.activity = UtilityM.CheckNull<string>(reader2["ACTIVITY_CD"]);

                                                        _statement = string.Format(_query,
                                                                                    string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                                                                    string.Concat("'", UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]), "'"),
                                                                                    string.Concat("'", UtilityM.CheckNull<string>(reader2["ACTIVITY_CD"]), "'")); 
                                                            

                                                        //tcaRet1.demandactivity = tca1;

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanDtl = new demand_list();

                                                                        loanDtl.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                                                        loanDtl.service_area_name = UtilityM.CheckNull<string>(reader["SERVICE_AREA_NAME"]);
                                                                        loanDtl.vill_name = UtilityM.CheckNull<string>(reader["VILL_NAME"]);
                                                                        loanDtl.month = UtilityM.CheckNull<Int64>(reader["MONTH"]);
                                                                        loanDtl.acc_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["ACC_CD"]));
                                                                        loanDtl.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                                                        tca1.demandlist.Add(loanDtl);
                                                                    }
                                                                    tcaRet1.demandactivity.Add(tca1); // transaction.Commit();
                                                                }
                                                            }
                                                        }
                                                        //loanDfltList.Add(tcaRet1);
                                                    }
                                                }
                                            }
                                        }
                                        loanDfltList.Add(tcaRet1);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<demand_list> GetDemandBlockwise(p_report_param prp)
        {
            List<demand_list> loanDfltList = new List<demand_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT  TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,"
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_PRN) CURR_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_PRN) OVD_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_INTT) CURR_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_INTT) OVD_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.PENAL_INTT) PENAL_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN) OUTSTANDING_PRN, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.UPTO_1) UPTO_1,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_1) ABOVE_1, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_2) ABOVE_2,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_3) ABOVE_3,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_4) ABOVE_4,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_5) ABOVE_5,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_6) ABOVE_6 "
                            + " FROM TT_BLOCK_ACTI_DEMAND "
                            + " GROUP BY TT_BLOCK_ACTI_DEMAND.BLOCK_NAME ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new demand_list();

                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);                     
                                        
                                       

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }



        internal List<blockwise_type> GetDemandBlockwisegroup(p_report_param prp)
        {
            List<blockwise_type> loanDfltList = new List<blockwise_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT  TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,"
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_PRN) CURR_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_PRN) OVD_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_INTT) CURR_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_INTT) OVD_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.PENAL_INTT) PENAL_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN) OUTSTANDING_PRN, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.UPTO_1) UPTO_1,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_1) ABOVE_1, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_2) ABOVE_2,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_3) ABOVE_3,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_4) ABOVE_4,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_5) ABOVE_5,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_6) ABOVE_6 "
                            + " FROM TT_BLOCK_ACTI_DEMAND "
                            + " WHERE TT_BLOCK_ACTI_DEMAND.ARDB_CD = {0} AND TT_BLOCK_ACTI_DEMAND.BLOCK_NAME = {1} GROUP BY TT_BLOCK_ACTI_DEMAND.BLOCK_NAME ";

            string _query1 = " SELECT DISTINCT block_name "
                             + "  FROM TT_BLOCK_ACTI_DEMAND "
                             + "  WHERE ARDB_CD = {0} ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }


                        string _statement1 = string.Format(_query1,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'")
                                        );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        blockwise_type tcaRet1 = new blockwise_type();

                                        var tca = new demandblock_type();
                                        tca.block = UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]);

                                        tcaRet1.blockwisetype = tca;

                                        _statement = string.Format(_query,
                                                                   string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                                                    string.Concat("'", UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]), "'"));

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var loanDtl = new demand_list();

                                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                                        tcaRet1.demandlist.Add(loanDtl);
                                                    }
                                                    //transaction.Commit();
                                                }
                                            }
                                        }
                                        loanDfltList.Add(tcaRet1);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<demand_list> GetDemandActivitywise(p_report_param prp)
        {
            List<demand_list> loanDfltList = new List<demand_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT  TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,"
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_PRN) CURR_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_PRN) OVD_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_INTT) CURR_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_INTT) OVD_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.PENAL_INTT) PENAL_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN) OUTSTANDING_PRN, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.UPTO_1) UPTO_1,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_1) ABOVE_1, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_2) ABOVE_2,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_3) ABOVE_3,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_4) ABOVE_4,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_5) ABOVE_5,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_6)  ABOVE_6 "
                            + " FROM TT_BLOCK_ACTI_DEMAND "
                            + " GROUP BY TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new demand_list();

                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.upto_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.above_2 = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                        loanDtl.above_3 = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                        loanDtl.above_4 = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                        loanDtl.above_5 = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);



                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }



        internal List<recovery_list> GetRecoveryList(p_report_param prp)
        {
            List<recovery_list> loanDfltList = new List<recovery_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand_recov";
            string _query = " SELECT DISTINCT TT_BLOCK_ACTI_DEMAND.ARDB_CD,TT_BLOCK_ACTI_DEMAND.LOAN_ID,             "
                           + " TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,           "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_PRN,            "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_PRN,                 "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_INTT,                  "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_INTT,                "
                           + " TT_BLOCK_ACTI_DEMAND.PENAL_INTT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.DISB_DT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN,                  "
                           + " TT_BLOCK_ACTI_DEMAND.UPTO_1,               "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_1,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_2,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_3,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_4,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_5,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_6,          "
                           + " TT_BLOCK_ACTI_DEMAND.PARTY_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.SERVICE_AREA_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.VILL_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.MONTH,          "
                           + " TT_BLOCK_ACTI_DEMAND.ACC_CD,          "
                            + " TT_BLOCK_ACTI_DEMAND.LOAN_ACC_NO,          "
                             + " TT_BLOCK_ACTI_DEMAND.ACC_DESC          "
                           + " FROM TT_BLOCK_ACTI_DEMAND               ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new recovery_list();

                                        loanDtl.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                        loanDtl.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                        loanDtl.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                        loanDtl.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                        loanDtl.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.service_area_name = UtilityM.CheckNull<string>(reader["SERVICE_AREA_NAME"]);
                                        loanDtl.vill_name = UtilityM.CheckNull<string>(reader["VILL_NAME"]);
                                        loanDtl.month = UtilityM.CheckNull<Int64>(reader["MONTH"]);
                                        loanDtl.acc_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["ACC_CD"]));
                                        loanDtl.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);
                                        loanDtl.acc_name = UtilityM.CheckNull<string>(reader["ACC_DESC"]);

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<UserwisetransDM> GetUserwiseTransDtls(p_report_param prp)
        {
            List<UserwisetransDM> loanDfltList = new List<UserwisetransDM>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            
            string _query = " SELECT V_TRANS_DTLS.TRANS_DT,             "
                           + " (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = V_TRANS_DTLS.ACC_TYPE_CD) ACC_DESC,           "
                           + "  V_TRANS_DTLS.ACC_NUM LOAN_ID,              "
                           + " V_TRANS_DTLS.TRANS_CD,                "
                           + " Decode(V_TRANS_DTLS.TRANS_TYPE,'B','Disbursment','Recovery') TRANS_TYPE,                  "
                           + " V_TRANS_DTLS.TRANS_MODE,                 "
                           + " V_TRANS_DTLS.AMOUNT,                     "
                           + "  Decode(V_TRANS_DTLS.TRF_TYPE,'C','Cash','Transfer') TRF_TYPE,                "
                           + " (V_TRANS_DTLS.CURR_PRN_RECOV + V_TRANS_DTLS.ADV_PRN_RECOV + V_TRANS_DTLS.OVD_PRN_RECOV) PRN_RECOV,                 "
                           + " (V_TRANS_DTLS.CURR_INTT_RECOV+V_TRANS_DTLS.OVD_INTT_RECOV+ V_TRANS_DTLS.PENAL_INTT_RECOV) INTT_RECOV,               "
                           + " V_TRANS_DTLS.CURR_INTT_RATE,             "
                           + " V_TRANS_DTLS.OVD_INTT_RATE,          "
                           + " (SELECT USER_FIRST_NAME || ' ' || nvl(USER_MIDDLE_NAME,'') || ' ' || USER_LAST_NAME FROM M_USER_MASTER WHERE USER_ID =SUBSTR(V_TRANS_DTLS.CREATED_BY,1, instr(V_TRANS_DTLS.CREATED_BY,'/',1)-1)) USER_NAME          "
                           + "  FROM V_TRANS_DTLS       "
                           + " WHERE(V_TRANS_DTLS.ARDB_CD = {0}) and  "
                           + " (V_TRANS_DTLS.TRANS_TYPE IN ('R','B')) and  "
                           + " (V_TRANS_DTLS.TRANS_DT  = substr({1},1,10)) and  "
                           + " (V_TRANS_DTLS.BRN_CD  = {2})  AND "
                           + " (V_TRANS_DTLS.CREATED_BY = {3}) AND "
                           + " (V_TRANS_DTLS.APPROVAL_STATUS = 'A' ) "
                           + " ORDER BY TRANS_TYPE,TRANS_CD ";


            string _query1 = " SELECT  DISTINCT V_TRANS_DTLS.CREATED_BY USER_ID, "
                             + " (SELECT USER_FIRST_NAME || ' '|| nvl(USER_MIDDLE_NAME,'') || ' ' || USER_LAST_NAME  FROM M_USER_MASTER WHERE USER_ID =SUBSTR(V_TRANS_DTLS.CREATED_BY,1,instr(V_TRANS_DTLS.CREATED_BY,'/',1)-1)) USER_NAME"
                             + "  FROM V_TRANS_DTLS "
                             + "  WHERE(V_TRANS_DTLS.ARDB_CD = {0}) and "
                             + " (V_TRANS_DTLS.TRANS_TYPE IN ('R','B')) and "
                             + " (V_TRANS_DTLS.TRANS_DT  = substr({1},1,10)) and"
                             + " (V_TRANS_DTLS.BRN_CD  = {2}) and "
                             + " (V_TRANS_DTLS.APPROVAL_STATUS = 'A') ";




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

                        string _statement1 = string.Format(_query1,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.to_dt , "'"),
                                         string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'")
                                         );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        UserwisetransDM tcaRet1 = new UserwisetransDM();

                                        var tca = new UserType();
                                        tca.user_id = UtilityM.CheckNull<string>(reader1["USER_ID"]);
                                        tca.user_name = UtilityM.CheckNull<string>(reader1["USER_NAME"]);
                                        
                                        tcaRet1.utype = tca;

                                        _statement = string.Format(_query,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.to_dt, "'"),
                                         string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                          string.Concat("'", UtilityM.CheckNull<string>(reader1["USER_ID"]), "'")
                                         );

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var loanDtl = new UserTransDtls();

                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                        loanDtl.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_DESC"]);
                                                        loanDtl.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                                        loanDtl.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                                        loanDtl.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                                                        loanDtl.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                                        loanDtl.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                                        loanDtl.prn_recov = UtilityM.CheckNull<decimal>(reader["PRN_RECOV"]);
                                                        loanDtl.intt_recov = UtilityM.CheckNull<decimal>(reader["INTT_RECOV"]);
                                                        loanDtl.curr_intt_rt = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                                                        loanDtl.ovd_intt_rt = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                                                        loanDtl.user_name = UtilityM.CheckNull<string>(reader["USER_NAME"]);

                                                        //loanDfltList.Add(loanDtl);
                                                        tcaRet1.utransdtls.Add(loanDtl);
                                                    }                                                    
                                                }
                                            }
                                        }
                                                    
                                        loanDfltList.Add(tcaRet1);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }

/*        SELECT V_USER_TRANS.BRN_CD,
         V_USER_TRANS.TRANS_DT,
         V_USER_TRANS.VTYPE,
         V_USER_TRANS.TRANS_CD,
         V_USER_TRANS.ACC_TYPE_CD,
         V_USER_TRANS.ACC_NAME,
         V_USER_TRANS.ACC_NUM,
         V_USER_TRANS.TRANS_TYPE,
         DECODE(V_USER_TRANS.TRANS_TYPE,'D', V_USER_TRANS.AMOUNT,'R', V_USER_TRANS.AMOUNT,0),   
         DECODE(V_USER_TRANS.TRANS_TYPE,'W', V_USER_TRANS.AMOUNT,'B', V_USER_TRANS.AMOUNT,0),   
         V_USER_TRANS.CREATED_BY,   
         V_USER_TRANS.CUST_NAME,   
         V_USER_TRANS.APPROVED_BY,   
         V_USER_TRANS.TRF_TYPE
    FROM V_USER_TRANS
   WHERE V_USER_TRANS.TRANS_DT = '16/05/2023' 



        internal List<UserwisetransDM> GetUserwiseTransDtlsAll(p_report_param prp)
        {
            List<UserwisetransDM> loanDfltList = new List<UserwisetransDM>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;

            string _query = " SELECT V_USER_TRANS.ARDB_CD,             "
                           + " V_USER_TRANS.TRANS_DT,           "
                           + " V_TRANS_DTLS.TRANS_CD,                "
                           + " V_USER_TRANS.ACC_TYPE_CD,                  "
                           + "  V_USER_TRANS.ACC_NAME,                 "
                           + "  V_USER_TRANS.ACC_NUM,                     "
                           + "   V_USER_TRANS.TRANS_TYPE,                "
                           + " DECODE(V_USER_TRANS.TRANS_TYPE,'D', V_USER_TRANS.AMOUNT,'R', V_USER_TRANS.AMOUNT,0) RECEIPT,                    "
                           + " DECODE(V_USER_TRANS.TRANS_TYPE,'W', V_USER_TRANS.AMOUNT,'B', V_USER_TRANS.AMOUNT,0) PAYMENT,               "
                           + "  V_USER_TRANS.CREATED_BY,            "
                           + "  V_USER_TRANS.APPROVED_BY,           "
                           + "  V_USER_TRANS.TRF_TYPE          "
                           + "  FROM V_USER_TRANS       "
                           + " WHERE(V_USER_TRANS.ARDB_CD = {0}) and  "
                           + " (V_TRANS_DTLS.TRANS_TYPE IN ('R','B')) and  "
                           + " (V_USER_TRANS.TRANS_DT  = substr({1},1,10)) and  "
                           + " (V_USER_TRANS.BRN_CD  = {2})  AND "
                           + " (SUBSTR(V_USER_TRANS.CREATED_BY,1, instr(V_USER_TRANS.CREATED_BY,'/',1)-1) ) ={3} "
                           + " ORDER BY TRANS_TYPE,TRANS_CD ";


            string _query1 = " SELECT  DISTINCT SUBSTR(V_USER_TRANS.CREATED_BY,1,instr(V_USER_TRANS.CREATED_BY,'/',1)-1) USER_ID, "
                             + " (SELECT USER_FIRST_NAME || ' '|| nvl(USER_MIDDLE_NAME,'') || ' ' || USER_LAST_NAME  FROM M_USER_MASTER WHERE USER_ID =SUBSTR(V_USER_TRANS.CREATED_BY,1,instr(V_USER_TRANS.CREATED_BY,'/',1)-1)) USER_NAME"
                             + "  FROM V_USER_TRANS "
                             + "  WHERE(V_USER_TRANS.ARDB_CD = {0}) and "
                             + " (V_USER_TRANS.TRANS_DT  = substr({1},1,10)) and"
                             + " (V_USER_TRANS.BRN_CD  = {2})";




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

                        string _statement1 = string.Format(_query1,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.to_dt, "'"),
                                         string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'")
                                         );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        UserwisetransDM tcaRet1 = new UserwisetransDM();

                                        var tca = new UserType();
                                        tca.user_id = UtilityM.CheckNull<string>(reader1["USER_ID"]);
                                        tca.user_name = UtilityM.CheckNull<string>(reader1["USER_NAME"]);

                                        tcaRet1.utype = tca;

                                        _statement = string.Format(_query,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.to_dt, "'"),
                                         string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                          string.Concat("'", UtilityM.CheckNull<string>(reader1["USER_ID"]), "'")
                                         );

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var loanDtl = new UserTransDtls();

                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        loanDtl.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                        loanDtl.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                                        loanDtl.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                                        loanDtl.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                                        loanDtl.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                                        loanDtl.prn_recov = UtilityM.CheckNull<decimal>(reader["RECEIPT"]);
                                                        loanDtl.intt_recov = UtilityM.CheckNull<decimal>(reader["PAYMENT"]);                                                        
                                                        loanDtl.user_name = UtilityM.CheckNull<string>(reader["CREATED_BY"]);

                                                        //loanDfltList.Add(loanDtl);
                                                        tcaRet1.utransdtls.Add(loanDtl);
                                                    }
                                                }
                                            }
                                        }

                                        loanDfltList.Add(tcaRet1);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }*/



        internal List<recoveryDM> GetRecoveryListGroupwise(p_report_param prp)
        {
            List<recoveryDM> loanDfltList = new List<recoveryDM>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand_recov";
            string _query = " SELECT DISTINCT TT_BLOCK_ACTI_DEMAND.ARDB_CD,TT_BLOCK_ACTI_DEMAND.LOAN_ID,             "
                           + " TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,           "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_PRN,            "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_PRN,                 "
                           + " TT_BLOCK_ACTI_DEMAND.CURR_INTT,                  "
                           + " TT_BLOCK_ACTI_DEMAND.OVD_INTT,                "
                           + " TT_BLOCK_ACTI_DEMAND.PENAL_INTT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.DISB_DT,                 "
                           + " TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN,                  "
                           + " TT_BLOCK_ACTI_DEMAND.UPTO_1,               "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_1,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_2,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_3,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_4,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_5,          "
                           + " TT_BLOCK_ACTI_DEMAND.ABOVE_6,          "
                           + " TT_BLOCK_ACTI_DEMAND.PARTY_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.SERVICE_AREA_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.VILL_NAME,          "
                           + " TT_BLOCK_ACTI_DEMAND.MONTH,          "
                           + " TT_BLOCK_ACTI_DEMAND.ACC_CD,          "
                            + " TT_BLOCK_ACTI_DEMAND.LOAN_ACC_NO          "
                           + " FROM TT_BLOCK_ACTI_DEMAND               "
                           + " WHERE TT_BLOCK_ACTI_DEMAND.ARDB_CD = {0}  "
                           + " AND TT_BLOCK_ACTI_DEMAND.BLOCK_NAME = {1}  "
                           + " AND TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD = {2}  ";


            string _query1 = " SELECT DISTINCT block_name "
                             + "  FROM TT_BLOCK_ACTI_DEMAND "
                             + "  WHERE ARDB_CD = {0} ";

            string _query2 = " SELECT DISTINCT activity_cd  "
                             + "  FROM TT_BLOCK_ACTI_DEMAND "
                             + "  WHERE ARDB_CD = {0} AND BLOCK_NAME = {1} ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        string _statement1 = string.Format(_query1,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'")
                                         );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        recoveryDM tcaRet1 = new recoveryDM();

                                        var tca = new demandblock_type();
                                        tca.block = UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]);

                                        string _statement2 = string.Format(_query2,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                        string.Concat("'", UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]), "'")
                                        );

                                        tcaRet1.recoveryblock = tca;

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {
                                                        var tca1 = new activitywiserecovery_type();
                                                        tca1.activitytype.activity = UtilityM.CheckNull<string>(reader2["ACTIVITY_CD"]);

                                                        _statement = string.Format(_query,
                                                                                    string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                                                                    string.Concat("'", UtilityM.CheckNull<string>(reader1["BLOCK_NAME"]), "'"),
                                                                                    string.Concat("'", UtilityM.CheckNull<string>(reader2["ACTIVITY_CD"]), "'"));


                                                        //tcaRet1.demandactivity = tca1;

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanDtl = new recovery_list();

                                                                        loanDtl.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                                                        loanDtl.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["UPTO_1"]);
                                                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                                                        loanDtl.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_2"]);
                                                                        loanDtl.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_3"]);
                                                                        loanDtl.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_4"]);
                                                                        loanDtl.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["ABOVE_5"]);
                                                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);
                                                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                                                        loanDtl.service_area_name = UtilityM.CheckNull<string>(reader["SERVICE_AREA_NAME"]);
                                                                        loanDtl.vill_name = UtilityM.CheckNull<string>(reader["VILL_NAME"]);
                                                                        loanDtl.month = UtilityM.CheckNull<Int64>(reader["MONTH"]);
                                                                        loanDtl.acc_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["ACC_CD"]));
                                                                        loanDtl.loan_acc_no = UtilityM.CheckNull<string>(reader["LOAN_ACC_NO"]);

                                                                        //loanDfltList.Add(loanDtl);
                                                                        tca1.recoverylist.Add(loanDtl);
                                                                    }
                                                                    tcaRet1.recoveryactivity.Add(tca1);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        loanDfltList.Add(tcaRet1);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<recovery_list> GetDemandCollectionBlockwise(p_report_param prp)
        {
            List<recovery_list> loanDfltList = new List<recovery_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand_recov";
            string _query = " SELECT  TT_BLOCK_ACTI_DEMAND.BLOCK_NAME,"
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_PRN) CURR_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_PRN) OVD_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_INTT) CURR_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_INTT) OVD_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.PENAL_INTT) PENAL_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN) OUTSTANDING_PRN, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.UPTO_1) CURR_PRN_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_1) ABOVE_1, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_2) OVD_PRN_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_3) CURR_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_4) OVD_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_5) PENAL_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_6) ABOVE_6 "
                            + " FROM TT_BLOCK_ACTI_DEMAND "
                            + " GROUP BY TT_BLOCK_ACTI_DEMAND.BLOCK_NAME ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new recovery_list();

                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        loanDtl.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanDtl.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanDtl.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<recovery_list> GetDemandCollectionActivitywise(p_report_param prp)
        {
            List<recovery_list> loanDfltList = new List<recovery_list>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_block_new_acti_demand";
            string _query = " SELECT  TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD,"
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_PRN) CURR_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_PRN) OVD_PRN,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.CURR_INTT) CURR_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OVD_INTT) OVD_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.PENAL_INTT) PENAL_INTT, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.OUTSTANDING_PRN) OUTSTANDING_PRN, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.UPTO_1) CURR_PRN_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_1) ABOVE_1, "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_2) OVD_PRN_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_3) CURR_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_4) OVD_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_5) PENAL_INTT_RECOV,  "
                            + "sum(TT_BLOCK_ACTI_DEMAND.ABOVE_6)  ABOVE_6 "
                            + " FROM TT_BLOCK_ACTI_DEMAND "
                            + " GROUP BY TT_BLOCK_ACTI_DEMAND.ACTIVITY_CD ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new recovery_list();

                                        loanDtl.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                        loanDtl.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                        loanDtl.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                        loanDtl.outstanding_prn = UtilityM.CheckNull<decimal>(reader["OUTSTANDING_PRN"]);
                                        loanDtl.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        loanDtl.above_1 = UtilityM.CheckNull<decimal>(reader["ABOVE_1"]);
                                        loanDtl.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        loanDtl.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanDtl.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanDtl.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        loanDtl.above_6 = UtilityM.CheckNull<decimal>(reader["ABOVE_6"]);

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<gm_loan_trans> PopulateAdvRecovStmt(p_report_param prp)
        {
            List<gm_loan_trans> loanDfltList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_adv_recov_stmt";
            string _query = " SELECT ACC_CD, ACC_DESC, ISSUE,"
                            + " CURR_PRN,  "
                            + " OVD_PRN,  "
                            + " CURR_BAL, "
                            + " OVD_BAL, "
                            + " ADV_PRN "
                            + " FROM tt_adv_recov_rep ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new gm_loan_trans();

                                        loanDtl.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);
                                        loanDtl.acc_typ_dsc = UtilityM.CheckNull<string>(reader["ACC_DESC"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["ISSUE"]);
                                        loanDtl.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                        loanDtl.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                        loanDtl.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                        loanDtl.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_BAL"]);
                                        loanDtl.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN"]);
                                        

                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<gm_loan_trans> PopulateInttRecovStmt(p_report_param prp)
        {
            List<gm_loan_trans> loanDfltList = new List<gm_loan_trans>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_loan_intt_reg";
            string _query = "SELECT M_ACC_MASTER.ACC_NAME ACC_DESC,   "
                           + " Sum(TT_LOAN_INTT_RECOV.CURR_INTT_RECOV) CURR_INTT_RECOV, "  
                           + " sum(TT_LOAN_INTT_RECOV.OVD_INTT_RECOV) OVD_INTT_RECOV,  " 
                           + " sum(TT_LOAN_INTT_RECOV.PENAL_INTT_RECOV) PENAL_INTT_RECOV, "  
                           + " sum(TT_LOAN_INTT_RECOV.CURR_INTT_COMP) CURR_INTT_COMP,   "
                           + " sum(TT_LOAN_INTT_RECOV.OVD_INTT_COMP) OVD_INTT_COMP, "
                           + " sum(TT_LOAN_INTT_RECOV.PENAL_INTT_COMP) PENAL_INTT_COMP "
                           + " FROM TT_LOAN_INTT_RECOV,M_ACC_MASTER  "
                           + " WHERE TT_LOAN_INTT_RECOV.LOAN_ACC = M_ACC_MASTER.ACC_CD "
                           + " AND M_ACC_MASTER.ARDB_CD = {0} "
                           + " Group by M_ACC_MASTER.ACC_NAME ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("ac_fund_type", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.fund_type;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query, String.Concat("'", prp.ardb_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new gm_loan_trans();

                                        loanDtl.acc_typ_dsc = UtilityM.CheckNull<string>(reader["ACC_DESC"]);
                                        loanDtl.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        loanDtl.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        loanDtl.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        loanDtl.curr_intt_calculated = UtilityM.CheckNull<decimal>(reader["CURR_INTT_COMP"]);
                                        loanDtl.ovd_intt_calculated = UtilityM.CheckNull<decimal>(reader["OVD_INTT_COMP"]);
                                        loanDtl.penal_intt_calculated = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_COMP"]);
                                        


                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


        internal List<tt_loan_opn_cls> PopulateLoanOpenRegister(p_report_param prp)
        {
            List<tt_loan_opn_cls> loanDfltList = new List<tt_loan_opn_cls>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_open_close_loan_reg";
            string _query = " SELECT TT_LOAN_OPN_CLS.TRANS_DT  TRANS_DT, "
                            + " MM_CUSTOMER.CUST_NAME  CUST_NAME,   "
                            + " TT_LOAN_OPN_CLS.LOAN_ID LOAN_ID,  "
                            + " M_ACC_MASTER.ACC_NAME ACC_NAME,  "
                            + " TT_LOAN_OPN_CLS.SANC_DT SANC_DT, "
                            + " TT_LOAN_OPN_CLS.SANC_AMT SANC_AMT, "
                            + " TT_LOAN_OPN_CLS.DISB_AMT DISB_AMT, "
                            + " TT_LOAN_OPN_CLS.INSTL_NO INSTL_NO, "
                            + " TT_LOAN_OPN_CLS.CURR_RT CURR_RT, "
                            + " TT_LOAN_OPN_CLS.OVD_RT OVD_RT, "
                            + " TT_LOAN_OPN_CLS.STATUS  STATUS"
                            + " FROM TT_LOAN_OPN_CLS,MM_CUSTOMER,M_ACC_MASTER "
                            + "  WHERE TT_LOAN_OPN_CLS.STATUS = 'O' "
                            + " AND  TT_LOAN_OPN_CLS.PARTY_CD = MM_CUSTOMER.CUST_CD "
                            + "  AND  TT_LOAN_OPN_CLS.ACC_CD = M_ACC_MASTER.ACC_CD "
                            + "  AND MM_CUSTOMER.ARDB_CD = {0} "
                            + "  AND M_ACC_MASTER.ARDB_CD = {1} ORDER BY TRANS_DT ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("as_flag", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.flag;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query, String.Concat("'", prp.ardb_cd, "'"), String.Concat("'", prp.ardb_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_loan_opn_cls();

                                        loanDtl.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanDtl.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]);
                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanDtl.sanc_dt = UtilityM.CheckNull<DateTime>(reader["SANC_DT"]);
                                        loanDtl.sanc_amt = UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                        loanDtl.instl_no = UtilityM.CheckNull<decimal>(reader["INSTL_NO"]);
                                        loanDtl.curr_rt = UtilityM.CheckNull<double>(reader["CURR_RT"]);
                                        loanDtl.ovd_rt = UtilityM.CheckNull<double>(reader["OVD_RT"]);
                                        loanDtl.status = UtilityM.CheckNull<string>(reader["STATUS"]);



                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }

        internal List<tt_loan_opn_cls> PopulateLoanCloseRegister(p_report_param prp)
        {
            List<tt_loan_opn_cls> loanDfltList = new List<tt_loan_opn_cls>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "p_open_close_loan_reg";
            string _query = " SELECT TT_LOAN_OPN_CLS.TRANS_DT  TRANS_DT, "
                            + " MM_CUSTOMER.CUST_NAME  CUST_NAME,   "
                            + " TT_LOAN_OPN_CLS.LOAN_ID LOAN_ID,  "
                            + " M_ACC_MASTER.ACC_NAME ACC_NAME,  "
                            + " TT_LOAN_OPN_CLS.SANC_DT SANC_DT, "
                            + " TT_LOAN_OPN_CLS.SANC_AMT SANC_AMT, "
                            + " TT_LOAN_OPN_CLS.CURR_RT CURR_RT, "
                            + " TT_LOAN_OPN_CLS.OVD_RT OVD_RT, "
                            + " TT_LOAN_OPN_CLS.STATUS  STATUS,"
                            + " TT_LOAN_OPN_CLS.CLOSING_AMT CLOSING_AMT, "
                            + " TT_LOAN_OPN_CLS.CLOSING_INTT CLOSING_INTT"
                            + " FROM TT_LOAN_OPN_CLS,MM_CUSTOMER,M_ACC_MASTER "
                            + " WHERE TT_LOAN_OPN_CLS.STATUS = 'C' "
                            + "  AND  TT_LOAN_OPN_CLS.PARTY_CD = MM_CUSTOMER.CUST_CD "
                            + "  AND  TT_LOAN_OPN_CLS.ACC_CD = M_ACC_MASTER.ACC_CD "
                            + "  AND MM_CUSTOMER.ARDB_CD = {0} "
                            + "  AND M_ACC_MASTER.ARDB_CD = {1} ORDER BY TRANS_DT ";


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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("as_flag", OracleDbType.Char, ParameterDirection.Input);
                            parm5.Value = prp.flag;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

                        }

                        _statement = string.Format(_query, String.Concat("'", prp.ardb_cd, "'"), String.Concat("'", prp.ardb_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var loanDtl = new tt_loan_opn_cls();

                                        loanDtl.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        loanDtl.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        loanDtl.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]);
                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                        loanDtl.sanc_dt = UtilityM.CheckNull<DateTime>(reader["SANC_DT"]);
                                        loanDtl.sanc_amt = UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                        loanDtl.curr_rt = UtilityM.CheckNull<double>(reader["CURR_RT"]);
                                        loanDtl.ovd_rt = UtilityM.CheckNull<double>(reader["OVD_RT"]);
                                        loanDtl.status = UtilityM.CheckNull<string>(reader["STATUS"]);
                                        loanDtl.closing_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_AMT"]);
                                        loanDtl.closing_intt = UtilityM.CheckNull<decimal>(reader["CLOSING_INTT"]);



                                        loanDfltList.Add(loanDtl);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


       internal List<LoanPassbook_Print> LoanPassBookPrint(p_report_param prp)
        {
            List<LoanPassbook_Print> passBookPrint = new List<LoanPassbook_Print>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";            

            string _query = " SELECT MD_LOAN_PASSBOOK.TRANS_DT,         "
                  + " MD_LOAN_PASSBOOK.TRANS_CD,                        "
                  + " MD_LOAN_PASSBOOK.LOAN_ID,                         "
                  + " MD_LOAN_PASSBOOK.ISSUE_AMT,                       "
                  + " MD_LOAN_PASSBOOK.CURR_PRN_RECOV,                  "
                  + " MD_LOAN_PASSBOOK.OVD_PRN_RECOV,                   "
                  + " MD_LOAN_PASSBOOK.ADV_PRN_RECOV,                   "
                  + " MD_LOAN_PASSBOOK.CURR_INTT_RECOV,                 "
                  + " MD_LOAN_PASSBOOK.OVD_INTT_RECOV,                  "
                  + " MD_LOAN_PASSBOOK.PENAL_INTT_RECOV,                "
                  + " MD_LOAN_PASSBOOK.CURR_PRN_BAL,                    "
                  + " MD_LOAN_PASSBOOK.OVD_PRN_BAL,                     "
                  + " MD_LOAN_PASSBOOK.CURR_INTT_BAL,                   "
                  + " MD_LOAN_PASSBOOK.OVD_INTT_BAL,                    "
                  + " MD_LOAN_PASSBOOK.PENAL_INTT_BAL,                  "
                  + " MD_LOAN_PASSBOOK.PARTICULARS,                     "
                  + " MD_LOAN_PASSBOOK.PRINTED_FLAG                    "
                  + " FROM MD_LOAN_PASSBOOK                                              "
                  + " WHERE (MD_LOAN_PASSBOOK.LOAN_ID = '{0}' )                         "
                  + " AND   (MD_LOAN_PASSBOOK.ARDB_CD = '{1}') AND (MD_LOAN_PASSBOOK.PRINTED_FLAG = 'N')                        "
                  + " AND   (MD_LOAN_PASSBOOK.TRANS_DT BETWEEN to_date('{2}', 'dd-mm-yyyy hh24:mi:ss') AND to_date('{3}', 'dd-mm-yyyy hh24:mi:ss') ) "
                  + " ORDER BY MD_LOAN_PASSBOOK.TRANS_DT, MD_LOAN_PASSBOOK.TRANS_CD ";

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
                                                   prp.loan_id,
                                                   prp.ardb_cd,
                                                   prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy HH:mm:ss") : "from_dt",
                                                   prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy HH:mm:ss") : "to_dt"
                                                   );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var pb = new LoanPassbook_Print();

                                        pb.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        pb.trans_cd = UtilityM.CheckNull<int>(reader["TRANS_CD"]);
                                        pb.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        pb.issue_amt = UtilityM.CheckNull<decimal>(reader["ISSUE_AMT"]);
                                        pb.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        pb.printed_flag = UtilityM.CheckNull<string>(reader["PRINTED_FLAG"]);
                                        pb.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        pb.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        pb.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                        pb.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        pb.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        pb.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        pb.curr_prn_bal = UtilityM.CheckNull<decimal>(reader["CURR_PRN_BAL"]);
                                        pb.ovd_prn_bal = UtilityM.CheckNull<decimal>(reader["OVD_PRN_BAL"]);
                                        pb.curr_intt_bal = UtilityM.CheckNull<decimal>(reader["CURR_INTT_BAL"]);
                                        pb.ovd_intt_bal = UtilityM.CheckNull<decimal>(reader["OVD_INTT_BAL"]);
                                        pb.penal_intt_bal = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_BAL"]);


                                        passBookPrint.Add(pb);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        passBookPrint = null;
                    }
                }
            }

            return passBookPrint;
        }



        internal List<LoanPassbook_Print> LoanGetUpdatePassbookData(p_report_param prp)
        {
            List<LoanPassbook_Print> passBookPrint = new List<LoanPassbook_Print>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            string _query = " SELECT MD_LOAN_PASSBOOK.TRANS_DT,         "
                  + " MD_LOAN_PASSBOOK.TRANS_CD,                        "
                  + " MD_LOAN_PASSBOOK.LOAN_ID,                         "
                  + " MD_LOAN_PASSBOOK.ISSUE_AMT,                       "
                  + " MD_LOAN_PASSBOOK.CURR_PRN_RECOV,                  "
                  + " MD_LOAN_PASSBOOK.OVD_PRN_RECOV,                   "
                  + " MD_LOAN_PASSBOOK.ADV_PRN_RECOV,                   "
                  + " MD_LOAN_PASSBOOK.CURR_INTT_RECOV,                 "
                  + " MD_LOAN_PASSBOOK.OVD_INTT_RECOV,                  "
                  + " MD_LOAN_PASSBOOK.PENAL_INTT_RECOV,                "
                  + " MD_LOAN_PASSBOOK.CURR_PRN_BAL,                    "
                  + " MD_LOAN_PASSBOOK.OVD_PRN_BAL,                     "
                  + " MD_LOAN_PASSBOOK.CURR_INTT_BAL,                   "
                  + " MD_LOAN_PASSBOOK.OVD_INTT_BAL,                    "
                  + " MD_LOAN_PASSBOOK.PENAL_INTT_BAL,                  "
                  + " MD_LOAN_PASSBOOK.PARTICULARS,                     "
                  + " MD_LOAN_PASSBOOK.PRINTED_FLAG                    "
                  + " FROM MD_LOAN_PASSBOOK                                              "
                  + " WHERE (MD_LOAN_PASSBOOK.LOAN_ID = '{0}' )                         "
                  + " AND   (MD_LOAN_PASSBOOK.ARDB_CD = '{1}')                         "
                  + " AND   (MD_LOAN_PASSBOOK.TRANS_DT BETWEEN to_date('{2}', 'dd-mm-yyyy hh24:mi:ss') AND to_date('{3}', 'dd-mm-yyyy hh24:mi:ss') ) "
                  + " ORDER BY MD_LOAN_PASSBOOK.TRANS_DT, MD_LOAN_PASSBOOK.TRANS_CD ";

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
                                                   prp.loan_id,
                                                   prp.ardb_cd,
                                                   prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy HH:mm:ss") : "from_dt",
                                                   prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy HH:mm:ss") : "to_dt"
                                                   );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var pb = new LoanPassbook_Print();

                                        pb.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        pb.trans_cd = UtilityM.CheckNull<int>(reader["TRANS_CD"]);
                                        pb.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        pb.issue_amt = UtilityM.CheckNull<decimal>(reader["ISSUE_AMT"]);
                                        pb.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        pb.printed_flag = UtilityM.CheckNull<string>(reader["PRINTED_FLAG"]);
                                        pb.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                        pb.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                        pb.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                        pb.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                        pb.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                        pb.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                        pb.curr_prn_bal = UtilityM.CheckNull<decimal>(reader["CURR_PRN_BAL"]);
                                        pb.ovd_prn_bal = UtilityM.CheckNull<decimal>(reader["OVD_PRN_BAL"]);
                                        pb.curr_intt_bal = UtilityM.CheckNull<decimal>(reader["CURR_INTT_BAL"]);
                                        pb.ovd_intt_bal = UtilityM.CheckNull<decimal>(reader["OVD_INTT_BAL"]);
                                        pb.penal_intt_bal = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_BAL"]);


                                        passBookPrint.Add(pb);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        passBookPrint = null;
                    }
                }
            }

            return passBookPrint;
        }


        internal int LoanUpdatePassbookData(List<LoanPassbook_Print> prp)
        {
            int _ret = 0;

            string _query = "UPDATE MD_LOAN_PASSBOOK  "
                            + " SET PRINTED_FLAG = {0} "
                            + " WHERE ARDB_CD = {1} "
                            + " AND LOAN_ID = {2} "
                            + " AND TRANS_DT = to_date({3},'DD/MM/YYYY') "
                            + " AND TRANS_CD = {4} "
                            + " AND DEL_FLAG = 'N' ";



            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < prp.Count; i++)
                        {
                            _statement = string.Format(_query,
                        string.Concat("'", prp[i].printed_flag, "'"),
                        string.Concat("'", prp[i].ardb_cd, "'"),
                        string.Concat("'", prp[i].loan_id, "'"),
                        string.Concat("'", prp[i].trans_dt.ToString("dd/MM/yyyy"), "'"),
                        prp[i].trans_cd
                        );

                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();

                            }
                        }
                        transaction.Commit();
                        _ret = 0;
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


        internal int LoanUpdatePassbookline(p_report_param prp)
        {
            int _ret = 0;

            string _query = "UPDATE MD_PASSBOOK_ACC_LINE  "
                            + " SET LINES_PRINTED = {0} "
                            + " WHERE ARDB_CD = {1} "
                            + " AND ACC_TYPE_CD = {2} "
                            + " AND ACC_NUM = {3} "
                            + " AND DEL_FLAG = 'N' ";



            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement = string.Format(_query,
                                                prp.lines_printed,
                                                string.Concat("'", prp.ardb_cd, "'"),
                                                prp.acc_cd,
                                                string.Concat("'", prp.loan_id, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }

                        transaction.Commit();
                        _ret = 0;
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



        internal int LoanGetPassbookline(p_report_param prp)
        {

            int lines_printed = 0;

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            // prp.from_dt = prp.from_dt.Date;
            // prp.to_dt = prp.to_dt.Date.AddHours(23).AddMinutes(59).AddSeconds(59);

            string _query = " SELECT MD_PASSBOOK_ACC_LINE.LINES_PRINTED         "
                  + " FROM MD_PASSBOOK_ACC_LINE                                              "
                  + " WHERE (MD_PASSBOOK_ACC_LINE.ACC_TYPE_CD = {0} ) AND                   "
                  + "       (MD_PASSBOOK_ACC_LINE.ACC_NUM = '{1}' )                         "
                  + " AND   (MD_PASSBOOK_ACC_LINE.ARDB_CD = '{2}') AND (MD_PASSBOOK_ACC_LINE.DEL_FLAG = 'N')  ";

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
                                                  prp.acc_cd,
                                                  prp.loan_id,
                                                   prp.ardb_cd);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        lines_printed = UtilityM.CheckNull<int>(reader["LINES_PRINTED"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        lines_printed = -1;
                    }
                }
            }

            return lines_printed;
        }


        internal List<td_loan_charges> GetLoanCharges(p_report_param loan)
        {
            List<td_loan_charges> loanRet = new List<td_loan_charges>();
            string _query = " SELECT ARDB_CD,LOAN_ID,CHARGE_ID,CHARGE_TYPE,to_date(CHARGE_DT,'dd/mm/yyyy'),CHARGE_AMT,APPROVAL_STATUS,REMARKS "
                     + " FROM TD_LOAN_CHARGES  "
                     + " WHERE ARDB_CD = {0} AND LOAN_ID={1} ";

            _statement = string.Format(_query,
                                        !string.IsNullOrWhiteSpace(loan.ardb_cd) ? string.Concat("'", loan.ardb_cd, "'") : "ARDB_CD",
                                        !string.IsNullOrWhiteSpace(loan.loan_id) ? string.Concat("'", loan.loan_id, "'") : "LOAN_ID");

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new td_loan_charges();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                d.charge_id = UtilityM.CheckNull<int>(reader["CHARGE_ID"]);
                                d.charge_type = UtilityM.CheckNull<string>(reader["CHARGE_TYPE"]);
                                d.charge_dt = UtilityM.CheckNull<DateTime>(reader["CHARGE_DT"]);
                                d.charge_amt = UtilityM.CheckNull<decimal>(reader["CHARGE_AMT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                                loanRet.Add(d);
                            }
                        }
                    }
                }
            }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanRet = null;
                    }
                }
            }
            return loanRet;
        }


        internal int InsertLoanChargesData(td_loan_charges prp)
        {
            int _ret = 0;

            string _query = "INSERT INTO TD_LOAN_CHARGES  "
                            + " VALUES({0},{1},{2},{3},{4},{5},'U',{6},{7},SYSDATE,NULL,NULL,NULL,NULL,'N')";



            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                        string.Concat("'", prp.ardb_cd, "'"),
                        string.Concat("'", prp.loan_id, "'"),
                        prp.charge_id,
                        string.Concat("'", prp.charge_type, "'"),
                        string.Concat("'", prp.charge_dt.ToString("dd/MM/yyyy"), "'"),
                        prp.charge_amt,
                        string.Concat("'", prp.remarks, "'"),
                        prp.created_by
                        );

                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();

                            }
                    
                        transaction.Commit();
                        _ret = 0;
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


        internal int UpdateLoanChargesData(td_loan_charges prp)
        {
            int _ret = 0;

            string _query = " UPDATE TD_LOAN_CHARGES  "
                          + " SET CHARGE_TYPE = {0} , CHARGE_DT = {1} , CHARGE_AMT = {2}, REMARKS= {3}, MODIFIED_BY= {4}, MODIFIED_DT = SYSDATE  "
                          + " WHERE ARDB_CD = {5} AND LOAN_ID= {6} AND CHARGE_ID = {7}";



            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                        string.Concat("'", prp.charge_type, "'"),
                        string.Concat("'", prp.charge_dt.ToString("dd/MM/yyyy"), "'"),
                        prp.charge_amt,
                        string.Concat("'", prp.remarks, "'"),
                        string.Concat("'", prp.modified_by, "'"),
                        string.Concat("'", prp.ardb_cd, "'"),
                        string.Concat("'", prp.loan_id, "'"),
                        prp.charge_id
                        );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }

                        transaction.Commit();
                        _ret = 0;
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


        internal int ApproveLoanChargesData(td_loan_charges prp)
        {
            int _ret = 0;

            string _query = " UPDATE TD_LOAN_CHARGES  "
                          + " SET APPROVAL_STATUS = 'A', APPROVED_BY = {0} AND APPROVED_DT = SYSDATE  "
                          + " WHERE ARDB_CD = {1} AND LOAN_ID= {2} AND CHARGE_ID = {3}";

           using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                        string.Concat("'", prp.approved_by, "'"),
                        string.Concat("'", prp.ardb_cd, "'"),
                        string.Concat("'", prp.loan_id, "'"),
                        prp.charge_id
                        );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }

                        transaction.Commit();
                        _ret = 0;
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


        internal int DeleteLoanChargesData(td_loan_charges prp)
        {
            int _ret = 0;

            string _query = " UPDATE TD_LOAN_CHARGES  "
                          + " SET DEL_FLAG = 'Y', MODIFIED_BY = {0} AND MODIFIED_DT = SYSDATE  "
                          + " WHERE ARDB_CD = {1} AND LOAN_ID= {2} AND CHARGE_ID = {3} AND APPROVAL_STATUS = 'U' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                        string.Concat("'", prp.modified_by, "'"),
                        string.Concat("'", prp.ardb_cd, "'"),
                        string.Concat("'", prp.loan_id, "'"),
                        prp.charge_id
                        );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }

                        transaction.Commit();
                        _ret = 0;
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



       internal tm_gold_master GetGoldMaster(tm_gold_master pm)
        {
            tm_gold_master loanRet = new tm_gold_master();
            string _query = " SELECT TM_GOLD_MASTER.ARDB_CD,TM_GOLD_MASTER.BRN_CD,TM_GOLD_MASTER.MEMBER_NO,TM_GOLD_MASTER.CUST_CD,TM_GOLD_MASTER.VALUATION_NO,TM_GOLD_MASTER.VALUE_DT, "
                          + " TM_GOLD_MASTER.LOAN_ID,TM_GOLD_MASTER.LGE_PAGE,TM_GOLD_MASTER.DUE_DT,MM_CUSTOMER.CUST_NAME,MM_CUSTOMER.PRESENT_ADDRESS  "
                          + " FROM TM_GOLD_MASTER,MM_CUSTOMER WHERE TM_GOLD_MASTER.CUST_CD =  MM_CUSTOMER.CUST_CD AND "
                          + " TM_GOLD_MASTER.ARDB_CD = {0} AND TM_GOLD_MASTER.BRN_CD = {1} AND TM_GOLD_MASTER.VALUATION_NO = {2} ";

            _statement = string.Format(_query,
                                          string.Concat("'", pm.ardb_cd, "'"),
                                          string.Concat("'", pm.brn_cd, "'"),
                                          string.Concat("'", pm.valuation_no, "'")
                                           );
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new tm_gold_master();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    d.member_no = UtilityM.CheckNull<string>(reader["MEMBER_NO"]);
                                    d.cust_cd = UtilityM.CheckNull<string>(reader["CUST_CD"]);
                                    d.valuation_no = UtilityM.CheckNull<Int64>(reader["VALUATION_NO"]);
                                    d.value_dt = UtilityM.CheckNull<DateTime>(reader["VALUE_DT"]);
                                    d.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                    d.lge_page = UtilityM.CheckNull<string>(reader["LGE_PAGE"]);
                                    d.due_dt = UtilityM.CheckNull<DateTime>(reader["DUE_DT"]);
                                    d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                    d.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                    loanRet = d;
                                }
                            }
                        }
                    }
                }
            }
            return loanRet;
        }



        internal List<tm_gold_master_dtls> GetGoldMasterDtls(tm_gold_master_dtls pm)
        {
            List<tm_gold_master_dtls> loanRet = new List<tm_gold_master_dtls>();
            string _query = " SELECT TM_GOLD_MASTER_DTLS.ARDB_CD,TM_GOLD_MASTER_DTLS.BRN_CD,TM_GOLD_MASTER_DTLS.VALUATION_NO,TM_GOLD_MASTER_DTLS.SL_NO,TM_GOLD_MASTER_DTLS.DESC_VAL,TM_GOLD_MASTER_DTLS.DESC_NO,TM_GOLD_MASTER_DTLS.GROSS_WE, "
                          + " TM_GOLD_MASTER_DTLS.ALLOY_STONE_WE,TM_GOLD_MASTER_DTLS.NET_WE,TM_GOLD_MASTER_DTLS.PURITY_WE,TM_GOLD_MASTER_DTLS.ACT_WE,TM_GOLD_MASTER_DTLS.ACT_RATE,TM_GOLD_MASTER_DTLS.NET_VALUE "
                          + " FROM TM_GOLD_MASTER_DTLS WHERE TM_GOLD_MASTER_DTLS.ARDB_CD = {0} AND TM_GOLD_MASTER_DTLS.BRN_CD = {1} AND TM_GOLD_MASTER_DTLS.VALUATION_NO = {2} ";

            _statement = string.Format(_query,
                                           string.Concat("'", pm.ardb_cd, "'") ,
                                           string.Concat("'", pm.brn_cd, "'"),
                                          string.Concat("'", pm.valuation_no, "'")
                                           );
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new tm_gold_master_dtls();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    d.valuation_no = UtilityM.CheckNull<Int64>(reader["VALUATION_NO"]);
                                    d.sl_no = UtilityM.CheckNull<Int16>(reader["SL_NO"]);
                                    d.desc_val = UtilityM.CheckNull<string>(reader["DESC_VAL"]);
                                    d.desc_no = UtilityM.CheckNull<Int16>(reader["DESC_NO"]);
                                    d.gross_we = UtilityM.CheckNull<decimal>(reader["GROSS_WE"]);
                                    d.alloy_stone_we = UtilityM.CheckNull<decimal>(reader["ALLOY_STONE_WE"]);
                                    d.net_we = UtilityM.CheckNull<decimal>(reader["NET_WE"]);
                                    d.purity_we = UtilityM.CheckNull<decimal>(reader["PURITY_WE"]);
                                    d.act_we = UtilityM.CheckNull<decimal>(reader["ACT_WE"]);
                                    d.act_rate = UtilityM.CheckNull<decimal>(reader["ACT_RATE"]);
                                    d.net_value = UtilityM.CheckNull<decimal>(reader["NET_VALUE"]);
                                    loanRet.Add(d);
                                }
                            }
                        }
                    }
                }
            }
            return loanRet;
        }



          internal string InsertLoanValuationData(LoanValuationDM acc)
          {
              string _section = null;
                Int64 maxvaluationno;

              using (var connection = OrclDbConnection.NewConnection)
              {
                  using (var transaction = connection.BeginTransaction())
                  {
                      try
                      {
                            _section = "InsertGoldMaster";
                            maxvaluationno = InsertGoldMaster(connection, acc.tmgoldmaster);
                            _section = "InsertGoldMasterDtls";
                            InsertGoldMasterDtls(connection, acc.tmgoldmasterdtls);

                            transaction.Commit();
                            return maxvaluationno.ToString();

                    }
                      catch (Exception ex)
                      {

                          transaction.Rollback();
                          return _section + " : " + ex.Message;
                      }

                  }
              }
          }


        internal Int64 InsertGoldMaster(DbConnection connection, tm_gold_master dep)
        {
            string _query = " INSERT INTO TM_GOLD_MASTER(ARDB_CD,BRN_CD,MEMBER_NO,CUST_CD,VALUATION_NO,VALUE_DT,"
                           + " LOAN_ID,LGE_PAGE,DUE_DT,CREATED_BY,CREATED_DT)  "
                          + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},SYSDATE)";

            Int64 maxvaluationno = PopulateValuationNumber(dep.ardb_cd,dep.brn_cd,dep.value_dt);

            _statement = string.Format(_query,
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.member_no, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.valuation_no, "'"),
            string.IsNullOrWhiteSpace(dep.value_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.value_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.loan_id, "'"),
            string.Concat("'", dep.lge_page, "'"),
            string.IsNullOrWhiteSpace(dep.due_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.due_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.created_by, "'")
                  );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return maxvaluationno;
        }


        internal bool InsertGoldMasterDtls(DbConnection connection, List<tm_gold_master_dtls> dep)
        {
            string _query = " INSERT INTO TM_GOLD_MASTER_DTLS(ARDB_CD,BRN_CD,VALUATION_NO,SL_NO,DESC_VAL,DESC_NO,GROSS_WE,ALLOY_STONE_WE,"
                           + " NET_WE,PURITY_WE,ACT_WE,ACT_RATE,NET_VALUE,CREATED_BY,CREATED_DT)  "
                          + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},SYSDATE)";

            for (int i = 0; i < dep.Count; i++)
            {
                 _statement = string.Format(_query,
                 string.Concat("'", dep[i].ardb_cd, "'"),
                 string.Concat("'", dep[i].brn_cd, "'"),
                 string.Concat("'", dep[i].valuation_no, "'"),
                 string.Concat("'", dep[i].sl_no, "'"),
                 string.Concat("'", dep[i].desc_val, "'"),
                 string.Concat("'", dep[i].desc_no, "'"),
                 string.Concat("'", dep[i].gross_we, "'"),
                 string.Concat("'", dep[i].alloy_stone_we, "'"),
                 string.Concat("'", dep[i].net_we, "'"),
                 string.Concat("'", dep[i].purity_we, "'"),
                 string.Concat("'", dep[i].act_we, "'"),
                 string.Concat("'", dep[i].act_rate, "'"),
                 string.Concat("'", dep[i].net_value, "'"),
                 string.Concat("'", dep[i].created_by, "'")
                   );

                    using (var command = OrclDbConnection.Command(connection, _statement))
                    {
                        command.ExecuteNonQuery();
                    }
            }
            return true;
        }


        internal Int64 PopulateValuationNumber(string ardb,string brn,DateTime valudt)
        {
             Int64 accNum  = 0;

            string _query = "Select f_get_valuation_no({0},{1},{2}) VALUATION_NO"
                           + " From  DUAL ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement = string.Format(_query,
                                        string.Concat("'", ardb, "'"),
                                         string.Concat("'", brn, "'"),
                                         string.Concat("'", valudt, "'")
                                        );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        accNum = UtilityM.CheckNull<Int64>(reader["VALUATION_NO"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        accNum = 0;
                    }
                }
            }
            return accNum;
        }


        internal int UpdateLoanValuationData(LoanValuationDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        UpdateGoldMaster(connection, acc.tmgoldmaster);
                        if (acc.tmgoldmasterdtls.Count > 0)
                          UpdateGoldMasterDtls(connection, acc.tmgoldmasterdtls);                        
                        transaction.Commit();
                        return 0;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }

                }
            }
        }


        internal bool UpdateGoldMaster(DbConnection connection, tm_gold_master dep)
        {
            string _query = " UPDATE  TM_GOLD_MASTER SET "
                  + "member_no          = NVL({0},  member_no         ),"
                  + "cust_cd            = NVL({1},  cust_cd           ),"
                  + "value_dt           = NVL({2},  value_dt          ),"
                  + "due_dt             = NVL({3},  due_dt            ), "
                  + "modified_by        = NVL({4},  modified_by       ), "
                  + "modified_dt        = sysdate                       "
                  + "WHERE ardb_cd  ={5} and brn_cd = NVL({6}, brn_cd) AND valuation_no = NVL({7},  valuation_no ) ";

            _statement = string.Format(_query,

            string.Concat("'", dep.member_no, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.value_dt, "'"),
            string.Concat("'", dep.due_dt, "'"),
            string.Concat("'", dep.modified_by, "'"),
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.valuation_no, "'")
            );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }



        internal bool UpdateGoldMasterDtls(DbConnection connection, List<tm_gold_master_dtls> dep)
        {
            string _query = " UPDATE  TM_GOLD_MASTER_DTLS SET "
                  + " sl_no              = NVL({0},  sl_no  ),"
                  + " desc_val           = NVL({1},  desc_val ),"
                  + " desc_no            = NVL({2},  desc_no  ),"
                  + " gross_we           = NVL({3},  gross_we ), "
                  + " alloy_stone_we     = NVL({4},  alloy_stone_we), "
                  + " net_we             = NVL({5},  net_we),  "
                  + " purity_we          = NVL({6},  purity_we),  "
                  + " act_we             = NVL({7},  act_we),  "
                  + " act_rate           = NVL({8},  act_rate),  "
                  + " net_value          = NVL({9},  net_value),  "
                  + " modified_by        = NVL({10},  modified_by),  "
                  + " modified_dt        = sysdate  "
                  + " WHERE ardb_cd  ={11} and brn_cd = NVL({12}, brn_cd) AND valuation_no = NVL({13},  valuation_no ) ";
            for (int i = 0; i < dep.Count; i++)
            {

                    _statement = string.Format(_query,

                string.Concat("'", dep[i].sl_no, "'"),
                string.Concat("'", dep[i].desc_val, "'"),
                string.Concat("'", dep[i].desc_no, "'"),
                string.Concat("'", dep[i].gross_we, "'"),
                string.Concat("'", dep[i].alloy_stone_we, "'"),
                string.Concat("'", dep[i].net_we, "'"),
                string.Concat("'", dep[i].purity_we, "'"),
                string.Concat("'", dep[i].act_we, "'"),
                string.Concat("'", dep[i].act_rate, "'"),
                 string.Concat("'", dep[i].net_value, "'"),
                  string.Concat("'", dep[i].modified_by, "'"),
                  string.Concat("'", dep[i].ardb_cd, "'"),
                  string.Concat("'", dep[i].brn_cd, "'"),
                  string.Concat("'", dep[i].valuation_no, "'")
                );

                    using (var command = OrclDbConnection.Command(connection, _statement))
                    {
                        command.ExecuteNonQuery();
                    }
            }
            return true;

        }



        internal List<activitywisedc_type> PopulateDcStatement(p_report_param prp)
        {
            List<activitywisedc_type> loanDfltList = new List<activitywisedc_type>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _procedure = "P_DC_FARM_NEW";
            string _query = " SELECT  TT_DC_FARM.LOAN_ID,"
                            + "TT_DC_FARM.PARTY_NAME ,  "
                            + "TT_DC_FARM.BLOCK_NAME,  "
                            + "TT_DC_FARM.APP_RECEPT_DT, "
                            + "TT_DC_FARM.LOAN_CASE_NO, "
                            + "TT_DC_FARM.SANC_DT, "
                            + "TT_DC_FARM.SANC_AMT, "
                            + "TT_DC_FARM.DISB_AMT,  "
                            + "TT_DC_FARM.DISB_DT, "
                            + "TT_DC_FARM.ACTIVITY_CD,  "
                            + "TT_DC_FARM.LAND_VALUE,  "
                            + "TT_DC_FARM.LAND_AREA,  "
                            + "TT_DC_FARM.ADDI_LAND_AMT,  "
                            + "TT_DC_FARM.PROJECT_COST, "
                            + "TT_DC_FARM.NET_INCOME_GEN, "
                            + "TT_DC_FARM.BOND_NO, "
                            + "TT_DC_FARM.BOND_DT, "
                            + "TT_DC_FARM.OPERATOR_NAME, "
                            + "TT_DC_FARM.DEP_AMT, "
                            + "TT_DC_FARM.LSO_NO, "
                            + "TT_DC_FARM.LSO_DATE, "
                            + "TT_DC_FARM.SEX, "
                            + "TT_DC_FARM.CULTI_VALUE, "
                            + "TT_DC_FARM.CULTI_AREA, "
                            + "TT_DC_FARM.PRONOTE_AMT, "
                            + "TT_DC_FARM.MACHINE "
                            + " FROM TT_DC_FARM  WHERE LOAN_ID = {0} ORDER BY TT_DC_FARM.DISB_DT";

            string _query1 = " SELECT DISTINCT TT_DC_FARM.ACTIVITY_CD,SEX  "
                            + " FROM TT_DC_FARM  WHERE TT_DC_FARM.ACTIVITY_CD = {0} AND TT_DC_FARM.SEX = {1}";

            string _query2 = " SELECT DISTINCT TT_DC_FARM.LOAN_ID  "
                           + " FROM TT_DC_FARM WHERE ACTIVITY_CD = {0}  AND  TT_DC_FARM.SEX = {1} ";



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

                        _statement = string.Format(_procedure);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);
                            

                            command.ExecuteNonQuery();

                        }

                        transaction.Commit();

                        string _statement1 = string.Format(_query1,
                                            string.Concat("'", prp.activity_cd, "'"),
                                            string.Concat("'", prp.sex, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        activitywisedc_type tcaRet1 = new activitywisedc_type();


                                        tcaRet1.activity_cd = UtilityM.CheckNull<string>(reader1["ACTIVITY_CD"]);
                                        tcaRet1.sex = UtilityM.CheckNull<string>(reader1["SEX"]);

                                        string _statement2 = string.Format(_query2,
                                            string.Concat("'", prp.activity_cd, "'"),
                                            string.Concat("'", prp.sex, "'"));

                                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                                        {
                                            using (var reader2 = command2.ExecuteReader())
                                            {
                                                if (reader2.HasRows)
                                                {
                                                    while (reader2.Read())
                                                    {

                                                        activitywiseloan tcaRet2 = new activitywiseloan();


                                                        tcaRet2.loan_id = UtilityM.CheckNull<string>(reader2["LOAN_ID"]);



                                                        _statement = string.Format(_query,
                                                                     string.Concat("'", UtilityM.CheckNull<string>(reader2["LOAN_ID"]), "'")
                                                                  );

                                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                                        {
                                                            using (var reader = command.ExecuteReader())
                                                            {
                                                                if (reader.HasRows)
                                                                {
                                                                    while (reader.Read())
                                                                    {
                                                                        var loanDtl = new dcstatement();

                                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                                        loanDtl.party_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                                                        loanDtl.block_name = UtilityM.CheckNull<string>(reader["BLOCK_NAME"]);
                                                                        loanDtl.application_dt = UtilityM.CheckNull<DateTime>(reader["APP_RECEPT_DT"]);
                                                                        loanDtl.loan_case_no = UtilityM.CheckNull<string>(reader["LOAN_CASE_NO"]);
                                                                        loanDtl.sanction_dt = UtilityM.CheckNull<DateTime>(reader["SANC_DT"]);
                                                                        loanDtl.sanction_amt = UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                                                        loanDtl.disb_amt = UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                                                        loanDtl.disb_dt = UtilityM.CheckNull<DateTime>(reader["DISB_DT"]);
                                                                        loanDtl.activity = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                                                        loanDtl.land_value = UtilityM.CheckNull<decimal>(reader["LAND_VALUE"]);
                                                                        loanDtl.land_area = UtilityM.CheckNull<string>(reader["LAND_AREA"]);
                                                                        loanDtl.addl_land_area = UtilityM.CheckNull<string>(reader["ADDI_LAND_AMT"]);
                                                                        loanDtl.project_cost = UtilityM.CheckNull<decimal>(reader["PROJECT_COST"]);
                                                                        loanDtl.net_income_gen = UtilityM.CheckNull<decimal>(reader["NET_INCOME_GEN"]);
                                                                        loanDtl.bond_no = UtilityM.CheckNull<string>(reader["BOND_NO"]);
                                                                        loanDtl.bond_dt = UtilityM.CheckNull<DateTime>(reader["BOND_DT"]);
                                                                        loanDtl.operator_name = UtilityM.CheckNull<string>(reader["OPERATOR_NAME"]);
                                                                        loanDtl.deposit_amt = UtilityM.CheckNull<decimal>(reader["DEP_AMT"]);
                                                                        loanDtl.own_contribution = UtilityM.CheckNull<decimal>(reader["PROJECT_COST"]) - UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                                                        loanDtl.lso_no = UtilityM.CheckNull<string>(reader["LSO_NO"]);
                                                                        loanDtl.lso_dt = UtilityM.CheckNull<DateTime>(reader["LSO_DATE"]);
                                                                        loanDtl.sex = UtilityM.CheckNull<string>(reader["SEX"]);
                                                                        loanDtl.culti_area = UtilityM.CheckNull<string>(reader["CULTI_AREA"]);
                                                                        loanDtl.culti_val = UtilityM.CheckNull<decimal>(reader["CULTI_VALUE"]);
                                                                        loanDtl.pronote_amt = UtilityM.CheckNull<decimal>(reader["PRONOTE_AMT"]);
                                                                        loanDtl.machine = UtilityM.CheckNull<string>(reader["MACHINE"]);


                                                                        tcaRet2.dc_statement.Add(loanDtl);

                                                                        tcaRet1.tot_disb = tcaRet1.tot_disb + UtilityM.CheckNull<decimal>(reader["DISB_AMT"]);
                                                                        tcaRet1.tot_project = tcaRet1.tot_project + UtilityM.CheckNull<decimal>(reader["PROJECT_COST"]);
                                                                        tcaRet1.tot_pronote = tcaRet1.tot_pronote + UtilityM.CheckNull<decimal>(reader["PRONOTE_AMT"]);
                                                                        tcaRet1.tot_sanc = tcaRet1.tot_sanc + UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);
                                                                        tcaRet1.tot_own_contribution = tcaRet1.tot_own_contribution + UtilityM.CheckNull<decimal>(reader["PROJECT_COST"]) - UtilityM.CheckNull<decimal>(reader["SANC_AMT"]);


                                                                    }

                                                                   // transaction.Commit();
                                                                }
                                                            }
                                                        }

                                                        tcaRet1.dclist.Add(tcaRet2);
                                                        
                                                    }
                                                }
                                            }
                                        }
                                        loanDfltList.Add(tcaRet1);
                                        
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        loanDfltList = null;
                    }
                }
            }
            return loanDfltList;
        }


    }
}



