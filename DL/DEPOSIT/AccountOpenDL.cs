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
    public class AccountOpenDL
    {
        string _statement;

        internal decimal F_CALCRDINTT_REG(p_gen_param prp)
        {
            decimal amount = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT F_CALCRDINTT_REG({0},{1},{2},{3},{4}) AMOUNT FROM DUAL";
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
                                         string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.as_acc_num, "'"),
                                         string.Concat("'", prp.ad_instl_amt, "'"),
                                         string.Concat("'", prp.an_instl_no, "'"),
                                         string.Concat("'", prp.an_intt_rate, "'")
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


        internal decimal F_CALCTDINTT_REG(p_gen_param prp)
        {
            decimal amount = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT F_CALCTDINTT_REG({0},{1},to_date('{2}','dd-mm-yyyy' ),{3},{4},{5}) AMOUNT FROM DUAL";
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
                                         string.Concat("'", prp.ad_acc_type_cd, "'"),
                                         string.Concat("'", prp.ad_prn_amt, "'"),
                                         string.Concat(prp.adt_temp_dt.ToString("dd/MM/yyyy")),
                                         string.Concat("'", prp.as_intt_type, "'"),
                                         string.Concat("'", prp.ai_period, "'"),
                                         string.Concat("'", prp.ad_intt_rt, "'")
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


        internal decimal F_CALC_SB_INTT(p_gen_param prm)
        {
            decimal amount = 0;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, "P_SB_PRODUCT_PATCH_NEW"))
                        {
                            prm.from_dt = prm.from_dt.AddDays(1);
                            // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("ad_acc_type", OracleDbType.Decimal, ParameterDirection.Input);
                            parm.Value = prm.ad_acc_type_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_acc_num", OracleDbType.Decimal, ParameterDirection.Input);
                            parm.Value = prm.as_acc_num;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.from_dt;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.to_dt;
                            command.Parameters.Add(parm);
                            using (var reader = command.ExecuteReader())
                            {
                                // transaction.Commit();
                                using (var command2 = OrclDbConnection.Command(connection, "P_GET_SB_INTT"))
                                {
                                    command2.CommandType = System.Data.CommandType.StoredProcedure;
                                    parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm.Value = prm.ardb_cd;
                                    command2.Parameters.Add(parm);
                                    parm = new OracleParameter("ad_acc_type", OracleDbType.Decimal, ParameterDirection.Input);
                                    parm.Value = prm.ad_acc_type_cd;
                                    command2.Parameters.Add(parm);
                                    parm = new OracleParameter("as_acc_num", OracleDbType.Decimal, ParameterDirection.Input);
                                    parm.Value = prm.as_acc_num;
                                    command2.Parameters.Add(parm);
                                    parm = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm.Value = prm.to_dt;
                                    command2.Parameters.Add(parm);
                                    parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm.Value = prm.brn_cd;
                                    command2.Parameters.Add(parm);
                                    parm = new OracleParameter("p_sbintt_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                                    command2.Parameters.Add(parm);
                                    using (var reader2 = command2.ExecuteReader())
                                    {
                                        // transaction.Commit();
                                        try
                                        {
                                            if (reader2.HasRows)
                                            {
                                                while (reader2.Read())
                                                {
                                                    amount = UtilityM.CheckNull<decimal>(reader2["ld_intt"]);
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {                                           		
                                            transaction.Rollback();
                                            amount = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        var abc = ex.Message;                        
                        transaction.Rollback();
                        amount = 0;
                    }
                }
            }
            return amount;
        }


      /*  internal decimal F_CALC_DDS_INTT(p_gen_param prm)
        {
            decimal amount = 0;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, "P_DDS_INTT_NEW"))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_acc_num", OracleDbType.Decimal, ParameterDirection.Input);
                            parm.Value = prm.as_acc_num;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.to_dt;
                            command.Parameters.Add(parm);
                            using (var reader = command.ExecuteReader())
                            {
                                string _query = "SELECT F_CALC_INTEREST_DDS({0},{1},to_date('{2}','dd-mm-yyyy' )) AMOUNT FROM DUAL";
                                try
                                {
                                    _statement = string.Format(_query,
                                                     string.Concat("'", prm.ardb_cd, "'"),
                                                     string.Concat("'", prm.as_acc_num, "'"),
                                                     string.Concat("to_date('", prm.to_dt.ToString("dd/MM/yyyy"), "' ,'dd/MM/yyyy')")                                                     
                                                    );
                                    using (var command2 = OrclDbConnection.Command(connection, _statement))
                                    {
                                        using (var reader2 = command2.ExecuteReader())
                                        {
                                            if (reader.HasRows)
                                            {
                                                while (reader.Read())
                                                {
                                                    amount = UtilityM.CheckNull<decimal>(reader2["AMOUNT"]);
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
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        var abc = ex.Message;
                        transaction.Rollback();
                        amount = 0;
                    }
                }
            }
            return amount;
        }*/



        // internal decimal F_CALC_SB_INTT(p_gen_param prp)
        // {
        //     decimal amount = 0;
        //     string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
        //     string _query = "SELECT f_calc_sb_intt({0},to_date('{1}','dd-mm-yyyy' ),{2}) AMOUNT FROM DUAL";
        //     using (var connection = OrclDbConnection.NewConnection)
        //     {
        //         using (var transaction = connection.BeginTransaction())
        //         {
        //             try
        //             {
        //                 using (var command = OrclDbConnection.Command(connection, _alter))
        //                 {
        //                     command.ExecuteNonQuery();
        //                 }
        //                 _statement = string.Format(_query,
        //                                  string.Concat("'", prp.as_acc_num, "'"),
        //                                  string.Concat( DateTime.Now.ToString("dd/MM/yyyy")),
        //                                  string.Concat("'", prp.brn_cd, "'")
        //                                 );
        //                 using (var command = OrclDbConnection.Command(connection, _statement))
        //                 {
        //                     using (var reader = command.ExecuteReader())
        //                     {
        //                         if (reader.HasRows)
        //                         {
        //                             while (reader.Read())
        //                             {
        //                                 amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //             catch (Exception ex)
        //             {
        //                 transaction.Rollback();
        //                 amount = 0;
        //             }
        //         }
        //     }
        //     return amount;
        // }

        internal decimal F_CAL_RD_PENALTY(p_gen_param prp)
        {
            decimal amount = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT F_CAL_RD_PENALTY({0},{1}) AMOUNT FROM DUAL";
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
                                         string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.as_acc_num, "'")
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


                
        internal float GET_INT_RATE(p_gen_param prp)
        {
            float int_rate = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = " SELECT INT_RATE FROM (Select intr.intt_rate INT_RATE "
                            + " From mm_acc_type_group act, mm_interest intr "
                            + "  Where intr.ardb_cd= {0} and act.acc_type_cd = {1} "
                            + " and intr.acc_type_cd = act.acc_group_cd "
                            + " and intr.effective_dt <= {2} "
                            + " and intr.catg_cd      = {3} "
                            + "and intr.no_of_days >= {4}   ORDER BY intr.EFFECTIVE_DT DESC ,intr.NO_OF_DAYS)"
                            + " Where rownum = 1 ";

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
                                                   string.Concat("'", prp.ardb_cd, "'"),
                                                   prp.acc_cd,
                                                   string.Concat("to_date('", prp.from_dt.ToString("dd/MM/yyyy"), "' ,'dd/MM/yyyy')"),
                                                   prp.ls_catg_cd,
                                                   prp.ai_period);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        int_rate = UtilityM.CheckNull<float>(reader["INT_RATE"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        int_rate = 0;
                    }
                }
            }
            return int_rate;
        }



        internal string PopulateAccountNumber(p_gen_param prp)
        {
            string accNum = "";
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT F_GET_ACCOUNT_NUMBER({0},{1},{2},{3},{4}) ACC_NUM FROM DUAL";
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
                                         string.Concat("'", prp.ardb_cd, "'"),
                                         string.Concat("'", prp.brn_cd, "'"),
                                         string.Concat("'", prp.gs_acc_type_cd, "'"),
                                         string.Concat("'", prp.ls_catg_cd, "'"),
                                         string.Concat("'", prp.ls_cons_cd, "'")
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
            return accNum;
        }



        internal string InsertAccountOpeningData(AccOpenDM acc)
        {
            string _section = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section = "GetTransCDMaxId";
                        int maxTransCD = GetTransCDMaxId(connection, acc.tddeftrans);
                        _section = "InsertDepositTemp";
                        if (!String.IsNullOrWhiteSpace(acc.tmdeposit.acc_num))
                            InsertDepositTemp(connection, acc.tmdeposit);
                        _section = "InsertDepositRenewTemp";
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositrenew.acc_num))
                            InsertDepositRenewTemp(connection, acc.tmdepositrenew, maxTransCD);
                        _section = "InsertIntroducerTemp";
                        if (acc.tdintroducer.Count > 0)
                            InsertIntroducerTemp(connection, acc.tdintroducer);
                        _section = "InsertNomineeTemp";
                        if (acc.tdnominee.Count > 0)
                            InsertNomineeTemp(connection, acc.tdnominee);
                        _section = "InsertSignatoryTemp";
                        if (acc.tdsignatory.Count > 0)
                            InsertSignatoryTemp(connection, acc.tdsignatory);
                        _section = "InsertAccholderTemp";
                        if (acc.tdaccholder.Count > 0)
                            InsertAccholderTemp(connection, acc.tdaccholder);
                        _section = "InsertDenominationDtls";
                        if (acc.tmdenominationtrans.Count > 0)
                            InsertDenominationDtls(connection, acc.tmdenominationtrans, maxTransCD);
                        _section = "InsertTransfer";
                        if (acc.tmtransfer.Count > 0)
                            InsertTransfer(connection, acc.tmtransfer, maxTransCD);
                        _section = "InsertDepTransTrf";
                        if (acc.tddeftranstrf.Count > 0)
                            InsertDepTransTrf(connection, acc.tddeftranstrf, maxTransCD);
                        _section = "InsertDepTrans";
                        if (!String.IsNullOrWhiteSpace(maxTransCD.ToString()) && maxTransCD != 0)
                            InsertDepTrans(connection, acc.tddeftrans, maxTransCD);
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


        internal int UpdateAccountOpeningData(AccOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.tmdeposit.acc_num))
                            UpdateDepositTemp(connection, acc.tmdeposit);
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositrenew.acc_num))
                            UpdateDepositRenewTemp(connection, acc.tmdepositrenew);
                        if (acc.tdintroducer.Count > 0)
                            UpdateIntroducerTemp(connection, acc.tdintroducer);
                        if (acc.tdnominee.Count > 0)
                            UpdateNomineeTemp(connection, acc.tdnominee);
                        if (acc.tdsignatory.Count > 0)
                            UpdateSignatoryTemp(connection, acc.tdsignatory);
                        if (acc.tdaccholder.Count > 0)
                            UpdateAccholderTemp(connection, acc.tdaccholder);
                        if (acc.tmdenominationtrans.Count > 0)
                        {
                           for (int i = 0; i < acc.tmdenominationtrans.Count; i++)
                           {
                               acc.tmdenominationtrans[i].trans_cd = acc.tddeftrans.trans_cd;
                           }
                            UpdateDenominationDtls(connection, acc.tmdenominationtrans);
                        }

                        if (acc.tmtransfer.Count > 0)
                            UpdateTransfer(connection, acc.tmtransfer);
                        if (acc.tddeftranstrf.Count > 0)
                            UpdateDepTransTrf(connection, acc.tddeftranstrf);
                        if (!String.IsNullOrWhiteSpace(acc.tddeftrans.trans_cd.ToString()))
                            UpdateDepTrans(connection, acc.tddeftrans);
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



        internal int DeleteAccountOpeningData(td_def_trans_trf acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.acc_num))
                        {
                            DeleteDepositTemp(connection, acc);

                            DeleteDepositRenewTemp(connection, acc);

                            DeleteIntroducerTemp(connection, acc);

                            DeleteNomineeTemp(connection, acc);

                            DeleteSignatoryTemp(connection, acc);

                            DeleteAccholderTemp(connection, acc);

                            DeleteDenominationDtls(connection, acc);

                            DeleteTransfer(connection, acc);

                            DeleteDepTransTrf(connection, acc);

                            DeleteDepTrans(connection, acc);
                            transaction.Commit();
                        }
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




        internal AccOpenDM GetAccountOpeningTempData(tm_deposit td)
        {
            AccOpenDM AccOpenDMRet = new AccOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        AccOpenDMRet.tmdeposit = GetDepositTemp(connection, td);
                        AccOpenDMRet.tmdepositrenew = GetDepositRenewTemp(connection, td);
                        AccOpenDMRet.tdintroducer = GetIntroducerTemp(connection, td);
                        AccOpenDMRet.tdnominee = GetNomineeTemp(connection, td);
                        AccOpenDMRet.tdsignatory = GetSignatoryTemp(connection, td);
                        AccOpenDMRet.tdaccholder = GetAccholderTemp(connection, td);
                        AccOpenDMRet.tddeftrans = GetDepTrans(connection, td);
                        if (!String.IsNullOrWhiteSpace(AccOpenDMRet.tddeftrans.trans_cd.ToString()) && AccOpenDMRet.tddeftrans.trans_cd > 0)
                        {
                            td.trans_cd = AccOpenDMRet.tddeftrans.trans_cd;
                            td.trans_dt = AccOpenDMRet.tddeftrans.trans_dt;
                            AccOpenDMRet.tmdenominationtrans = GetDenominationDtls(connection, td);
                            AccOpenDMRet.tmtransfer = GetTransfer(connection, td);
                            AccOpenDMRet.tddeftranstrf = GetDepTransTrf(connection, td);
                        }
                        // transaction.Commit();
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




        internal tm_deposit GetDepositTemp(DbConnection connection, tm_deposit dep)
        {
            tm_deposit depRet = new tm_deposit();
            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT_TEMP                                                                  "
                            + " WHERE ARDB_CD = {0} AND BRN_CD={1} AND ACC_NUM={2} AND ACC_TYPE_CD={3} AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
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
                                var d = new tm_deposit();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                d.renew_id = UtilityM.CheckNull<int>(reader["RENEW_ID"]);
                                d.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                d.intt_trf_type = UtilityM.CheckNull<string>(reader["INTT_TRF_TYPE"]);
                                d.constitution_cd = UtilityM.CheckNull<int>(reader["CONSTITUTION_CD"]);
                                d.oprn_instr_cd = UtilityM.CheckNull<int>(reader["OPRN_INSTR_CD"]);
                                d.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                d.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                d.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);
                                d.dep_period = UtilityM.CheckNull<string>(reader["DEP_PERIOD"]);
                                d.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                d.instl_no = UtilityM.CheckNull<decimal>(reader["INSTL_NO"]);
                                d.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                d.intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                d.tds_applicable = UtilityM.CheckNull<string>(reader["TDS_APPLICABLE"]);
                                d.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                d.acc_close_dt = UtilityM.CheckNull<DateTime>(reader["ACC_CLOSE_DT"]);
                                d.closing_prn_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_PRN_AMT"]);
                                d.closing_intt_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_INTT_AMT"]);
                                d.penal_amt = UtilityM.CheckNull<decimal>(reader["PENAL_AMT"]);
                                d.ext_instl_tot = UtilityM.CheckNull<decimal>(reader["EXT_INSTL_TOT"]);
                                d.mat_status = UtilityM.CheckNull<string>(reader["MAT_STATUS"]);
                                d.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                d.curr_bal = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                d.clr_bal = UtilityM.CheckNull<decimal>(reader["CLR_BAL"]);
                                d.standing_instr_flag = UtilityM.CheckNull<string>(reader["STANDING_INSTR_FLAG"]);
                                d.cheque_facility_flag = UtilityM.CheckNull<string>(reader["CHEQUE_FACILITY_FLAG"]);
                                d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                d.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                d.user_acc_num = UtilityM.CheckNull<string>(reader["USER_ACC_NUM"]);
                                d.lock_mode = UtilityM.CheckNull<string>(reader["LOCK_MODE"]);
                                d.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]);
                                d.cert_no = UtilityM.CheckNull<string>(reader["CERT_NO"]);
                                d.bonus_amt = UtilityM.CheckNull<decimal>(reader["BONUS_AMT"]);
                                d.penal_intt_rt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RT"]);
                                d.bonus_intt_rt = UtilityM.CheckNull<decimal>(reader["BONUS_INTT_RT"]);
                                d.transfer_flag = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                d.transfer_dt = UtilityM.CheckNull<DateTime>(reader["TRANSFER_DT"]);
                                d.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }




        internal tm_deposit GetDepositRenewTemp(DbConnection connection, tm_deposit dep)
        {
            tm_deposit depRet = new tm_deposit();
            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, CATG_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT_RENEW_TEMP                                                                  "
                            + " WHERE ARDB_CD = {0} AND BRN_CD={1} AND ACC_NUM={2} AND ACC_TYPE_CD={3} AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
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
                                var d = new tm_deposit();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                d.renew_id = UtilityM.CheckNull<int>(reader["RENEW_ID"]);
                                d.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                d.intt_trf_type = UtilityM.CheckNull<string>(reader["INTT_TRF_TYPE"]);
                                d.constitution_cd = UtilityM.CheckNull<int>(reader["CONSTITUTION_CD"]);
                                d.oprn_instr_cd = UtilityM.CheckNull<int>(reader["OPRN_INSTR_CD"]);
                                d.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                d.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                d.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);
                                d.dep_period = UtilityM.CheckNull<string>(reader["DEP_PERIOD"]);
                                d.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                d.instl_no = UtilityM.CheckNull<decimal>(reader["INSTL_NO"]);
                                d.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                d.intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                d.tds_applicable = UtilityM.CheckNull<string>(reader["TDS_APPLICABLE"]);
                                d.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                d.acc_close_dt = UtilityM.CheckNull<DateTime>(reader["ACC_CLOSE_DT"]);
                                d.closing_prn_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_PRN_AMT"]);
                                d.closing_intt_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_INTT_AMT"]);
                                d.penal_amt = UtilityM.CheckNull<decimal>(reader["PENAL_AMT"]);
                                d.ext_instl_tot = UtilityM.CheckNull<decimal>(reader["EXT_INSTL_TOT"]);
                                d.mat_status = UtilityM.CheckNull<string>(reader["MAT_STATUS"]);
                                d.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                d.curr_bal = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                d.clr_bal = UtilityM.CheckNull<decimal>(reader["CLR_BAL"]);
                                d.standing_instr_flag = UtilityM.CheckNull<string>(reader["STANDING_INSTR_FLAG"]);
                                d.cheque_facility_flag = UtilityM.CheckNull<string>(reader["CHEQUE_FACILITY_FLAG"]);
                                d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                d.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                d.user_acc_num = UtilityM.CheckNull<string>(reader["USER_ACC_NUM"]);
                                d.lock_mode = UtilityM.CheckNull<string>(reader["LOCK_MODE"]);
                                d.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]);
                                d.cert_no = UtilityM.CheckNull<string>(reader["CERT_NO"]);
                                d.bonus_amt = UtilityM.CheckNull<decimal>(reader["BONUS_AMT"]);
                                d.penal_intt_rt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RT"]);
                                d.bonus_intt_rt = UtilityM.CheckNull<decimal>(reader["BONUS_INTT_RT"]);
                                d.transfer_flag = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                d.transfer_dt = UtilityM.CheckNull<DateTime>(reader["TRANSFER_DT"]);
                                d.agent_cd = UtilityM.CheckNull<Int32>(reader["CATG_CD"]).ToString();
                                d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }



        internal List<td_introducer> GetIntroducerTemp(DbConnection connection, tm_deposit dep)
        {
            List<td_introducer> indList = new List<td_introducer>();
            string _query = "SELECT ARDB_CD,BRN_CD, "
                 + " ACC_TYPE_CD,        "
                 + " ACC_NUM,            "
                 + " SRL_NO,             "
                 + " INTRODUCER_NAME,    "
                 + " INTRODUCER_ACC_TYPE,"
                 + " INTRODUCER_ACC_NUM ,DEL_FLAG "
                 + " FROM TD_INTRODUCER_TEMP  "
                 + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2}  AND ACC_TYPE_CD = {3} AND DEL_FLAG='N' ";

            _statement = string.Format(_query,
            !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
            !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
            !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
            (dep.acc_type_cd > 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var i = new td_introducer();
                                i.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                i.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                i.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                i.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);

                                i.srl_no = UtilityM.CheckNull<Int16>(reader["SRL_NO"]);
                                i.introducer_name = UtilityM.CheckNull<string>(reader["INTRODUCER_NAME"]);
                                i.introducer_acc_type = UtilityM.CheckNull<int>(reader["INTRODUCER_ACC_TYPE"]);
                                i.introducer_acc_num = UtilityM.CheckNull<string>(reader["INTRODUCER_ACC_NUM"]);
                                i.del_flag= UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                indList.Add(i);
                            }
                        }
                    }
                }
            }

            return indList;
        }



        internal List<td_nominee> GetNomineeTemp(DbConnection connection, tm_deposit dep)
        {
            List<td_nominee> nomList = new List<td_nominee>();

            string _query = "SELECT ARDB_CD, BRN_CD, "
             + " ACC_TYPE_CD, "
             + " ACC_NUM,     "
             + " NOM_ID,      "
             + " NOM_NAME,    "
             + " NOM_ADDR1,   "
             + " NOM_ADDR2,   "
             + " PHONE_NO,    "
             + " PERCENTAGE,  "
             + " RELATION ,DEL_FLAG    "
             + " FROM TD_NOMINEE_TEMP "
             + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND ACC_TYPE_CD = {2} AND ACC_NUM = {3} AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
            dep.acc_type_cd != 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD",
            !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num");
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var n = new td_nominee();
                                n.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                n.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                n.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                n.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                n.nom_id = UtilityM.CheckNull<Int16>(reader["NOM_ID"]);

                                n.nom_name = UtilityM.CheckNull<string>(reader["NOM_NAME"]);
                                n.nom_addr1 = UtilityM.CheckNull<string>(reader["NOM_ADDR1"]);
                                n.nom_addr2 = UtilityM.CheckNull<string>(reader["NOM_ADDR2"]);
                                n.phone_no = UtilityM.CheckNull<string>(reader["PHONE_NO"]);
                                n.percentage = UtilityM.CheckNull<Single>(reader["PERCENTAGE"]);
                                n.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                n.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                nomList.Add(n);
                            }
                        }
                    }
                }
            }
            return nomList;
        }



        internal List<td_signatory> GetSignatoryTemp(DbConnection connection, tm_deposit sig)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "SELECT ARDB_CD, BRN_CD,"
             + " ACC_TYPE_CD,"
             + " ACC_NUM,"
             + " SIGNATORY_NAME,DEL_FLAG "
             + " FROM TD_SIGNATORY_TEMP"
             + " WHERE ARDB_CD={0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = {3} AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(sig.ardb_cd) ? string.Concat("'", sig.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(sig.brn_cd) ? string.Concat("'", sig.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(sig.acc_num) ? string.Concat("'", sig.acc_num, "'") : "acc_num",
                                           sig.acc_type_cd != 0 ? Convert.ToString(sig.acc_type_cd) : "ACC_TYPE_CD"
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
                                var s = new td_signatory();
                                s.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                s.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                s.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                s.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                s.signatory_name = UtilityM.CheckNull<string>(reader["SIGNATORY_NAME"]);
                                s.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                sigList.Add(s);
                            }
                        }
                    }
                }
            }
            return sigList;
        }



        internal List<td_accholder> GetAccholderTemp(DbConnection connection, tm_deposit acc)
        {
            List<td_accholder> accList = new List<td_accholder>();

            dynamic _query = " SELECT ARDB_CD, BRN_CD, "
                 + " ACC_TYPE_CD,   "
                 + " ACC_NUM,       "
                 + " ACC_HOLDER,    "
                 + " RELATION,      "
                 + " CUST_CD, DEL_FLAG        "
                 + " FROM TD_ACCHOLDER_TEMP "
                 + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = {3} AND DEL_FLAG='N'  ";
            var v1 = !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd";
            var v2 = !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd";
            var v3 = !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num";
            dynamic v4 = (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD";
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
                                a.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                accList.Add(a);
                            }
                        }
                    }
                }
            }
            return accList;
        }



        internal List<tm_denomination_trans> GetDenominationDtls(DbConnection connection, tm_deposit tdt)
        {
            List<tm_denomination_trans> tdtRets = new List<tm_denomination_trans>();
            string _query = "SELECT TM_DENOMINATION_TRANS.BRN_CD,TM_DENOMINATION_TRANS.TRANS_DT,TM_DENOMINATION_TRANS.TRANS_CD,TM_DENOMINATION_TRANS.RUPEES,"
                          + " TM_DENOMINATION_TRANS.COUNT,TM_DENOMINATION_TRANS.CREATED_DT,TM_DENOMINATION_TRANS.CREATED_BY,TM_DENOMINATION_TRANS.TOTAL"
                          + " FROM TM_DENOMINATION_TRANS WHERE  TM_DENOMINATION_TRANS.BRN_CD= {0}  AND TM_DENOMINATION_TRANS.TRANS_DT =  to_date('{1}','dd-mm-yyyy' ) AND"
                          + " TM_DENOMINATION_TRANS.TRANS_CD = {2}";
            _statement = string.Format(_query,
                                        string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                        tdt.trans_dt != null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "trans_dt",
                                        tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
                                        );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new tm_denomination_trans();
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                            tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                            tdtr.rupees = UtilityM.CheckNull<Double>(reader["RUPEES"]);
                            tdtr.count = UtilityM.CheckNull<Int64>(reader["COUNT"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.total = UtilityM.CheckNull<Double>(reader["TOTAL"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }
            return tdtRets;
        } 


        internal List<tm_transfer> GetTransfer(DbConnection connection, tm_deposit tdt)
        {
            List<tm_transfer> tdtRets = new List<tm_transfer>();
            string _query = "SELECT  TRF_DT,"
         + " TRF_CD,"
         + " TRANS_CD,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " BRN_CD,ARDB_CD,DEL_FLAG "
         + " FROM TM_TRANSFER"
         + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
         + " (TRF_DT = to_date('{2}','dd-mm-yyyy' )) AND  "
         + " (  TRANS_CD = {3} ) AND (DEL_FLAG = 'N') ";
            _statement = string.Format(_query,
                                         string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                         string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                         tdt.trans_dt != null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "TRF_DT",
                                         tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "TRANS_CD"
                                         );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new tm_transfer();
                            tdtr.trf_dt = UtilityM.CheckNull<DateTime>(reader["TRF_DT"]);
                            tdtr.trf_cd = UtilityM.CheckNull<Int32>(reader["TRF_CD"]);
                            tdtr.trans_cd = UtilityM.CheckNull<decimal>(reader["TRANS_CD"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag= UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }

            return tdtRets;
        }    



        internal List<td_def_trans_trf> GetDepTransTrf(DbConnection connection, tm_deposit tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " INSTRUMENT_DT,"
         + " INSTRUMENT_NUM,"
         + " PAID_TO,"
         + " TOKEN_NUM,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " MODIFIED_BY,"
         + " MODIFIED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " PARTICULARS,"
         + " TR_ACC_TYPE_CD,"
         + " TR_ACC_NUM,"
         + " VOUCHER_DT,"
         + " VOUCHER_ID,"
         + " TRF_TYPE,"
         + " TR_ACC_CD,"
         + " ACC_CD,"
         + " SHARE_AMT,"
         + " SUM_ASSURED,"
         + " PAID_AMT,"
         + " CURR_PRN_RECOV,"
         + " OVD_PRN_RECOV,"
         + " CURR_INTT_RECOV,"
         + " OVD_INTT_RECOV,"
         + " REMARKS,"
         + " CROP_CD,"
         + " ACTIVITY_CD,"
         + " CURR_INTT_RATE,"
         + " OVD_INTT_RATE,"
         + " INSTL_NO,"
         + " INSTL_START_DT,"
         + " PERIODICITY,"
         + " DISB_ID,"
         + " COMP_UNIT_NO,"
         + " ONGOING_UNIT_NO,"
         + " MIS_ADVANCE_RECOV,"
         + " AUDIT_FEES_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD, ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV "
          + " FROM TD_DEP_TRANS_TRF"
           + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
            + " (TRANS_DT = to_date('{2}','dd-mm-yyyy' )) AND  "
          + " (  TRANS_CD = {3} ) AND (DEL_FLAG='N')  ";
            _statement = string.Format(_query,
                                        string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                        string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                        tdt.trans_dt != null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "trans_dt",
                                        tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
                                        );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new td_def_trans_trf();
                            tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                            tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                            tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                            tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                            tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                            tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                            tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                            tdtr.instrument_dt = UtilityM.CheckNull<DateTime>(reader["INSTRUMENT_DT"]);
                            tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                            tdtr.paid_to = UtilityM.CheckNull<string>(reader["PAID_TO"]);
                            tdtr.token_num = UtilityM.CheckNull<string>(reader["TOKEN_NUM"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                            tdtr.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                            tdtr.tr_acc_type_cd = UtilityM.CheckNull<Int32>(reader["TR_ACC_TYPE_CD"]);
                            tdtr.tr_acc_num = UtilityM.CheckNull<string>(reader["TR_ACC_NUM"]);
                            tdtr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                            tdtr.voucher_id = UtilityM.CheckNull<decimal>(reader["VOUCHER_ID"]);
                            tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                            tdtr.tr_acc_cd = UtilityM.CheckNull<int>(reader["TR_ACC_CD"]);
                            tdtr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                            tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["SHARE_AMT"]);
                            tdtr.sum_assured = UtilityM.CheckNull<decimal>(reader["SUM_ASSURED"]);
                            tdtr.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                            tdtr.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                            tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                            tdtr.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                            tdtr.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                            tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                            tdtr.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                            tdtr.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                            tdtr.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                            tdtr.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                            tdtr.instl_no = UtilityM.CheckNull<Int32>(reader["INSTL_NO"]);
                            tdtr.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                            tdtr.periodicity = UtilityM.CheckNull<Int16>(reader["PERIODICITY"]);
                            tdtr.disb_id = UtilityM.CheckNull<decimal>(reader["DISB_ID"]);
                            tdtr.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                            tdtr.ongoing_unit_no = UtilityM.CheckNull<decimal>(reader["ONGOING_UNIT_NO"]);
                            tdtr.mis_advance_recov = UtilityM.CheckNull<decimal>(reader["MIS_ADVANCE_RECOV"]);
                            tdtr.audit_fees_recov = UtilityM.CheckNull<decimal>(reader["AUDIT_FEES_RECOV"]);
                            tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                            tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                            tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                            tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                            tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                            tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }
            return tdtRets;
        }   


       internal td_def_trans_trf GetDepTrans(DbConnection connection,tm_deposit tdt)
        {
            td_def_trans_trf tdtRets = new td_def_trans_trf();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " INSTRUMENT_DT,"
         + " INSTRUMENT_NUM,"
         + " PAID_TO,"
         + " TOKEN_NUM,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " MODIFIED_BY,"
         + " MODIFIED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " PARTICULARS,"
         + " TR_ACC_TYPE_CD,"
         + " TR_ACC_NUM,"
         + " VOUCHER_DT,"
         + " VOUCHER_ID,"
         + " TRF_TYPE,"
         + " TR_ACC_CD,"
         + " ACC_CD,"
         + " SHARE_AMT,"
         + " SUM_ASSURED,"
         + " PAID_AMT,"
         + " CURR_PRN_RECOV,"
         + " OVD_PRN_RECOV,"
         + " CURR_INTT_RECOV,"
         + " OVD_INTT_RECOV,"
         + " REMARKS,"
         + " CROP_CD,"
         + " ACTIVITY_CD,"
         + " CURR_INTT_RATE,"
         + " OVD_INTT_RATE,"
         + " INSTL_NO,"
         + " INSTL_START_DT,"
         + " PERIODICITY,"
         + " DISB_ID,"
         + " COMP_UNIT_NO,"
         + " ONGOING_UNIT_NO,"
         + " MIS_ADVANCE_RECOV,"
         + " AUDIT_FEES_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD,ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV "
         + " FROM TD_DEP_TRANS"
         + " WHERE  ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = {3} AND  NVL(APPROVAL_STATUS, 'U') = 'U' AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                                      !string.IsNullOrWhiteSpace(tdt.ardb_cd) ? string.Concat("'", tdt.ardb_cd, "'") : "ardb_cd",
                                      !string.IsNullOrWhiteSpace(tdt.brn_cd) ? string.Concat("'", tdt.brn_cd, "'") : "brn_cd",
                                      !string.IsNullOrWhiteSpace(tdt.acc_num) ? string.Concat("'", tdt.acc_num, "'") : "acc_num",
                                      tdt.acc_type_cd != 0 ? Convert.ToString(tdt.acc_type_cd) : "ACC_TYPE_CD"
                                      );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new td_def_trans_trf();
                            tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                            tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                            tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                            tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                            tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                            tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                            tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                            tdtr.instrument_dt = UtilityM.CheckNull<DateTime>(reader["INSTRUMENT_DT"]);
                            tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                            tdtr.paid_to = UtilityM.CheckNull<string>(reader["PAID_TO"]);
                            tdtr.token_num = UtilityM.CheckNull<string>(reader["TOKEN_NUM"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                            tdtr.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                            tdtr.tr_acc_type_cd = UtilityM.CheckNull<Int32>(reader["TR_ACC_TYPE_CD"]);
                            tdtr.tr_acc_num = UtilityM.CheckNull<string>(reader["TR_ACC_NUM"]);
                            tdtr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                            tdtr.voucher_id = UtilityM.CheckNull<decimal>(reader["VOUCHER_ID"]);
                            tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                            tdtr.tr_acc_cd = UtilityM.CheckNull<int>(reader["TR_ACC_CD"]);
                            tdtr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                            tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["SHARE_AMT"]);
                            tdtr.sum_assured = UtilityM.CheckNull<decimal>(reader["SUM_ASSURED"]);
                            tdtr.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                            tdtr.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                            tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                            tdtr.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                            tdtr.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                            tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                            tdtr.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                            tdtr.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                            tdtr.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                            tdtr.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                            tdtr.instl_no = UtilityM.CheckNull<Int32>(reader["INSTL_NO"]);
                            tdtr.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                            tdtr.periodicity = UtilityM.CheckNull<Int16>(reader["PERIODICITY"]);
                            tdtr.disb_id = UtilityM.CheckNull<decimal>(reader["DISB_ID"]);
                            tdtr.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                            tdtr.ongoing_unit_no = UtilityM.CheckNull<decimal>(reader["ONGOING_UNIT_NO"]);
                            tdtr.mis_advance_recov = UtilityM.CheckNull<decimal>(reader["MIS_ADVANCE_RECOV"]);
                            tdtr.audit_fees_recov = UtilityM.CheckNull<decimal>(reader["AUDIT_FEES_RECOV"]);
                            tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                            tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                            tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                            tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                            tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                            tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                            tdtRets = tdtr;
                        }
                    }
                }

            }
            return tdtRets;
        }



        internal int GetTransCDMaxId(DbConnection connection, td_def_trans_trf tvd)
        {
            int maxTransCD = 0;
            string _query = "Select Nvl(max(trans_cd) + 1, 1) max_trans_cd"
                            + " From   td_dep_trans"
                            + " Where ardb_cd = {0} and  trans_dt =  {1} "
                            + " And    brn_cd = {2} ";
            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tvd.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tvd.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            string.IsNullOrWhiteSpace(tvd.brn_cd) ? "brn_cd" : string.Concat("'", tvd.brn_cd, "'")
                                            );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            maxTransCD = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["MAX_TRANS_CD"]));
                        }
                    }
                }
            }
            return maxTransCD;
        }




        internal int GetTrfCDMaxId(DbConnection connection, tm_transfer tvd)
        {
            int maxTransCD = 0;
            string _query = "Select Nvl(max(trf_cd) + 1, 1) max_trf_cd"
                            + " From   tm_transfer"
                            + " Where ARDB_CD = {0} AND  trf_dt =  {1} "
                            + "  And    brn_cd = {2} ";
            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tvd.trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tvd.trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            string.IsNullOrWhiteSpace(tvd.brn_cd) ? "brn_cd" : string.Concat("'", tvd.brn_cd, "'")
                                            );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            maxTransCD = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["max_trf_cd"]));
                        }
                    }
                }
            }
            return maxTransCD;
        }



        internal bool InsertDepositTemp(DbConnection connection, tm_deposit dep)
        {
            string _query = " INSERT INTO TM_DEPOSIT_TEMP ( BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                           + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                           + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                           + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                           + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                           + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD,ARDB_CD,DEL_FLAG )  "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                           + " {15},{16}, {17}, {18},{19},{20},{21},{22},{23},{24}, "
                           + " {25},{26},{27},{28},{29}, SYSDATE,{30},SYSDATE,{31}, "
                           + " {32},{33},{34}, {35},{36},{37},{38},{39},{40},{41},{42},{43},{44},'N')";

            _statement = string.Format(_query,            
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.renew_id, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.intt_trf_type, "'"),
            string.Concat("'", dep.constitution_cd, "'"),
            string.Concat("'", dep.oprn_instr_cd, "'"),
            string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.prn_amt, "'"),
            string.Concat("'", dep.intt_amt, "'"),
            string.Concat("'", dep.dep_period, "'"),
            string.Concat("'", dep.instl_amt, "'"),
            string.Concat("'", dep.instl_no, "'"),
            string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.intt_rt, "'"),
            string.Concat("'", dep.tds_applicable, "'"),
            string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.closing_prn_amt, "'"),
            string.Concat("'", dep.closing_intt_amt, "'"),
            string.Concat("'", dep.penal_amt, "'"),
            string.Concat("'", dep.ext_instl_tot, "'"),
            string.Concat("'", dep.mat_status, "'"),
            string.Concat("'", dep.acc_status, "'"),
            string.Concat("'", dep.curr_bal, "'"),
            string.Concat("'", dep.clr_bal, "'"),
            string.Concat("'", dep.standing_instr_flag, "'"),
            string.Concat("'", dep.cheque_facility_flag, "'"),
            string.Concat("'", dep.created_by, "'"),
            //string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.modified_by, "'"),
            //string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.approval_status, "'"),
            string.Concat("'", dep.approved_by, "'"),
            string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.user_acc_num, "'"),
            string.Concat("'", dep.lock_mode, "'"),
            string.Concat("'", dep.loan_id, "'"),
            string.Concat("'", dep.cert_no, "'"),
            string.Concat("'", dep.bonus_amt, "'"),
            string.Concat("'", dep.penal_intt_rt, "'"),
            string.Concat("'", dep.bonus_intt_rt, "'"),
            string.Concat("'", dep.transfer_flag, "'"),
            string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.agent_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'")
                                         );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }

        internal bool InsertDepositRenewTemp(DbConnection connection, tm_deposit dep, int transcd)
        {
            string _query = " INSERT INTO TM_DEPOSIT_RENEW_TEMP (BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                        + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                        + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                        + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                        + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                        + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, CATG_CD,ARDB_CD,DEL_FLAG )  "
                        + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                        + " {15},{16}, {17}, {18},{19},{20},{21},{22},{23},{24}, "
                        + " {25},{26},{27},{28},{29}, SYSDATE,{30},SYSDATE,{31}, "
                        + " {32},{33},{34}, {35},{36},{37},{38},{39},{40},{41},{42},{43},{44},'N')";

            _statement = string.Format(_query,
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.renew_id, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.intt_trf_type, "'"),
            string.Concat("'", dep.constitution_cd, "'"),
            string.Concat("'", dep.oprn_instr_cd, "'"),
            string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.prn_amt, "'"),
            string.Concat("'", dep.intt_amt, "'"),
            string.Concat("'", dep.dep_period, "'"),
            string.Concat("'", dep.instl_amt, "'"),
            string.Concat("'", dep.instl_no, "'"),
            string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.intt_rt, "'"),
            string.Concat("'", dep.tds_applicable, "'"),
            string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.closing_prn_amt, "'"),
            string.Concat("'", dep.closing_intt_amt, "'"),
            string.Concat("'", dep.penal_amt, "'"),
            string.Concat("'", dep.ext_instl_tot, "'"),
            string.Concat("'", dep.mat_status, "'"),
            string.Concat("'", dep.acc_status, "'"),
            string.Concat("'", dep.curr_bal, "'"),
            string.Concat("'", dep.clr_bal, "'"),
            string.Concat("'", dep.standing_instr_flag, "'"),
            string.Concat("'", dep.cheque_facility_flag, "'"),
            string.Concat("'", dep.created_by, "'"),
            //string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.modified_by, "'"),
            //string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.approval_status, "'"),
            string.Concat("'", dep.approved_by, "'"),
            string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            //string.Concat("'", dep.user_acc_num, "'"),
            string.Concat(transcd),
            string.Concat("'", dep.lock_mode, "'"),
            string.Concat("'", dep.loan_id, "'"),
            string.Concat("'", dep.cert_no, "'"),
            string.Concat("'", dep.bonus_amt, "'"),
            string.Concat("'", dep.penal_intt_rt, "'"),
            string.Concat("'", dep.bonus_intt_rt, "'"),
            string.Concat("'", dep.transfer_flag, "'"),
            string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.agent_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'")
                                         );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }

        internal bool InsertIntroducerTemp(DbConnection connection, List<td_introducer> ind)
        {
            string _query = "INSERT INTO TD_INTRODUCER_TEMP ( ardb_cd,brn_cd, acc_type_cd, acc_num, srl_no, introducer_name, introducer_acc_type, introducer_acc_num,del_flag) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5} , {6}, {7}, 'N' ) ";
            for (int i = 0; i < ind.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", ind[i].ardb_cd, "'"),
                                                       string.Concat("'", ind[i].brn_cd, "'"),
                                                       ind[i].acc_type_cd,
                                                       string.Concat("'", ind[i].acc_num, "'"),
                                                       ind[i].srl_no,
                                                       string.Concat("'", ind[i].introducer_name, "'"),
                                                       string.Concat("'", ind[i].introducer_acc_type, "'"),
                                                       string.Concat("'", ind[i].introducer_acc_num, "'")
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            return true;
        }



        internal bool InsertNomineeTemp(DbConnection connection, List<td_nominee> nom)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "INSERT INTO TD_NOMINEE_TEMP (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                          + " VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                _statement = string.Format(_query,
                                                  string.Concat("'", nom[i].ardb_cd, "'"),
                                                  string.Concat("'", nom[i].brn_cd, "'"),
                                                  nom[i].acc_type_cd,
                                                  string.Concat("'", nom[i].acc_num, "'"),
                                                  nom[i].nom_id,
                                                  string.Concat("'", nom[i].nom_name, "'"),
                                                  string.Concat("'", nom[i].nom_addr1, "'"),
                                                  string.Concat("'", nom[i].nom_addr2, "'"),
                                                  string.Concat("'", nom[i].phone_no, "'"),
                                                  nom[i].percentage,
                                                  string.Concat("'", nom[i].relation, "'")
                                                   );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }



        internal bool InsertSignatoryTemp(DbConnection connection, List<td_signatory> sig)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "INSERT INTO TD_SIGNATORY_TEMP ( ardb_cd, brn_cd, acc_type_cd, acc_num, signatory_name,del_flag) "
                          + " VALUES( {0},{1},{2},{3},{4},'N') ";
            for (int i = 0; i < sig.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", sig[i].ardb_cd, "'"),
                                                       string.Concat("'", sig[i].brn_cd, "'"),
                                                       sig[i].acc_type_cd,
                                                       string.Concat("'", sig[i].acc_num, "'"),
                                                       string.Concat("'", sig[i].signatory_name, "'")
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }



        internal bool InsertAccholderTemp(DbConnection connection, List<td_accholder> acc)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "INSERT INTO TD_ACCHOLDER_TEMP ( ardb_cd, brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,del_flag ) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N') ";

            for (int i = 0; i < acc.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", acc[i].ardb_cd, "'"),
                                                       string.Concat("'", acc[i].brn_cd, "'"),
                                                       acc[i].acc_type_cd,
                                                       string.Concat("'", acc[i].acc_num, "'"),
                                                       string.Concat("'", acc[i].acc_holder, "'"),
                                                       string.Concat("'", acc[i].relation, "'"),
                                                       acc[i].cust_cd
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                    
                }
            }

            return true;
        }



        internal bool InsertDenominationDtls(DbConnection connection, List<tm_denomination_trans> tdt, int transcd)
        {
            List<tm_denomination_trans> tdtRets = new List<tm_denomination_trans>();
            string _query = "INSERT INTO TM_DENOMINATION_TRANS (BRN_CD, TRANS_DT, TRANS_CD, RUPEES, COUNT, TOTAL, CREATED_DT, CREATED_BY)"
                            + " VALUES ({0}, {1}, {2}, {3}, {4}, {5}, SYSDATE, {6})";

            for (int i = 0; i < tdt.Count; i++)
            {
                _statement = string.Format(_query,
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(transcd),
                             string.Concat(tdt[i].rupees),
                             string.Concat(tdt[i].count),
                             string.Concat(tdt[i].total),
                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].created_by, "'")
                             );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }


        internal bool InsertTransfer(DbConnection connection, List<tm_transfer> tdt, int transcd)
        {


            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TM_TRANSFER (TRF_DT,TRF_CD,TRANS_CD,CREATED_BY,"
                        + " CREATED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG)"
                        + " VALUES ({0},{1},{2},{3},"
                        + " {4},{5},{6},{7},"
                        + " {8},{9},'N')";

            for (int i = 0; i < tdt.Count; i++)
            {
                //int maxTrfCD = GetTrfCDMaxId(connection, tdt[i]);
                _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(transcd),
                             string.Concat(transcd),
                             string.Concat("'", tdt[i].created_by, "'"),
                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("sysdate"),
                             string.Concat("'", tdt[i].approval_status, "'"),
                             string.Concat("'", tdt[i].approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.Concat("'", tdt[i].ardb_cd, "'")
                             );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }


        internal bool InsertDepTransTrf(DbConnection connection, List<td_def_trans_trf> tdt, int transcd)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TD_DEP_TRANS_TRF (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                        + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                        + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG,HOME_BRN_CD,INTRA_BRANCH_TRN,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                        + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                        + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                        + " {38},{39},{40},{41},{42},{43},{44}, {45},{46},{47},{48},{49},"
                        + " {50},{51},'N',{52},{53},{54},{55},{56})";


            for (int i = 0; i < tdt.Count; i++)
            {



                _statement = string.Format(_query,
                                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat(transcd),
                                             string.Concat("'", tdt[i].acc_type_cd, "'"),
                                             string.Concat("'", tdt[i].acc_num, "'"),
                                             string.Concat("'", tdt[i].trans_type, "'"),
                                             string.Concat("'", tdt[i].trans_mode, "'"),
                                             string.Concat("'", tdt[i].amount, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].instrument_num, "'"),
                                             string.Concat("'", tdt[i].paid_to, "'"),
                                             string.Concat("'", tdt[i].token_num, "'"),
                                             string.Concat("'", tdt[i].created_by, "'"),
                                             string.Concat("sysdate"),
                                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].modified_by, "'"),
                                             string.Concat("sysdate"),
                                             //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].approval_status, "'"),
                                             string.Concat("'", tdt[i].approved_by, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].particulars, "'"),
                                             string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                             string.Concat("'", tdt[i].tr_acc_num, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].voucher_id, "'"),
                                             string.Concat("'", tdt[i].trf_type, "'"),
                                             string.Concat("'", tdt[i].tr_acc_cd, "'"),
                                             string.Concat("'", tdt[i].acc_cd, "'"),
                                             string.Concat("'", tdt[i].share_amt, "'"),
                                             string.Concat("'", tdt[i].sum_assured, "'"),
                                             string.Concat("'", tdt[i].paid_amt, "'"),
                                             string.Concat("'", tdt[i].curr_prn_recov, "'"),
                                             string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                                             string.Concat("'", tdt[i].curr_intt_recov, "'"),
                                             string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                                             string.Concat("'", tdt[i].remarks, "'"),
                                             string.Concat("'", tdt[i].crop_cd, "'"),
                                             string.Concat("'", tdt[i].activity_cd, "'"),
                                             string.Concat("'", tdt[i].curr_intt_rate, "'"),
                                             string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                                             string.Concat("'", tdt[i].instl_no, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].periodicity, "'"),
                                             string.Concat("'", tdt[i].disb_id, "'"),
                                             string.Concat("'", tdt[i].comp_unit_no, "'"),
                                             string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                             string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                             string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                             string.Concat("'", tdt[i].sector_cd, "'"),
                                             string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                             string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].brn_cd, "'"),
                                             string.Concat("'", tdt[i].ardb_cd, "'"),
                                             string.Concat("'", tdt[i].home_brn_cd, "'"),
                                             string.Concat("'", tdt[i].intra_branch_trn, "'"),
                                             string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                             string.Concat("'", tdt[i].adv_prn_recov, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }




        internal bool InsertDepTrans(DbConnection connection, td_def_trans_trf tdt, int transcd)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TD_DEP_TRANS (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                        + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                        + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG,HOME_BRN_CD,INTRA_BRANCH_TRN,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                        + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                        + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                        + " {38},{39},{40},{41},{42},{43},{44}, {45},{46},{47},{48},{49},"
                        + " {50},{51},'N',{52},{53},{54},{55},{56})";

            _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", transcd, "'"),
                             string.Concat("'", tdt.acc_type_cd, "'"),
                             string.Concat("'", tdt.acc_num, "'"),
                             string.Concat("'", tdt.trans_type, "'"),
                             string.Concat("'", tdt.trans_mode, "'"),
                             string.Concat("'", tdt.amount, "'"),
                             string.IsNullOrWhiteSpace(tdt.instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.instrument_num, "'"),
                             string.Concat("'", tdt.paid_to, "'"),
                             string.Concat("'", tdt.token_num, "'"),
                             string.Concat("'", tdt.created_by, "'"),
                             string.Concat("sysdate"),
                             // string.IsNullOrWhiteSpace(tdt.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.modified_by, "'"),
                             string.Concat("sysdate"),
                             //string.IsNullOrWhiteSpace(tdt.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.approval_status, "'"),
                             string.Concat("'", tdt.approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.particulars, "'"),
                             string.Concat("'", tdt.tr_acc_type_cd, "'"),
                             string.Concat("'", tdt.tr_acc_num, "'"),
                             string.IsNullOrWhiteSpace(tdt.voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.voucher_id, "'"),
                             string.Concat("'", tdt.trf_type, "'"),
                             string.Concat("'", tdt.tr_acc_cd, "'"),
                             string.Concat("'", tdt.acc_cd, "'"),
                             string.Concat("'", tdt.share_amt, "'"),
                             string.Concat("'", tdt.sum_assured, "'"),
                             string.Concat("'", tdt.paid_amt, "'"),
                             string.Concat("'", tdt.curr_prn_recov, "'"),
                             string.Concat("'", tdt.ovd_prn_recov, "'"),
                             string.Concat("'", tdt.curr_intt_recov, "'"),
                             string.Concat("'", tdt.ovd_intt_recov, "'"),
                             string.Concat("'", tdt.remarks, "'"),
                             string.Concat("'", tdt.crop_cd, "'"),
                             string.Concat("'", tdt.activity_cd, "'"),
                             string.Concat("'", tdt.curr_intt_rate, "'"),
                             string.Concat("'", tdt.ovd_intt_rate, "'"),
                             string.Concat("'", tdt.instl_no, "'"),
                             string.IsNullOrWhiteSpace(tdt.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.periodicity, "'"),
                             string.Concat("'", tdt.disb_id, "'"),
                             string.Concat("'", tdt.comp_unit_no, "'"),
                             string.Concat("'", tdt.ongoing_unit_no, "'"),
                             string.Concat("'", tdt.mis_advance_recov, "'"),
                             string.Concat("'", tdt.audit_fees_recov, "'"),
                             string.Concat("'", tdt.sector_cd, "'"),
                             string.Concat("'", tdt.spl_prog_cd, "'"),
                             string.Concat("'", tdt.borrower_cr_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.home_brn_cd, "'"),
                             string.Concat("'", tdt.intra_branch_trn, "'"),
                             string.Concat("'", tdt.penal_intt_recov, "'"),
                             string.Concat("'", tdt.adv_prn_recov, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                             );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }


        internal bool UpdateDepositTemp(DbConnection connection, tm_deposit dep)
        {
            string _query = " UPDATE TM_DEPOSIT_TEMP SET "
                  + "brn_cd               = NVL({0},  brn_cd              ),"
                  + "acc_type_cd          = NVL({1},  acc_type_cd         ),"
                  + "acc_num              = NVL({2},  acc_num             ),"
                  + "renew_id             = NVL({3},  renew_id            ),"
                  + "cust_cd              = NVL({4},  cust_cd             ),"
                  + "intt_trf_type        = NVL({5},  intt_trf_type       ),"
                  + "constitution_cd      = NVL({6},  constitution_cd     ),"
                  + "oprn_instr_cd        = NVL({7},  oprn_instr_cd       ),"
                  + "opening_dt           = NVL({8},  opening_dt ),"
                  + "prn_amt              = NVL({9},  prn_amt             ),"
                  + "intt_amt             = NVL({10}, intt_amt            ),"
                  + "dep_period           = NVL({11}, dep_period          ),"
                  + "instl_amt            = NVL({12}, instl_amt           ),"
                  + "instl_no             = NVL({13}, instl_no            ),"
                  + "mat_dt               = NVL({14}, mat_dt ),"
                  + "intt_rt              = NVL({15}, intt_rt             ),"
                  + "tds_applicable       = NVL({16}, tds_applicable      ),"
                  + "last_intt_calc_dt    = NVL({17}, last_intt_calc_dt ),"
                  + "acc_close_dt         = NVL({18}, acc_close_dt ),"
                  + "closing_prn_amt      = NVL({19}, closing_prn_amt     ),"
                  + "closing_intt_amt     = NVL({20}, closing_intt_amt    ),"
                  + "penal_amt            = NVL({21}, penal_amt           ),"
                  + "ext_instl_tot        = NVL({22}, ext_instl_tot       ),"
                  + "mat_status           = NVL({23}, mat_status          ),"
                  + "acc_status           = NVL({24}, acc_status          ),"
                  + "curr_bal             = NVL({25}, curr_bal            ),"
                  + "clr_bal              = NVL({26}, clr_bal             ),"
                  + "standing_instr_flag  = NVL({27}, standing_instr_flag ),"
                  + "cheque_facility_flag = NVL({28}, cheque_facility_flag),"
                  + "created_by           = NVL({29}, created_by          ),"
                  + "created_dt           = NVL({30}, created_dt ),"
                  + "modified_by          = NVL({31}, modified_by         ),"
                  + "modified_dt          = NVL({32}, modified_dt ),"
                  + "approval_status      = NVL({33}, approval_status     ),"
                  + "approved_by          = NVL({34}, approved_by         ),"
                  + "approved_dt          = NVL({35}, approved_dt ),"
                  + "user_acc_num         = NVL({36}, user_acc_num        ),"
                  + "lock_mode            = NVL({37}, lock_mode           ),"
                  + "loan_id              = NVL({38}, loan_id             ),"
                  + "cert_no              = NVL({39}, cert_no             ),"
                  + "bonus_amt            = NVL({40}, bonus_amt           ),"
                  + "penal_intt_rt        = NVL({41}, penal_intt_rt       ),"
                  + "bonus_intt_rt        = NVL({42}, bonus_intt_rt       ),"
                  + "transfer_flag        = NVL({43}, transfer_flag       ),"
                  + "transfer_dt          = NVL({44}, transfer_dt ),"
                  + "agent_cd             = NVL({45}, agent_cd            ) "
                  + "WHERE ardb_cd={46} and brn_cd = NVL({47}, brn_cd) AND acc_num = NVL({48},  acc_num ) AND acc_type_cd=NVL({49},  acc_type_cd ) ";

            _statement = string.Format(_query,
                        string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.renew_id, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.intt_trf_type, "'"),
            string.Concat("'", dep.constitution_cd, "'"),
            string.Concat("'", dep.oprn_instr_cd, "'"),
            string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.prn_amt, "'"),
            string.Concat("'", dep.intt_amt, "'"),
            string.Concat("'", dep.dep_period, "'"),
            string.Concat("'", dep.instl_amt, "'"),
            string.Concat("'", dep.instl_no, "'"),
            string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.intt_rt, "'"),
            string.Concat("'", dep.tds_applicable, "'"),
            string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.closing_prn_amt, "'"),
            string.Concat("'", dep.closing_intt_amt, "'"),
            string.Concat("'", dep.penal_amt, "'"),
            string.Concat("'", dep.ext_instl_tot, "'"),
            string.Concat("'", dep.mat_status, "'"),
            string.Concat("'", dep.acc_status, "'"),
            string.Concat("'", dep.curr_bal, "'"),
            string.Concat("'", dep.clr_bal, "'"),
            string.Concat("'", dep.standing_instr_flag, "'"),
            string.Concat("'", dep.cheque_facility_flag, "'"),
            string.Concat("'", dep.created_by, "'"),
            string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.modified_by, "'"),
            string.Concat("sysdate"),
            //string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.approval_status, "'"),
            string.Concat("'", dep.approved_by, "'"),
            string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.user_acc_num, "'"),
            string.Concat("'", dep.lock_mode, "'"),
            string.Concat("'", dep.loan_id, "'"),
            string.Concat("'", dep.cert_no, "'"),
            string.Concat("'", dep.bonus_amt, "'"),
            string.Concat("'", dep.penal_intt_rt, "'"),
            string.Concat("'", dep.bonus_intt_rt, "'"),
            string.Concat("'", dep.transfer_flag, "'"),
            string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.agent_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.acc_type_cd, "'")
                        );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }

        internal bool UpdateDepositRenewTemp(DbConnection connection, tm_deposit dep)
        {
            string _query = " UPDATE TM_DEPOSIT_RENEW_TEMP SET "
                  + "brn_cd               = NVL({0},  brn_cd              ),"
                  + "acc_type_cd          = NVL({1},  acc_type_cd         ),"
                  + "acc_num              = NVL({2},  acc_num             ),"
                  + "renew_id             = NVL({3},  renew_id            ),"
                  + "cust_cd              = NVL({4},  cust_cd             ),"
                  + "intt_trf_type        = NVL({5},  intt_trf_type       ),"
                  + "constitution_cd      = NVL({6},  constitution_cd     ),"
                  + "oprn_instr_cd        = NVL({7},  oprn_instr_cd       ),"
                  + "opening_dt           = NVL({8},  opening_dt ),"
                  + "prn_amt              = NVL({9},  prn_amt             ),"
                  + "intt_amt             = NVL({10}, intt_amt            ),"
                  + "dep_period           = NVL({11}, dep_period          ),"
                  + "instl_amt            = NVL({12}, instl_amt           ),"
                  + "instl_no             = NVL({13}, instl_no            ),"
                  + "mat_dt               = NVL({14}, mat_dt ),"
                  + "intt_rt              = NVL({15}, intt_rt             ),"
                  + "tds_applicable       = NVL({16}, tds_applicable      ),"
                  + "last_intt_calc_dt    = NVL({17}, last_intt_calc_dt ),"
                  + "acc_close_dt         = NVL({18}, acc_close_dt ),"
                  + "closing_prn_amt      = NVL({19}, closing_prn_amt     ),"
                  + "closing_intt_amt     = NVL({20}, closing_intt_amt    ),"
                  + "penal_amt            = NVL({21}, penal_amt           ),"
                  + "ext_instl_tot        = NVL({22}, ext_instl_tot       ),"
                  + "mat_status           = NVL({23}, mat_status          ),"
                  + "acc_status           = NVL({24}, acc_status          ),"
                  + "curr_bal             = NVL({25}, curr_bal            ),"
                  + "clr_bal              = NVL({26}, clr_bal             ),"
                  + "standing_instr_flag  = NVL({27}, standing_instr_flag ),"
                  + "cheque_facility_flag = NVL({28}, cheque_facility_flag),"
                  + "created_by           = NVL({29}, created_by          ),"
                  + "created_dt           = NVL({30}, created_dt ),"
                  + "modified_by          = NVL({31}, modified_by         ),"
                  + "modified_dt          = NVL({32}, modified_dt ),"
                  + "approval_status      = NVL({33}, approval_status     ),"
                  + "approved_by          = NVL({34}, approved_by         ),"
                  + "approved_dt          = NVL({35}, approved_dt ),"
                  + "user_acc_num         = {36},"
                  + "lock_mode            = NVL({37}, lock_mode           ),"
                  + "loan_id              = NVL({38}, loan_id             ),"
                  + "cert_no              = NVL({39}, cert_no             ),"
                  + "bonus_amt            = NVL({40}, bonus_amt           ),"
                  + "penal_intt_rt        = NVL({41}, penal_intt_rt       ),"
                  + "bonus_intt_rt        = NVL({42}, bonus_intt_rt       ),"
                  + "transfer_flag        = NVL({43}, transfer_flag       ),"
                  + "transfer_dt          = NVL({44}, transfer_dt ),"
                  + "CATG_CD             = NVL({45}, CATG_CD            ) "
                  + "WHERE ardb_cd = {46} And brn_cd = NVL({47}, brn_cd) AND acc_num = NVL({48},  acc_num ) AND acc_type_cd=NVL({49},  acc_type_cd ) And Del_FLAG='N' And user_acc_num={50} ";

            _statement = string.Format(_query,
                        string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.renew_id, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.intt_trf_type, "'"),
            string.Concat("'", dep.constitution_cd, "'"),
            string.Concat("'", dep.oprn_instr_cd, "'"),
            string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.prn_amt, "'"),
            string.Concat("'", dep.intt_amt, "'"),
            string.Concat("'", dep.dep_period, "'"),
            string.Concat("'", dep.instl_amt, "'"),
            string.Concat("'", dep.instl_no, "'"),
            string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.intt_rt, "'"),
            string.Concat("'", dep.tds_applicable, "'"),
            string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.closing_prn_amt, "'"),
            string.Concat("'", dep.closing_intt_amt, "'"),
            string.Concat("'", dep.penal_amt, "'"),
            string.Concat("'", dep.ext_instl_tot, "'"),
            string.Concat("'", dep.mat_status, "'"),
            string.Concat("'", dep.acc_status, "'"),
            string.Concat("'", dep.curr_bal, "'"),
            string.Concat("'", dep.clr_bal, "'"),
            string.Concat("'", dep.standing_instr_flag, "'"),
            string.Concat("'", dep.cheque_facility_flag, "'"),
            string.Concat("'", dep.created_by, "'"),
            string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.modified_by, "'"),
            string.Concat("sysdate"),
            //string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.approval_status, "'"),
            string.Concat("'", dep.approved_by, "'"),
            string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.trans_cd, "'"),
            string.Concat("'", dep.lock_mode, "'"),
            string.Concat("'", dep.loan_id, "'"),
            string.Concat("'", dep.cert_no, "'"),
            string.Concat("'", dep.bonus_amt, "'"),
            string.Concat("'", dep.penal_intt_rt, "'"),
            string.Concat("'", dep.bonus_intt_rt, "'"),
            string.Concat("'", dep.transfer_flag, "'"),
            string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.agent_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.trans_cd, "'")
                        );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }

        internal bool UpdateIntroducerTemp(DbConnection connection, List<td_introducer> ind)
        {
            string _queryd = " DELETE FROM TD_INTRODUCER_TEMP "
             + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = {3} AND DEL_FLAG='N' ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(ind[0].ardb_cd) ? string.Concat("'", ind[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(ind[0].brn_cd) ? string.Concat("'", ind[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(ind[0].acc_num) ? string.Concat("'", ind[0].acc_num, "'") : "acc_num",
                                     (ind[0].acc_type_cd > 0) ? ind[0].acc_type_cd.ToString() : "ACC_TYPE_CD"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            string _query = " UPDATE TD_INTRODUCER_TEMP "
            + " SET brn_cd          = {0} , "
            + " acc_type_cd         = {1} , "
            + " acc_num             = {2} , "
            + " srl_no              = {3} , "
            + " introducer_name     = {4} , "
            + " introducer_acc_type = {5} , "
            + " introducer_acc_num  = {6}   "
           + "  WHERE ardb_cd={7} and brn_cd = {8} AND acc_num = {9} AND acc_type_cd=NVL({10},  acc_type_cd ) and DEL_FLAG='N'  ";
            string _queryins = "INSERT INTO TD_INTRODUCER_TEMP ( ardb_cd,brn_cd, acc_type_cd, acc_num, srl_no, introducer_name, introducer_acc_type, introducer_acc_num,DEL_FLAG) "
                          + " VALUES( {0},{1},{2},{3}, {4}, {5} , {6},{7},'N' ) ";

            for (int i = 0; i < ind.Count; i++)
            {
                ind[i].upd_ins_flag = "I";
                if (ind[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(ind[i].brn_cd) ? string.Concat("'", ind[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(ind[i].acc_type_cd.ToString()) ? string.Concat("'", ind[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(ind[i].acc_num) ? string.Concat("'", ind[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(ind[i].srl_no.ToString()) ? string.Concat("'", ind[i].srl_no, "'") : "srl_no",
                                         !string.IsNullOrWhiteSpace(ind[i].introducer_name) ? string.Concat("'", ind[i].introducer_name, "'") : "introducer_name",
                                         !string.IsNullOrWhiteSpace(ind[i].introducer_acc_type.ToString()) ? string.Concat("'", ind[i].introducer_acc_type, "'") : "introducer_acc_type",
                                         !string.IsNullOrWhiteSpace(ind[i].introducer_acc_num) ? string.Concat("'", ind[i].introducer_acc_num, "'") : "introducer_acc_num",
                                         !string.IsNullOrWhiteSpace(ind[i].ardb_cd) ? string.Concat("'", ind[i].ardb_cd, "'") : "ardb_cd",
                                        !string.IsNullOrWhiteSpace(ind[i].brn_cd) ? string.Concat("'", ind[i].brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(ind[i].acc_type_cd.ToString()) ? string.Concat("'", ind[i].acc_type_cd, "'") : "acc_type_cd",
                                           string.Concat("'", ind[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                      string.Concat("'", ind[i].ardb_cd, "'"),
                                                      string.Concat("'", ind[i].brn_cd, "'"),
                                                      ind[i].acc_type_cd,
                                                      string.Concat("'", ind[i].acc_num, "'"),
                                                      ind[i].srl_no,
                                                      string.Concat("'", ind[i].introducer_name, "'"),
                                                      string.Concat("'", ind[i].introducer_acc_type, "'"),
                                                      string.Concat("'", ind[i].introducer_acc_num, "'")
                                                       );
                }

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }


        internal bool UpdateNomineeTemp(DbConnection connection, List<td_nominee> nom)
        {
            string _queryd = " DELETE FROM TD_NOMINEE_TEMP "
                         + " WHERE ardb_cd={0} and  brn_cd = {1} AND acc_num = {2} ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(nom[0].ardb_cd) ? string.Concat("'", nom[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(nom[0].brn_cd) ? string.Concat("'", nom[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(nom[0].acc_num) ? string.Concat("'", nom[0].acc_num, "'") : "acc_num"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = " UPDATE TD_NOMINEE_TEMP "
             + " SET brn_cd  = {0} , "
             + " acc_type_cd = {1} , "
             + " acc_num     = {2} , "
             + " nom_id      = {3} , "
             + " nom_name    = {4} , "
             + " nom_addr1   = {5} , "
             + " nom_addr2   = {6} , "
             + " phone_no    = {7} , "
             + " percentage  = {8} , "
             + " relation    = {9}  "
             + " WHERE ardb_cd = {10} And brn_cd = {11} AND acc_num = {12} AND nom_id = {13} AND acc_type_cd=NVL({14},  acc_type_cd ) ";
            string _queryins = "INSERT INTO TD_NOMINEE_TEMP (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                         + " VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                nom[i].upd_ins_flag = "I";
                if (nom[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(nom[i].brn_cd) ? "brn_cd" : string.Concat("'", nom[i].brn_cd, "'"),
                                         !string.IsNullOrWhiteSpace(nom[i].acc_type_cd.ToString()) ? string.Concat("'", nom[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].acc_num) ? string.Concat("'", nom[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_id.ToString()) ? string.Concat("'", nom[i].nom_id, "'") : "nom_id",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_name) ? string.Concat("'", nom[i].nom_name, "'") : "nom_name",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_addr1) ? string.Concat("'", nom[i].nom_addr1, "'") : "nom_addr1",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_addr2) ? string.Concat("'", nom[i].nom_addr2, "'") : "nom_addr2",
                                         !string.IsNullOrWhiteSpace(nom[i].phone_no) ? string.Concat("'", nom[i].phone_no, "'") : "phone_no",
                                         !string.IsNullOrWhiteSpace(nom[i].percentage.ToString()) ? string.Concat("'", nom[i].percentage, "'") : "percentage",
                                         !string.IsNullOrWhiteSpace(nom[i].relation) ? string.Concat("'", nom[i].relation, "'") : "relation",
                                         !string.IsNullOrWhiteSpace(nom[i].ardb_cd) ? string.Concat("'", nom[i].ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].brn_cd) ? string.Concat("'", nom[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].acc_num) ? string.Concat("'", nom[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_id.ToString()) ? string.Concat("'", nom[i].nom_id, "'") : "nom_id",
                                          string.Concat("'", nom[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                 string.Concat("'", nom[i].ardb_cd, "'"),
                                                 string.Concat("'", nom[i].brn_cd, "'"),
                                                 nom[i].acc_type_cd,
                                                 string.Concat("'", nom[i].acc_num, "'"),
                                                 nom[i].nom_id,
                                                 string.Concat("'", nom[i].nom_name, "'"),
                                                 string.Concat("'", nom[i].nom_addr1, "'"),
                                                 string.Concat("'", nom[i].nom_addr2, "'"),
                                                 string.Concat("'", nom[i].phone_no, "'"),
                                                 nom[i].percentage,
                                                 string.Concat("'", nom[i].relation, "'")
                                                  );

                }

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }


        internal bool UpdateSignatoryTemp(DbConnection connection, List<td_signatory> sig)
        {
            string _queryd = " DELETE FROM TD_SIGNATORY_TEMP  "
             + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND acc_type_cd={3} AND DEL_FLAG= 'N' ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(sig[0].ardb_cd) ? string.Concat("'", sig[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(sig[0].brn_cd) ? string.Concat("'", sig[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(sig[0].acc_num) ? string.Concat("'", sig[0].acc_num, "'") : "acc_num",
                                     !string.IsNullOrWhiteSpace(sig[0].acc_type_cd.ToString()) ? string.Concat("'", sig[0].acc_type_cd, "'") : "acc_type_cd"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = " UPDATE TD_SIGNATORY_TEMP  "
             + " SET brn_cd     = {0}  ,  "
             + " acc_type_cd    = {1}  ,  "
             + " acc_num        = {2}  ,  "
             + " signatory_name = {3}     "
            + " WHERE ardb_cd ={4}, brn_cd = {5} AND acc_num = {6} AND acc_type_cd=NVL({7},  acc_type_cd ) ";
            string _queryins = "INSERT INTO TD_SIGNATORY_TEMP ( ardb_cd,brn_cd, acc_type_cd, acc_num, signatory_name,del_flag) "
                          + " VALUES( {0},{1},{2},{3},{4},'N') ";
            for (int i = 0; i < sig.Count; i++)
            {
                sig[i].upd_ins_flag = "I";
                if (sig[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(sig[i].brn_cd) ? string.Concat("'", sig[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(sig[i].acc_type_cd.ToString()) ? string.Concat("'", sig[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(sig[i].acc_num) ? string.Concat("'", sig[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(sig[i].signatory_name) ? string.Concat("'", sig[i].signatory_name, "'") : "signatory_name",
                                         !string.IsNullOrWhiteSpace(sig[i].ardb_cd) ? string.Concat("'", sig[i].ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(sig[i].brn_cd) ? string.Concat("'", sig[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(sig[i].acc_num) ? string.Concat("'", sig[i].acc_num, "'") : "acc_num",
                                         string.Concat("'", sig[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                            string.Concat("'", sig[i].ardb_cd, "'"),
                                                           string.Concat("'", sig[i].brn_cd, "'"),
                                                           sig[i].acc_type_cd,
                                                           string.Concat("'", sig[i].acc_num, "'"),
                                                           string.Concat("'", sig[i].signatory_name, "'")
                                                            );

                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }


        internal bool UpdateAccholderTemp(DbConnection connection, List<td_accholder> acc)
        {
            string _queryd = " DELETE FROM td_accholder_temp "
            + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = {3}";
            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(acc[0].ardb_cd) ? string.Concat("'", acc[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(acc[0].brn_cd) ? string.Concat("'", acc[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(acc[0].acc_num) ? string.Concat("'", acc[0].acc_num, "'") : "acc_num",
                                     (acc[0].acc_type_cd > 0) ? acc[0].acc_type_cd.ToString() : "ACC_TYPE_CD"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = " UPDATE td_accholder_temp   "
                 + " SET brn_cd     = {0}, "
                 + " acc_type_cd    = {1}, "
                 + " acc_num        = {2}, "
                 + " acc_holder     = {3}, "
                 + " relation       = {4}, "
                 + " cust_cd        = {5} "
                + " WHERE ardb_cd={6} and brn_cd = {7} AND acc_num = {8} AND acc_type_cd=NVL({9},  acc_type_cd )  and del_flag='N' ";
            string _queryins = "INSERT INTO TD_ACCHOLDER_TEMP ( ardb_cd,brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,del_flag ) "
                        + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N' ) ";

            for (int i = 0; i < acc.Count; i++)
            {
                acc[i].upd_ins_flag = "I";
                if (acc[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_type_cd.ToString()) ? string.Concat("'", acc[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_holder) ? string.Concat("'", acc[i].acc_holder, "'") : "acc_holder",
                                         !string.IsNullOrWhiteSpace(acc[i].relation) ? string.Concat("'", acc[i].relation, "'") : "relation",
                                         !string.IsNullOrWhiteSpace(acc[i].cust_cd.ToString()) ? string.Concat("'", acc[i].cust_cd, "'") : "cust_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].ardb_cd) ? string.Concat("'", acc[i].ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                         string.Concat("'", acc[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                       string.Concat("'", acc[i].ardb_cd, "'"),
                                                       string.Concat("'", acc[i].brn_cd, "'"),
                                                       acc[i].acc_type_cd,
                                                       string.Concat("'", acc[i].acc_num, "'"),
                                                       string.Concat("'", acc[i].acc_holder, "'"),
                                                       string.Concat("'", acc[i].relation, "'"),
                                                       acc[i].cust_cd
                                                        );

                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            return true;
        }


        public bool UpdateDenominationDtls(DbConnection connection, List<tm_denomination_trans> tdt)
        {
            string _queryd = "DELETE FROM TM_DENOMINATION_TRANS "
                            + " WHERE BRN_CD = {0} AND TRANS_DT = {1} AND TRANS_CD = {2} ";
            try
            {
                _statement = string.Format(_queryd,
                             string.IsNullOrWhiteSpace(tdt[0].brn_cd) ? "brn_cd" : string.Concat("'", tdt[0].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[0].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[0].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             tdt[0].trans_cd != 0 ? Convert.ToString(tdt[0].trans_cd) : "trans_cd"
                           );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = "UPDATE TM_DENOMINATION_TRANS SET RUPEES = {0},COUNT = {1},TOTAL = {2} "
                            + " WHERE BRN_CD = {3} AND TRANS_DT = {4} AND TRANS_CD = {5} ";
            string _queryins = "INSERT INTO TM_DENOMINATION_TRANS (BRN_CD, TRANS_DT, TRANS_CD, RUPEES, COUNT, TOTAL, CREATED_DT, CREATED_BY)"
                           + " VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})";

            for (int i = 0; i < tdt.Count; i++)
            {
                tdt[i].upd_ins_flag = "I";
                if (tdt[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                   string.Concat(tdt[i].rupees),
                                   string.Concat(tdt[i].count),
                                   string.Concat(tdt[i].total),
                                   string.IsNullOrWhiteSpace(tdt[i].brn_cd) ? "brn_cd" : string.Concat("'", tdt[i].brn_cd, "'"),
                                   string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                   tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : "trans_cd"
                                 );
                }
                else
                {
                    _statement = string.Format(_queryins,
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                             string.Concat(tdt[i].rupees),
                             string.Concat(tdt[i].count),
                             string.Concat(tdt[i].total),
                             //.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("sysdate"),
                             string.Concat("'", tdt[i].created_by, "'")
                             );
                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }

            }
            return true;
        }


        public bool UpdateTransfer(DbConnection connection, List<tm_transfer> tdt)
        {
            string _queryd = "DELETE FROM TM_TRANSFER  "
             + " WHERE  (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
             + " (TRF_DT = {2}) AND  "
             + " (  TRANS_CD = {3} ) ";
            try
            {
                _statement = string.Format(_queryd,
                             string.Concat("'", tdt[0].ardb_cd, "'"),
                             string.Concat("'", tdt[0].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[0].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[0].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt[0].trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }

            string _query = "UPDATE TM_TRANSFER SET "
                   + " TRF_DT               =NVL({0},TRF_DT       ),"
                   + " TRF_CD               =NVL({1},TRF_CD       ),"
                   + " TRANS_CD               =NVL({2},TRANS_CD       ),"
                   + " CREATED_BY             =NVL({3},CREATED_BY     ),"
                   + " CREATED_DT             =NVL({4},CREATED_DT     ),"
                   + " APPROVAL_STATUS        =NVL({5},APPROVAL_STATUS),"
                   + " APPROVED_BY            =NVL({6},APPROVED_BY    ),"
                   + " APPROVED_DT            =NVL({7},APPROVED_DT    ),"
                   + " BRN_CD                 =NVL({8},BRN_CD         )"
              + " WHERE (ARDB_CD = {9}) AND (BRN_CD = {10}) AND "
              + " (TRF_DT = {11} ) AND  "
              + " (  TRF_CD = {12} ) AND DEL_FLAG ='N' ";
            string _queryins = "INSERT INTO TM_TRANSFER (TRF_DT,TRF_CD,TRANS_CD,CREATED_BY,"
                      + " CREATED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,"
                      + " BRN_CD,ARDB_CD,DEL_FLAG)"
                      + " VALUES ({0},{1},{2},{3},"
                      + " {4},{5},{6},{7},"
                      + " {8},{9},'N')";
            for (int i = 0; i < tdt.Count; i++)
            {
                tdt[i].upd_ins_flag = "I";
                if (tdt[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].trans_cd, "'"),
                             string.Concat("'", tdt[i].trans_cd, "'"),
                             string.Concat("'", tdt[i].created_by, "'"),
                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("sysdate"),
                             string.Concat("'", tdt[i].approval_status, "'"),
                             string.Concat("'", tdt[i].approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.Concat("'", tdt[i].ardb_cd, "'"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].trf_cd, "'")
                             );
                }
                else
                {
                    int maxTrfCD = GetTrfCDMaxId(connection, tdt[i]);
                    _statement = string.Format(_queryins,
                           string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                           tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                           string.Concat("'", tdt[i].created_by, "'"),
                           //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           string.Concat("sysdate"),
                           string.Concat("'", tdt[i].approval_status, "'"),
                           string.Concat("'", tdt[i].approved_by, "'"),
                           string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           string.Concat("'", tdt[i].brn_cd, "'"),
                           string.Concat("'", tdt[i].ardb_cd, "'")
                           );
                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }

            }
            return true;
        }


        public bool UpdateDepTransTrf(DbConnection connection, List<td_def_trans_trf> tdt)
        {
            string _queryd = "DELETE FROM TD_DEP_TRANS_TRF  "
            + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
            + " (TRANS_DT = {2}) AND  "
            + " (  TRANS_CD = {3} ) ";

            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt[0].ardb_cd, "'"),
                             string.Concat("'", tdt[0].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[0].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[0].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt[0].trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            string _query = "UPDATE TD_DEP_TRANS_TRF SET "
         + " TRANS_DT               =NVL({0},TRANS_DT       ),"
         + " TRANS_CD               =NVL({1},TRANS_CD       ),"
         + " ACC_TYPE_CD            =NVL({2},ACC_TYPE_CD    ),"
         + " ACC_NUM                =NVL({3},ACC_NUM        ),"
         + " TRANS_TYPE             =NVL({4},TRANS_TYPE     ),"
         + " TRANS_MODE             =NVL({5},TRANS_MODE     ),"
         + " AMOUNT                 =NVL({6},AMOUNT         ),"
         + " INSTRUMENT_DT          =NVL({7},INSTRUMENT_DT  ),"
         + " INSTRUMENT_NUM         =NVL({8},INSTRUMENT_NUM ),"
         + " PAID_TO                =NVL({9},PAID_TO        ),"
         + " TOKEN_NUM              =NVL({10},TOKEN_NUM      ),"
         + " CREATED_BY             =NVL({11},CREATED_BY     ),"
         + " CREATED_DT             =NVL({12},CREATED_DT     ),"
         + " MODIFIED_BY            =NVL({13},MODIFIED_BY    ),"
         + " MODIFIED_DT            =NVL({14},MODIFIED_DT    ),"
         + " APPROVAL_STATUS        =NVL({15},APPROVAL_STATUS),"
         + " APPROVED_BY            =NVL({16},APPROVED_BY    ),"
         + " APPROVED_DT            =NVL({17},APPROVED_DT    ),"
         + " PARTICULARS            =NVL({18},PARTICULARS    ),"
         + " TR_ACC_TYPE_CD         =NVL({19},TR_ACC_TYPE_CD ),"
         + " TR_ACC_NUM             =NVL({20},TR_ACC_NUM     ),"
         + " VOUCHER_DT             =NVL({21},VOUCHER_DT     ),"
         + " VOUCHER_ID             =NVL({22},VOUCHER_ID     ),"
         + " TRF_TYPE               =NVL({23},TRF_TYPE       ),"
         + " TR_ACC_CD              =NVL({24},TR_ACC_CD      ),"
         + " ACC_CD                 =NVL({25},ACC_CD         ),"
         + " SHARE_AMT              =NVL({26},SHARE_AMT      ),"
         + " SUM_ASSURED            =NVL({27},SUM_ASSURED    ),"
         + " PAID_AMT               =NVL({28},PAID_AMT       ),"
         + " CURR_PRN_RECOV         =NVL({29},CURR_PRN_RECOV ),"
         + " OVD_PRN_RECOV          =NVL({30},OVD_PRN_RECOV  ),"
         + " CURR_INTT_RECOV        =NVL({31},CURR_INTT_RECOV),"
         + " OVD_INTT_RECOV         =NVL({32},OVD_INTT_RECOV ),"
         + " REMARKS                =NVL({33},REMARKS        ),"
         + " CROP_CD                =NVL({34},CROP_CD        ),"
         + " ACTIVITY_CD            =NVL({35},ACTIVITY_CD    ),"
         + " CURR_INTT_RATE         =NVL({36},CURR_INTT_RATE ),"
         + " OVD_INTT_RATE          =NVL({37},OVD_INTT_RATE  ),"
         + " INSTL_NO               =NVL({38},INSTL_NO       ),"
         + " INSTL_START_DT         =NVL({39},INSTL_START_DT ),"
         + " PERIODICITY            =NVL({40},PERIODICITY    ),"
         + " DISB_ID                =NVL({41},DISB_ID        ),"
         + " COMP_UNIT_NO           =NVL({42},COMP_UNIT_NO   ),"
         + " ONGOING_UNIT_NO        =NVL({43},ONGOING_UNIT_NO),"
         + " MIS_ADVANCE_RECOV      =NVL({44},MIS_ADVANCE_RECOV),"
         + " AUDIT_FEES_RECOV       =NVL({45},AUDIT_FEES_RECOV),"
         + " SECTOR_CD              =NVL({46},SECTOR_CD      ),"
         + " SPL_PROG_CD            =NVL({47},SPL_PROG_CD    ),"
         + " BORROWER_CR_CD         =NVL({48},BORROWER_CR_CD ),"
         + " INTT_TILL_DT           =NVL({49},INTT_TILL_DT   ),"
         + " BRN_CD                 =NVL({50},BRN_CD         ),"
         + " PENAL_INTT_RECOV       =NVL({51},PENAL_INTT_RECOV),"
         + " ADV_PRN_RECOV          =NVL({52},ADV_PRN_RECOV) "
    + " WHERE (ARDB_CD = {53}) AND (BRN_CD = {54}) AND "
    + " (TRANS_DT = {55}) AND  "
    + " (  TRANS_CD = {56} ) AND  "
    + " ACC_TYPE_CD = {57} AND "
    + " ACC_NUM = {58} AND DEL_FLAG = 'N' ";
            string _queryins = "INSERT INTO TD_DEP_TRANS_TRF (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                                + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                                + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                                + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                                + " BRN_CD,ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                                + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                                + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                                + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                                + " {38},{39},{40},{41},{42},{43},{44},{45},{46},{47},{48},{49}, "
                                + " {50},{51},'N',{52},{53},{54})";
            for (int i = 0; i < tdt.Count; i++)
            {
                tdt[i].upd_ins_flag = "I";
                if (tdt[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                 string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat(tdt[i].trans_cd),
                                 string.Concat("'", tdt[i].acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].acc_num, "'"),
                                 string.Concat("'", tdt[i].trans_type, "'"),
                                 string.Concat("'", tdt[i].trans_mode, "'"),
                                 string.Concat("'", tdt[i].amount, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].instrument_num, "'"),
                                 string.Concat("'", tdt[i].paid_to, "'"),
                                 string.Concat("'", tdt[i].token_num, "'"),
                                 string.Concat("'", tdt[i].created_by, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].modified_by, "'"),
                                 string.Concat("sysdate"),
                                 //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].approval_status, "'"),
                                 string.Concat("'", tdt[i].approved_by, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].particulars, "'"),
                                 string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].tr_acc_num, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].voucher_id, "'"),
                                 string.Concat("'", tdt[i].trf_type, "'"),
                                 string.Concat("'", tdt[i].tr_acc_cd, "'"),
                                 string.Concat("'", tdt[i].acc_cd, "'"),
                                 string.Concat("'", tdt[i].share_amt, "'"),
                                 string.Concat("'", tdt[i].sum_assured, "'"),
                                 string.Concat("'", tdt[i].paid_amt, "'"),
                                 string.Concat("'", tdt[i].curr_prn_recov, "'"),
                                 string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                                 string.Concat("'", tdt[i].curr_intt_recov, "'"),
                                 string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                                 string.Concat("'", tdt[i].remarks, "'"),
                                 string.Concat("'", tdt[i].crop_cd, "'"),
                                 string.Concat("'", tdt[i].activity_cd, "'"),
                                 string.Concat("'", tdt[i].curr_intt_rate, "'"),
                                 string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                                 string.Concat("'", tdt[i].instl_no, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].periodicity, "'"),
                                 string.Concat("'", tdt[i].disb_id, "'"),
                                 string.Concat("'", tdt[i].comp_unit_no, "'"),
                                 string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                 string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                 string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                 string.Concat("'", tdt[i].sector_cd, "'"),
                                 string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                 string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].brn_cd, "'"),
                                 string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                 string.Concat("'", tdt[i].adv_prn_recov, "'"),
                                 string.Concat("'", tdt[i].ardb_cd, "'"),
                                 string.Concat("'", tdt[i].brn_cd, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat(tdt[i].trans_cd),
                                 string.Concat("'", tdt[i].acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].acc_num, "'")
                                 );
                }
                else
                {
                    _statement = string.Format(_queryins,
                            string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                            string.Concat("'", tdt[i].acc_type_cd, "'"),
                            string.Concat("'", tdt[i].acc_num, "'"),
                            string.Concat("'", tdt[i].trans_type, "'"),
                            string.Concat("'", tdt[i].trans_mode, "'"),
                            string.Concat("'", tdt[i].amount, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].instrument_num, "'"),
                            string.Concat("'", tdt[i].paid_to, "'"),
                            string.Concat("'", tdt[i].token_num, "'"),
                            string.Concat("'", tdt[i].created_by, "'"),
                            string.Concat("sysdate"),
                            //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].modified_by, "'"),
                            string.Concat("sysdate"),
                            //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].approval_status, "'"),
                            string.Concat("'", tdt[i].approved_by, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].particulars, "'"),
                            string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                            string.Concat("'", tdt[i].tr_acc_num, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].voucher_id, "'"),
                            string.Concat("'", tdt[i].trf_type, "'"),
                            string.Concat("'", tdt[i].tr_acc_cd, "'"),
                            string.Concat("'", tdt[i].acc_cd, "'"),
                            string.Concat("'", tdt[i].share_amt, "'"),
                            string.Concat("'", tdt[i].sum_assured, "'"),
                            string.Concat("'", tdt[i].paid_amt, "'"),
                            string.Concat("'", tdt[i].curr_prn_recov, "'"),
                            string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                            string.Concat("'", tdt[i].curr_intt_recov, "'"),
                            string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                            string.Concat("'", tdt[i].remarks, "'"),
                            string.Concat("'", tdt[i].crop_cd, "'"),
                            string.Concat("'", tdt[i].activity_cd, "'"),
                            string.Concat("'", tdt[i].curr_intt_rate, "'"),
                            string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                            string.Concat("'", tdt[i].instl_no, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].periodicity, "'"),
                            string.Concat("'", tdt[i].disb_id, "'"),
                            string.Concat("'", tdt[i].comp_unit_no, "'"),
                            string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                            string.Concat("'", tdt[i].mis_advance_recov, "'"),
                            string.Concat("'", tdt[i].audit_fees_recov, "'"),
                            string.Concat("'", tdt[i].sector_cd, "'"),
                            string.Concat("'", tdt[i].spl_prog_cd, "'"),
                            string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].brn_cd, "'"),
                            string.Concat("'", tdt[i].ardb_cd, "'"),
                            string.Concat("'", tdt[i].penal_intt_recov, "'"),
                            string.Concat("'", tdt[i].adv_prn_recov, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                            );

                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }

            }
            return true;
        }



        public bool UpdateDepTrans(DbConnection connection, td_def_trans_trf tdt)
        {
            string _query = "UPDATE TD_DEP_TRANS SET "
                     + " TRANS_DT               =NVL({0},TRANS_DT       ),"
         + " TRANS_CD               =NVL({1},TRANS_CD       ),"
         + " ACC_TYPE_CD            =NVL({2},ACC_TYPE_CD    ),"
         + " ACC_NUM                =NVL({3},ACC_NUM        ),"
         + " TRANS_TYPE             =NVL({4},TRANS_TYPE     ),"
         + " TRANS_MODE             =NVL({5},TRANS_MODE     ),"
         + " AMOUNT                 =NVL({6},AMOUNT         ),"
         + " INSTRUMENT_DT          =NVL({7},INSTRUMENT_DT  ),"
         + " INSTRUMENT_NUM         =NVL({8},INSTRUMENT_NUM ),"
         + " PAID_TO                =NVL({9},PAID_TO        ),"
         + " TOKEN_NUM              =NVL({10},TOKEN_NUM      ),"
         + " CREATED_BY             =NVL({11},CREATED_BY     ),"
         + " CREATED_DT             =NVL({12},CREATED_DT     ),"
         + " MODIFIED_BY            =NVL({13},MODIFIED_BY    ),"
         + " MODIFIED_DT            =NVL({14},MODIFIED_DT    ),"
         + " APPROVAL_STATUS        =NVL({15},APPROVAL_STATUS),"
         + " APPROVED_BY            =NVL({16},APPROVED_BY    ),"
         + " APPROVED_DT            =NVL({17},APPROVED_DT    ),"
         + " PARTICULARS            =NVL({18},PARTICULARS    ),"
         + " TR_ACC_TYPE_CD         =NVL({19},TR_ACC_TYPE_CD ),"
         + " TR_ACC_NUM             =NVL({20},TR_ACC_NUM     ),"
         + " VOUCHER_DT             =NVL({21},VOUCHER_DT     ),"
         + " VOUCHER_ID             =NVL({22},VOUCHER_ID     ),"
         + " TRF_TYPE               =NVL({23},TRF_TYPE       ),"
         + " TR_ACC_CD              =NVL({24},TR_ACC_CD      ),"
         + " ACC_CD                 =NVL({25},ACC_CD         ),"
         + " SHARE_AMT              =NVL({26},SHARE_AMT      ),"
         + " SUM_ASSURED            =NVL({27},SUM_ASSURED    ),"
         + " PAID_AMT               =NVL({28},PAID_AMT       ),"
         + " CURR_PRN_RECOV         =NVL({29},CURR_PRN_RECOV ),"
         + " OVD_PRN_RECOV          =NVL({30},OVD_PRN_RECOV  ),"
         + " CURR_INTT_RECOV        =NVL({31},CURR_INTT_RECOV),"
         + " OVD_INTT_RECOV         =NVL({32},OVD_INTT_RECOV ),"
         + " REMARKS                =NVL({33},REMARKS        ),"
         + " CROP_CD                =NVL({34},CROP_CD        ),"
         + " ACTIVITY_CD            =NVL({35},ACTIVITY_CD    ),"
         + " CURR_INTT_RATE         =NVL({36},CURR_INTT_RATE ),"
         + " OVD_INTT_RATE          =NVL({37},OVD_INTT_RATE  ),"
         + " INSTL_NO               =NVL({38},INSTL_NO       ),"
         + " INSTL_START_DT         =NVL({39},INSTL_START_DT ),"
         + " PERIODICITY            =NVL({40},PERIODICITY    ),"
         + " DISB_ID                =NVL({41},DISB_ID        ),"
         + " COMP_UNIT_NO           =NVL({42},COMP_UNIT_NO   ),"
         + " ONGOING_UNIT_NO        =NVL({43},ONGOING_UNIT_NO),"
         + " MIS_ADVANCE_RECOV      =NVL({44},MIS_ADVANCE_RECOV),"
         + " AUDIT_FEES_RECOV       =NVL({45},AUDIT_FEES_RECOV),"
         + " SECTOR_CD              =NVL({46},SECTOR_CD      ),"
         + " SPL_PROG_CD            =NVL({47},SPL_PROG_CD    ),"
         + " BORROWER_CR_CD         =NVL({48},BORROWER_CR_CD ),"
         + " INTT_TILL_DT           =NVL({49},INTT_TILL_DT   ),"
         + " BRN_CD                 =NVL({50},BRN_CD         ),"
         + " PENAL_INTT_RECOV       =NVL({51},PENAL_INTT_RECOV),"
         + " ADV_PRN_RECOV          =NVL({52},ADV_PRN_RECOV )"
                + " WHERE ARDB_CD = {53} AND BRN_CD = {54} AND "
                + " TRANS_DT = {55} AND  "
                + " TRANS_CD = {56} AND  "
                + " ACC_TYPE_CD = {57} AND "
                + " ACC_NUM = {58} AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                            string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd),
                             string.Concat("'", tdt.acc_type_cd, "'"),
                             string.Concat("'", tdt.acc_num, "'"),
                             string.Concat("'", tdt.trans_type, "'"),
                             string.Concat("'", tdt.trans_mode, "'"),
                             string.Concat("'", tdt.amount, "'"),
                             string.IsNullOrWhiteSpace(tdt.instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.instrument_num, "'"),
                             string.Concat("'", tdt.paid_to, "'"),
                             string.Concat("'", tdt.token_num, "'"),
                             string.Concat("'", tdt.created_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.modified_by, "'"),
                             string.Concat("sysdate"),
                             //string.IsNullOrWhiteSpace(tdt.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.approval_status, "'"),
                             string.Concat("'", tdt.approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.particulars, "'"),
                             string.Concat("'", tdt.tr_acc_type_cd, "'"),
                             string.Concat("'", tdt.tr_acc_num, "'"),
                             string.IsNullOrWhiteSpace(tdt.voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.voucher_id, "'"),
                             string.Concat("'", tdt.trf_type, "'"),
                             string.Concat("'", tdt.tr_acc_cd, "'"),
                             string.Concat("'", tdt.acc_cd, "'"),
                             string.Concat("'", tdt.share_amt, "'"),
                             string.Concat("'", tdt.sum_assured, "'"),
                             string.Concat("'", tdt.paid_amt, "'"),
                             string.Concat("'", tdt.curr_prn_recov, "'"),
                             string.Concat("'", tdt.ovd_prn_recov, "'"),
                             string.Concat("'", tdt.curr_intt_recov, "'"),
                             string.Concat("'", tdt.ovd_intt_recov, "'"),
                             string.Concat("'", tdt.remarks, "'"),
                             string.Concat("'", tdt.crop_cd, "'"),
                             string.Concat("'", tdt.activity_cd, "'"),
                             string.Concat("'", tdt.curr_intt_rate, "'"),
                             string.Concat("'", tdt.ovd_intt_rate, "'"),
                             string.Concat("'", tdt.instl_no, "'"),
                             string.IsNullOrWhiteSpace(tdt.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.periodicity, "'"),
                             string.Concat("'", tdt.disb_id, "'"),
                             string.Concat("'", tdt.comp_unit_no, "'"),
                             string.Concat("'", tdt.ongoing_unit_no, "'"),
                             string.Concat("'", tdt.mis_advance_recov, "'"),
                             string.Concat("'", tdt.audit_fees_recov, "'"),
                             string.Concat("'", tdt.sector_cd, "'"),
                             string.Concat("'", tdt.spl_prog_cd, "'"),
                             string.Concat("'", tdt.borrower_cr_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.Concat("'", tdt.penal_intt_recov, "'"),
                             string.Concat("'", tdt.adv_prn_recov, "'"),
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                            string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat(tdt.trans_cd),
                                             string.Concat("'", tdt.acc_type_cd, "'"),
                                             string.Concat("'", tdt.acc_num, "'")
                                             );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }


        internal string GetCustMinSavingsAccNo(tm_deposit cust)
        {
            string accountNumber = null;
            string _query = "SELECT MIN(ACC_NUM) ACC_NUM FROM TM_DEPOSIT"
                            + " WHERE ARDB_CD = {0} AND ACC_TYPE_CD = {1} "
                            + " AND CUST_CD = {2} AND UPPER(ACC_STATUS) <> 'C' AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                       string.Concat("'", cust.ardb_cd, "'"),
                                       cust.acc_type_cd == 0 ? 8 : cust.acc_type_cd,
                                       cust.cust_cd);
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                accountNumber = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                            }
                        }
                    }
                }
                return accountNumber;
            }
        }

        internal bool DeleteDepositTemp(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE TM_DEPOSIT_TEMP SET DEL_FLAG = 'Y'  "
                  + "WHERE ARDB_CD = {0} AND brn_cd = NVL({1}, brn_cd) AND acc_num = NVL({2},  acc_num ) AND acc_type_cd=NVL({3},  acc_type_cd ) AND DEL_FLAG='N'";

            _statement = string.Format(_queryd,
                                         !string.IsNullOrWhiteSpace(ind.ardb_cd) ? string.Concat("'", ind.ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(ind.brn_cd) ? string.Concat("'", ind.brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(ind.acc_num) ? string.Concat("'", ind.acc_num, "'") : "acc_num",
                                         (ind.acc_type_cd > 0) ? ind.acc_type_cd.ToString() : "ACC_TYPE_CD"
                                          );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }

        internal bool DeleteDepositRenewTemp(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE  TM_DEPOSIT_RENEW_TEMP  SET DEL_FLAG = 'Y' "
                  + "WHERE  ARDB_CD = {0} AND brn_cd = NVL({1}, brn_cd) AND acc_num = NVL({2},  acc_num ) AND acc_type_cd=NVL({3},  acc_type_cd ) AND DEL_FLAG = 'N' ";

            _statement = string.Format(_queryd,
                                         !string.IsNullOrWhiteSpace(ind.ardb_cd) ? string.Concat("'", ind.ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(ind.brn_cd) ? string.Concat("'", ind.brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(ind.acc_num) ? string.Concat("'", ind.acc_num, "'") : "acc_num",
                                         (ind.acc_type_cd > 0) ? ind.acc_type_cd.ToString() : "ACC_TYPE_CD"
                                          );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }

        internal bool DeleteIntroducerTemp(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE TD_INTRODUCER_TEMP SET DEL_FLAG = 'Y' "
             + " WHERE   ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = {3} AND DEL_FLAG = 'N' ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(ind.ardb_cd) ? string.Concat("'", ind.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(ind.brn_cd) ? string.Concat("'", ind.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(ind.acc_num) ? string.Concat("'", ind.acc_num, "'") : "acc_num",
                                     (ind.acc_type_cd > 0) ? ind.acc_type_cd.ToString() : "ACC_TYPE_CD"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }


        internal bool DeleteNomineeTemp(DbConnection connection, td_def_trans_trf nom)
        {
            string _queryd = " UPDATE TD_NOMINEE_TEMP  SET DEL_FLAG = 'Y' "
                         + " WHERE   ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2}  AND acc_type_cd={3} AND DEL_FLAG='N'";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                     !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }


        internal bool DeleteSignatoryTemp(DbConnection connection, td_def_trans_trf sig)
        {
            string _queryd = " UPDATE TD_SIGNATORY_TEMP  SET DEL_FLAG = 'Y' "
             + " WHERE    ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2} AND acc_type_cd={3} AND DEL_FLAG='N' ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(sig.ardb_cd) ? string.Concat("'", sig.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(sig.brn_cd) ? string.Concat("'", sig.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(sig.acc_num) ? string.Concat("'", sig.acc_num, "'") : "acc_num",
                                     !string.IsNullOrWhiteSpace(sig.acc_type_cd.ToString()) ? string.Concat("'", sig.acc_type_cd, "'") : "acc_type_cd"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }

            return true;
        }

        internal bool DeleteAccholderTemp(DbConnection connection, td_def_trans_trf acc)
        {
            string _queryd = " UPDATE td_accholder_temp   SET DEL_FLAG = 'Y' "
            + " WHERE   ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = {3} AND DEL_FLAG = 'N' ";
            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num",
                                     (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            return true;
        }

        internal bool DeleteDenominationDtls(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "DELETE FROM TM_DENOMINATION_TRANS "
                            + " WHERE BRN_CD = {0} AND TRANS_DT = {1} AND TRANS_CD = {2} ";
            try
            {
                _statement = string.Format(_queryd,
                             string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
                           );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            return true;
        }


        internal bool DeleteTransfer(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TM_TRANSFER SET DEL_FLAG = 'Y' "
              + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
               + " (TRF_DT = {2}) AND  "
              + " (  TRANS_CD = {3} ) AND (DEL_FLAG = 'N') ";
            try
            {
                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;

        }


        internal bool DeleteDepTransTrf(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TD_DEP_TRANS_TRF    SET DEL_FLAG = 'Y'  "
           + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
           + " (TRANS_DT = {2}) AND  "
           + " (  TRANS_CD = {3} )  AND DEL_FLAG='N' ";
            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }
        
        internal bool DeleteDepTrans(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TD_DEP_TRANS     SET DEL_FLAG = 'Y'  "
                    + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND "
                + " TRANS_DT = {2} AND  "
                + " TRANS_CD = {3}  AND DEL_FLAG='N' ";
            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }

        internal AccOpenDM GetAccountOpeningData(tm_deposit td)
        {
            AccOpenDM AccOpenDMRet = new AccOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        AccOpenDMRet.tmdeposit = GetDeposit(connection, td);
                        AccOpenDMRet.tdnominee = GetNominee(connection, td);
                        AccOpenDMRet.tdsignatory = GetSignatory(connection, td);
                        AccOpenDMRet.tdaccholder = GetAccholder(connection, td);
                        AccOpenDMRet.tdintroducer = GetIntroducer(connection, td);

                        return AccOpenDMRet;
                    }
                    catch (Exception ex)
                    {
                        return null;
                    }

                }
            }
        }



        internal tm_deposit GetDeposit(DbConnection connection, tm_deposit dep)
        {
            tm_deposit depRet = new tm_deposit();
            string _query = " SELECT ARDB_CD, BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, NVL(ACC_STATUS,'O') ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD ,DEL_FLAG                                  "
                            + " FROM TM_DEPOSIT  T1                                                               "
                            + " WHERE ARDB_CD = {0} AND ACC_NUM={1} AND ACC_TYPE_CD={2} AND DEL_FLAG = 'N'  "
                            + " AND T1.RENEW_ID = ( SELECT MAX(RENEW_ID) FROM TM_DEPOSIT T2 WHERE  ARDB_CD = {3} AND  T1.BRN_CD = T2.BRN_CD AND T1.ACC_NUM = T2.ACC_NUM AND T1.ACC_TYPE_CD = T2.ACC_TYPE_CD )";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD",
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd"
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
                                var d = new tm_deposit();
                                d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                d.renew_id = UtilityM.CheckNull<int>(reader["RENEW_ID"]);
                                d.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                d.intt_trf_type = UtilityM.CheckNull<string>(reader["INTT_TRF_TYPE"]);
                                d.constitution_cd = UtilityM.CheckNull<int>(reader["CONSTITUTION_CD"]);
                                d.oprn_instr_cd = UtilityM.CheckNull<int>(reader["OPRN_INSTR_CD"]);
                                d.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                d.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                d.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);
                                d.dep_period = UtilityM.CheckNull<string>(reader["DEP_PERIOD"]);
                                d.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                d.instl_no = UtilityM.CheckNull<decimal>(reader["INSTL_NO"]);
                                d.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                d.intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                d.tds_applicable = UtilityM.CheckNull<string>(reader["TDS_APPLICABLE"]);
                                d.last_intt_calc_dt = UtilityM.CheckNull<DateTime>(reader["LAST_INTT_CALC_DT"]);
                                d.acc_close_dt = UtilityM.CheckNull<DateTime>(reader["ACC_CLOSE_DT"]);
                                d.closing_prn_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_PRN_AMT"]);
                                d.closing_intt_amt = UtilityM.CheckNull<decimal>(reader["CLOSING_INTT_AMT"]);
                                d.penal_amt = UtilityM.CheckNull<decimal>(reader["PENAL_AMT"]);
                                d.ext_instl_tot = UtilityM.CheckNull<decimal>(reader["EXT_INSTL_TOT"]);
                                d.mat_status = UtilityM.CheckNull<string>(reader["MAT_STATUS"]);
                                d.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                d.curr_bal = UtilityM.CheckNull<decimal>(reader["CURR_BAL"]);
                                d.clr_bal = UtilityM.CheckNull<decimal>(reader["CLR_BAL"]);
                                d.standing_instr_flag = UtilityM.CheckNull<string>(reader["STANDING_INSTR_FLAG"]);
                                d.cheque_facility_flag = UtilityM.CheckNull<string>(reader["CHEQUE_FACILITY_FLAG"]);
                                d.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                d.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                d.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                d.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                d.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                d.user_acc_num = UtilityM.CheckNull<string>(reader["USER_ACC_NUM"]);
                                d.lock_mode = UtilityM.CheckNull<string>(reader["LOCK_MODE"]);
                                d.loan_id = UtilityM.CheckNull<decimal>(reader["LOAN_ID"]);
                                d.cert_no = UtilityM.CheckNull<string>(reader["CERT_NO"]);
                                d.bonus_amt = UtilityM.CheckNull<decimal>(reader["BONUS_AMT"]);
                                d.penal_intt_rt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RT"]);
                                d.bonus_intt_rt = UtilityM.CheckNull<decimal>(reader["BONUS_INTT_RT"]);
                                d.transfer_flag = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                d.transfer_dt = UtilityM.CheckNull<DateTime>(reader["TRANSFER_DT"]);
                                d.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }


        internal List<td_nominee> GetNominee(DbConnection connection, tm_deposit dep)
        {
            List<td_nominee> nomList = new List<td_nominee>();

            string _query = "SELECT ARDB_CD,BRN_CD, "
             + " ACC_TYPE_CD, "
             + " ACC_NUM,     "
             + " NOM_ID,      "
             + " NOM_NAME,    "
             + " NOM_ADDR1,   "
             + " NOM_ADDR2,   "
             + " PHONE_NO,    "
             + " PERCENTAGE,  "
             + " RELATION,DEL_FLAG     "
             + " FROM TD_NOMINEE"
             + " WHERE ARDB_CD = {0} AND ACC_TYPE_CD = {1} AND ACC_NUM = {2} AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                       !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                       dep.acc_type_cd != 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD",
                                       !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num");

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var n = new td_nominee();
                                n.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                n.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                n.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                n.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                n.nom_id = UtilityM.CheckNull<Int16>(reader["NOM_ID"]);

                                n.nom_name = UtilityM.CheckNull<string>(reader["NOM_NAME"]);
                                n.nom_addr1 = UtilityM.CheckNull<string>(reader["NOM_ADDR1"]);
                                n.nom_addr2 = UtilityM.CheckNull<string>(reader["NOM_ADDR2"]);
                                n.phone_no = UtilityM.CheckNull<string>(reader["PHONE_NO"]);
                                n.percentage = UtilityM.CheckNull<Single>(reader["PERCENTAGE"]);
                                n.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                n.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                nomList.Add(n);
                            }
                        }
                    }
                }
            }
            return nomList;
        }



        internal List<td_signatory> GetSignatory(DbConnection connection, tm_deposit sig)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "SELECT ARDB_CD,BRN_CD,"
             + " ACC_TYPE_CD,"
             + " ACC_NUM,"
             + " SIGNATORY_NAME,DEL_FLAG"
             + " FROM TD_SIGNATORY"
             + " WHERE ARDB_CD={0} AND ACC_NUM = {1} AND  ACC_TYPE_CD = {2} ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(sig.ardb_cd) ? string.Concat("'", sig.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(sig.acc_num) ? string.Concat("'", sig.acc_num, "'") : "acc_num",
                                           sig.acc_type_cd != 0 ? Convert.ToString(sig.acc_type_cd) : "ACC_TYPE_CD"
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
                                var s = new td_signatory();
                                s.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                s.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                s.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                s.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                s.signatory_name = UtilityM.CheckNull<string>(reader["SIGNATORY_NAME"]);
                                s.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                sigList.Add(s);
                            }
                        }
                    }
                }
            }
            return sigList;
        }




        internal List<td_accholder> GetAccholder(DbConnection connection, tm_deposit acc)
        {
            List<td_accholder> accList = new List<td_accholder>();

            dynamic _query = " SELECT ARDB_CD, BRN_CD, "
                 + " ACC_TYPE_CD,   "
                 + " ACC_NUM,       "
                 + " ACC_HOLDER,    "
                 + " RELATION,      "
                 + " CUST_CD,DEL_FLAG        "
                 + " FROM TD_ACCHOLDER "
                 + " WHERE ARDB_CD = {0} AND ACC_NUM = {1} AND  ACC_TYPE_CD = {2}  ";
            var v1 = !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd";
            var v3 = !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num";
            dynamic v4 = (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD";
            _statement = string.Format(_query, v1,v3,v4);


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
                                a.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                accList.Add(a);
                            }
                        }
                    }
                }
            }
            return accList;
        }


        internal List<td_introducer> GetIntroducer(DbConnection connection, tm_deposit dep)
        {
            List<td_introducer> indList = new List<td_introducer>();
            string _query = "SELECT ARDB_CD, BRN_CD, "
                 + " ACC_TYPE_CD,        "
                 + " ACC_NUM,            "
                 + " SRL_NO,             "
                 + " INTRODUCER_NAME,    "
                 + " INTRODUCER_ACC_TYPE,"
                 + " INTRODUCER_ACC_NUM,DEL_FLAG  "
                 + " FROM TD_INTRODUCER  "
                 + " WHERE ARDB_CD={0} AND ACC_NUM = {1}  AND ACC_TYPE_CD = {2}";

            _statement = string.Format(_query,
            !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
            !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
            (dep.acc_type_cd > 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var i = new td_introducer();
                                i.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                i.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                i.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                i.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);

                                i.srl_no = UtilityM.CheckNull<Int16>(reader["SRL_NO"]);
                                i.introducer_name = UtilityM.CheckNull<string>(reader["INTRODUCER_NAME"]);
                                i.introducer_acc_type = UtilityM.CheckNull<int>(reader["INTRODUCER_ACC_TYPE"]);
                                i.introducer_acc_num = UtilityM.CheckNull<string>(reader["INTRODUCER_ACC_NUM"]);
                                i.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                indList.Add(i);
                            }
                        }
                    }
                }
            }

            return indList;
        }





        internal int UpdateAccountOpeningDataOrg(AccOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.tmdeposit.acc_num))
                            UpdateDeposit(connection, acc.tmdeposit);

                        if (acc.tdnominee.Count > 0)
                            UpdateNominee(connection, acc.tdnominee);

                        if (acc.tdsignatory.Count > 0)
                            UpdateSignatory(connection, acc.tdsignatory);

                        if (acc.tdaccholder.Count > 0)
                            UpdateAccholder(connection, acc.tdaccholder);

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



        internal bool UpdateDeposit(DbConnection connection, tm_deposit dep)
        {
            string _query = " UPDATE TM_DEPOSIT SET "
                  + "oprn_instr_cd        = NVL({0},  oprn_instr_cd       ),"
                  + "standing_instr_flag  = NVL({1}, standing_instr_flag ),"
                  + "cheque_facility_flag = NVL({2}, cheque_facility_flag),"
                  + "tds_applicable       = NVL({3}, tds_applicable      ),"
                  + "cert_no       = NVL({4}, cert_no      ),"
                  + "user_acc_num       = NVL({5}, user_acc_num      ),"
                  + "modified_by          = NVL({6}, modified_by   ),"
                  + "modified_dt          = NVL({7}, modified_dt   ),"
                  + "constitution_cd          = NVL({8}, constitution_cd   ),ext_instl_tot={9}"
                  + "WHERE ardb_cd={10} AND acc_num = NVL({11},  acc_num ) AND acc_type_cd=NVL({12},  acc_type_cd ) and renew_id={13} and del_flag='N' ";

            _statement = string.Format(_query,
            string.Concat("'", dep.oprn_instr_cd, "'"),
            string.Concat("'", dep.standing_instr_flag, "'"),
            string.Concat("'", dep.cheque_facility_flag, "'"),
            string.Concat("'", dep.tds_applicable, "'"),
            string.Concat("'", dep.cert_no, "'"),
             string.Concat("'", dep.user_acc_num, "'"),
            string.Concat("'", dep.modified_by, "'"),
            string.Concat("sysdate"),
            string.Concat("'", dep.constitution_cd, "'"),
            string.Concat("'", dep.ext_instl_tot, "'"),
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.renew_id, "'")
                        );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }


        internal bool UpdateNominee(DbConnection connection, List<td_nominee> nom)
        {
            string _queryd = " DELETE FROM TD_NOMINEE "
                         + " WHERE ardb_cd = {0} AND acc_num = {1} AND acc_type_cd = {2}";


            _statement = string.Format(_queryd,
                                 !string.IsNullOrWhiteSpace(nom[0].ardb_cd) ? string.Concat("'", nom[0].ardb_cd, "'") : "ardb_cd",
                                 !string.IsNullOrWhiteSpace(nom[0].acc_num) ? string.Concat("'", nom[0].acc_num, "'") : "acc_num",
                                 !string.IsNullOrWhiteSpace(nom[0].acc_type_cd.ToString()) ? string.Concat("'", nom[0].acc_type_cd, "'") : "acc_type_cd"
                                  );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }

            string _queryins = "INSERT INTO TD_NOMINEE (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                         + " VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                _statement = string.Format(_queryins,
                                             string.Concat("'", nom[i].ardb_cd, "'"),
                                             string.Concat("'", nom[i].brn_cd, "'"),
                                             nom[i].acc_type_cd,
                                             string.Concat("'", nom[i].acc_num, "'"),
                                             nom[i].nom_id,
                                             string.Concat("'", nom[i].nom_name, "'"),
                                             string.Concat("'", nom[i].nom_addr1, "'"),
                                             string.Concat("'", nom[i].nom_addr2, "'"),
                                             string.Concat("'", nom[i].phone_no, "'"),
                                             nom[i].percentage,
                                             string.Concat("'", nom[i].relation, "'")
                                              );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }

                internal bool UpdateSignatory(DbConnection connection, List<td_signatory> sig)
        {
            string _queryd=" DELETE FROM TD_SIGNATORY  "
             +" WHERE ardb_cd={0} AND acc_num = {1} AND acc_type_cd={2}";

                     _statement = string.Format(_queryd,
                                          !string.IsNullOrWhiteSpace(sig[0].ardb_cd) ? string.Concat("'", sig[0].ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(sig[0].acc_num) ? string.Concat("'", sig[0].acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(sig[0].acc_type_cd.ToString()) ? string.Concat("'", sig[0].acc_type_cd, "'") : "acc_type_cd"
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                           
                        }
                        

            string _queryins = "INSERT INTO TD_SIGNATORY ( ardb_cd,brn_cd, acc_type_cd, acc_num, signatory_name,del_flag) "
                          + " VALUES( {0},{1},{2},{3},{4},'N') ";
                          
            for (int i = 0; i < sig.Count; i++)
            {
                
                _statement = string.Format(_queryins,
                                                       string.Concat("'", sig[i].ardb_cd, "'"),
                                                       string.Concat("'", sig[i].brn_cd, "'"),
                                                       sig[i].acc_type_cd,
                                                       string.Concat("'", sig[i].acc_num, "'"),
                                                       string.Concat("'", sig[i].signatory_name, "'")
                                                        );

                
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }


        internal bool UpdateAccholder(DbConnection connection, List<td_accholder> acc)
        {
             string _queryd=" DELETE FROM td_accholder "
             +" WHERE ardb_cd={0} AND acc_num = {1} AND  ACC_TYPE_CD = {2}";

                     _statement = string.Format(_queryd,
                                          !string.IsNullOrWhiteSpace(acc[0].ardb_cd) ? string.Concat("'", acc[0].ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(acc[0].acc_num) ? string.Concat("'", acc[0].acc_num, "'") : "acc_num",
                                          (acc[0].acc_type_cd > 0) ? acc[0].acc_type_cd.ToString() : "ACC_TYPE_CD"
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                           
                        }
            
             string _queryins = "INSERT INTO TD_ACCHOLDER ( ardb_cd,brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,del_flag ) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N' ) ";

            for (int i = 0; i < acc.Count; i++)
            {
                
                    _statement = string.Format(_queryins,
                                                       string.Concat("'", acc[i].ardb_cd, "'"),
                                                       string.Concat("'", acc[i].brn_cd, "'"),
                                                       acc[i].acc_type_cd,
                                                       string.Concat("'", acc[i].acc_num, "'"),
                                                       string.Concat("'", acc[i].acc_holder, "'"),
                                                       string.Concat("'", acc[i].relation, "'"),
                                                       acc[i].cust_cd
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            return true;
        }
    }
}

