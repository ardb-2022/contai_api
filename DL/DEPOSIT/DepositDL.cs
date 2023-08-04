using System;
using System.Data;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;
using Oracle.ManagedDataAccess.Client;

namespace SBWSDepositApi.Deposit
{
    public class DepositDL
    {
        string _statement;
        internal List<tm_deposit> GetDepositTemp(tm_deposit dep)
        {
            List<tm_deposit> depoList = new List<tm_deposit>();

            string _query = " SELECT BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD                                   "
                            + " FROM TM_DEPOSIT_TEMP                                                                  "
                            + " WHERE BRN_CD={0} AND ACC_NUM={1}   AND ACC_TYPE_CD = {2}   ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
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
                                    var d = new tm_deposit();
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

                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }        

        internal decimal InsertDepositTemp(tm_deposit dep)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = " INSERT INTO TM_DEPOSIT_TEMP ( BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                           + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                           + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                           + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                           + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                           + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD,ARDB_CD,DEL_FLAG   "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},to_date('{8}','dd-mm-yyyy' ),{9},{10},{11},{12},{13}, to_date('{14}','dd-mm-yyyy' ),"
                           + " {15},{16}, to_date('{17}','dd-mm-yyyy' ), to_date('{18}','dd-mm-yyyy' ),{19},{20},{21},{22},{23},{24}, "
                           + " {25},{26},{27},{28},{29}, to_date('{30}','dd-mm-yyyy' ),{31}, to_date('{32}','dd-mm-yyyy' ),{33}, "
                           + " to_date('{34}','dd-mm-yyyy' ),{35},{36}, to_date('{37}','dd-mm-yyyy' ),{38},{39},{40},{41},{42},{43},to_date('{44}','dd-mm-yyyy' ),{45},{46},'N')";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_type_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.renew_id, "'"),
                       string.Concat("'", dep.cust_cd, "'"),
                       string.Concat("'", dep.intt_trf_type, "'"),
                       string.Concat("'", dep.constitution_cd, "'"),
                       string.Concat("'", dep.oprn_instr_cd, "'"),
                       string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? null : string.Concat("'", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.prn_amt, "'"),
                       string.Concat("'", dep.intt_amt, "'"),
                       string.Concat("'", dep.dep_period, "'"),
                       string.Concat("'", dep.instl_amt, "'"),
                       string.Concat("'", dep.instl_no, "'"),
                       string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? null : string.Concat("'", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.intt_rt, "'"),
                       string.Concat("'", dep.tds_applicable, "'"),
                       string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? null : string.Concat("'", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? null : string.Concat("'", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "'"),
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
                       string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? null : string.Concat("'", dep.created_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.modified_by, "'"),
                       string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? null : string.Concat("'", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.approval_status, "'"),
                       string.Concat("'", dep.approved_by, "'"),
                       string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? null : string.Concat("'", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.user_acc_num, "'"),
                       string.Concat("'", dep.lock_mode, "'"),
                       string.Concat("'", dep.loan_id, "'"),
                       string.Concat("'", dep.cert_no, "'"),
                       string.Concat("'", dep.bonus_amt, "'"),
                       string.Concat("'", dep.penal_intt_rt, "'"),
                       string.Concat("'", dep.bonus_intt_rt, "'"),
                       string.Concat("'", dep.transfer_flag, "'"),
                       string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? null : string.Concat("'", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.agent_cd, "'"),
                        string.Concat("'", dep.ardb_cd, "'")
                                                    );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }

                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }
                }
            }

            return 0;
        }
        internal int UpdateDepositTemp(tm_deposit dep)
        {
            int _ret = 0;

            string _query = " UPDATE TM_DEPOSIT_TEMP "
               + "brn_cd               = NVL({0},  brn_cd              ),"
               + "acc_type_cd          = NVL({1},  acc_type_cd         ),"
               + "acc_num              = NVL({2},  acc_num             ),"
               + "renew_id             = NVL({3},  renew_id            ),"
               + "cust_cd              = NVL({4},  cust_cd             ),"
               + "intt_trf_type        = NVL({5},  intt_trf_type       ),"
               + "constitution_cd      = NVL({6},  constitution_cd     ),"
               + "oprn_instr_cd        = NVL({7},  oprn_instr_cd       ),"
               + "opening_dt           = NVL(to_date('{8}','dd-mm-yyyy' ),  opening_dt ),"
               + "prn_amt              = NVL({9},  prn_amt             ),"
               + "intt_amt             = NVL({10}, intt_amt            ),"
               + "dep_period           = NVL({11}, dep_period          ),"
               + "instl_amt            = NVL({12}, instl_amt           ),"
               + "instl_no             = NVL({13}, instl_no            ),"
               + "mat_dt               = NVL(to_date('{14}','dd-mm-yyyy' ), mat_dt ),"
               + "intt_rt              = NVL({15}, intt_rt             ),"
               + "tds_applicable       = NVL({16}, tds_applicable      ),"
               + "last_intt_calc_dt    = NVL(to_date('{17}','dd-mm-yyyy' ), last_intt_calc_dt ),"
               + "acc_close_dt         = NVL(to_date('{18}','dd-mm-yyyy' ), acc_close_dt ),"
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
               + "created_dt           = NVL(to_date('{30}','dd-mm-yyyy' ), created_dt ),"
               + "modified_by          = NVL({31}, modified_by         ),"
               + "modified_dt          = NVL(to_date({32}','dd-mm-yyyy' ), modified_dt ),"
               + "approval_status      = NVL({33}, approval_status     ),"
               + "approved_by          = NVL({34}, approved_by         ),"
               + "approved_dt          = NVL(to_date('{35}','dd-mm-yyyy' ), approved_dt ),"
               + "user_acc_num         = NVL({36}, user_acc_num        ),"
               + "lock_mode            = NVL({37}, lock_mode           ),"
               + "loan_id              = NVL({38}, loan_id             ),"
               + "cert_no              = NVL({39}, cert_no             ),"
               + "bonus_amt            = NVL({40}, bonus_amt           ),"
               + "penal_intt_rt        = NVL({41}, penal_intt_rt       ),"
               + "bonus_intt_rt        = NVL({42}, bonus_intt_rt       ),"
               + "transfer_flag        = NVL({43}, transfer_flag       ),"
               + "transfer_dt          = NVL(to_date('{44}','dd-mm-yyyy' ), transfer_dt ),"
               + "agent_cd             = NVL({45}, agent_cd            ) "
               + "WHERE brn_cd = NVL({46}, brn_cd) AND acc_num = NVL('{47}',  acc_num )";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_type_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.renew_id, "'"),
                       string.Concat("'", dep.cust_cd, "'"),
                       string.Concat("'", dep.intt_trf_type, "'"),
                       string.Concat("'", dep.constitution_cd, "'"),
                       string.Concat("'", dep.oprn_instr_cd, "'"),
                       string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? null : string.Concat("'", dep.opening_dt.Value.ToString("dd/MM/yyyy"), ","),
                       string.Concat("'", dep.prn_amt, "'"),
                       string.Concat("'", dep.intt_amt, "'"),
                       string.Concat("'", dep.dep_period, "'"),
                       string.Concat("'", dep.instl_amt, "'"),
                       string.Concat("'", dep.instl_no, "'"),
                       string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? null : string.Concat("'", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.intt_rt, "'"),
                       string.Concat("'", dep.tds_applicable, "'"),
                       string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? null : string.Concat("'", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? null : string.Concat("'", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "'"),
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
                       string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? null : string.Concat("'", dep.created_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.modified_by, "'"),
                       string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? null : string.Concat("'", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.approval_status, "'"),
                       string.Concat("'", dep.approved_by, "'"),
                       string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? null : string.Concat("'", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.user_acc_num, "'"),
                       string.Concat("'", dep.lock_mode, "'"),
                       string.Concat("'", dep.loan_id, "'"),
                       string.Concat("'", dep.cert_no, "'"),
                       string.Concat("'", dep.bonus_amt, "'"),
                       string.Concat("'", dep.penal_intt_rt, "'"),
                       string.Concat("'", dep.bonus_intt_rt, "'"),
                       string.Concat("'", dep.transfer_flag, "'"),
                       string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? null : string.Concat("'", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.agent_cd, "'"),
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_num, "'")
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
        internal int DeleteDepositTemp(tm_deposit dep)
        {
            int _ret = 0;

            string _query = " UPDATE TM_DEPOSIT_TEMP SET DEL_FLAG = 'Y'  "
                          + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND ACC_TYPE_CD={3} and DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                             !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                             !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                             !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                             dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
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
        internal List<tm_deposit> GetDeposit(tm_deposit dep)
        {
            List<tm_deposit> depoList = new List<tm_deposit>();

            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT                                                                  "
                            + " WHERE ARDB_CD = {0} AND ACC_NUM={1} AND ACC_TYPE_CD={2} and DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
                                           );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        try
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

                                        depoList.Add(d);
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

            return depoList;
        }
        internal decimal InsertDeposit(tm_deposit dep)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = " INSERT INTO TM_DEPOSIT ( BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                           + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                           + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                           + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                           + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                           + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD, ARDB_CD, DEL_FLAG  "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},to_date('{8}','dd-mm-yyyy' ),{9},{10},{11},{12},{13}, to_date('{14}','dd-mm-yyyy' ),"
                           + " {15},{16}, to_date('{17}','dd-mm-yyyy' ), to_date('{18}','dd-mm-yyyy' ),{19},{20},{21},{22},{23},{24}, "
                           + " {25},{26},{27},{28},{29}, to_date('{30}','dd-mm-yyyy' ),{31}, to_date('{32}','dd-mm-yyyy' ),{33}, "
                           + " to_date('{34}','dd-mm-yyyy' ),{35},{36}, to_date('{37}','dd-mm-yyyy' ),{38},{39},{40},{41},{42},{43},to_date('{44}','dd-mm-yyyy' ),{45},{46},'N')";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_type_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.renew_id, "'"),
                       string.Concat("'", dep.cust_cd, "'"),
                       string.Concat("'", dep.intt_trf_type, "'"),
                       string.Concat("'", dep.constitution_cd, "'"),
                       string.Concat("'", dep.oprn_instr_cd, "'"),
                       string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? null : string.Concat("'", dep.opening_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.prn_amt, "'"),
                       string.Concat("'", dep.intt_amt, "'"),
                       string.Concat("'", dep.dep_period, "'"),
                       string.Concat("'", dep.instl_amt, "'"),
                       string.Concat("'", dep.instl_no, "'"),
                       string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? null : string.Concat("'", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.intt_rt, "'"),
                       string.Concat("'", dep.tds_applicable, "'"),
                       string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? null : string.Concat("'", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? null : string.Concat("'", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "'"),
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
                       string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? null : string.Concat("'", dep.created_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.modified_by, "'"),
                       string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? null : string.Concat("'", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.approval_status, "'"),
                       string.Concat("'", dep.approved_by, "'"),
                       string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? null : string.Concat("'", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.user_acc_num, "'"),
                       string.Concat("'", dep.lock_mode, "'"),
                       string.Concat("'", dep.loan_id, "'"),
                       string.Concat("'", dep.cert_no, "'"),
                       string.Concat("'", dep.bonus_amt, "'"),
                       string.Concat("'", dep.penal_intt_rt, "'"),
                       string.Concat("'", dep.bonus_intt_rt, "'"),
                       string.Concat("'", dep.transfer_flag, "'"),
                       string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? null : string.Concat("'", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.agent_cd, "'"),
                       string.Concat("'", dep.ardb_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }

                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }
                }
            }

            return 0;
        }
        internal int UpdateDeposit(tm_deposit dep)
        {
            int _ret = 0;

            string _query = " UPDATE TM_DEPOSIT SET "
               + "brn_cd               = NVL({0},  brn_cd              ),"
               + "acc_type_cd          = NVL({1},  acc_type_cd         ),"
               + "acc_num              = NVL({2},  acc_num             ),"
               + "renew_id             = NVL({3},  renew_id            ),"
               + "cust_cd              = NVL({4},  cust_cd             ),"
               + "intt_trf_type        = NVL({5},  intt_trf_type       ),"
               + "constitution_cd      = NVL({6},  constitution_cd     ),"
               + "oprn_instr_cd        = NVL({7},  oprn_instr_cd       ),"
               + "opening_dt           = NVL(to_date('{8}','dd-mm-yyyy' ),  opening_dt ),"
               + "prn_amt              = NVL({9},  prn_amt             ),"
               + "intt_amt             = NVL({10}, intt_amt            ),"
               + "dep_period           = NVL({11}, dep_period          ),"
               + "instl_amt            = NVL({12}, instl_amt           ),"
               + "instl_no             = NVL({13}, instl_no            ),"
               + "mat_dt               = NVL(to_date('{14}','dd-mm-yyyy' ), mat_dt ),"
               + "intt_rt              = NVL({15}, intt_rt             ),"
               + "tds_applicable       = NVL({16}, tds_applicable      ),"
               + "last_intt_calc_dt    = NVL(to_date('{17}','dd-mm-yyyy' ), last_intt_calc_dt ),"
               + "acc_close_dt         = NVL(to_date('{18}','dd-mm-yyyy' ), acc_close_dt ),"
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
               + "created_dt           = NVL(to_date('{30}','dd-mm-yyyy' ), created_dt ),"
               + "modified_by          = NVL({31}, modified_by         ),"
               + "modified_dt          = NVL(to_date('{32}','dd-mm-yyyy' ), modified_dt ),"
               + "approval_status      = NVL({33}, approval_status     ),"
               + "approved_by          = NVL({34}, approved_by         ),"
               + "approved_dt          = NVL(to_date('{35}','dd-mm-yyyy' ), approved_dt ),"
               + "user_acc_num         = NVL({36}, user_acc_num        ),"
               + "lock_mode            = NVL({37}, lock_mode           ),"
               + "loan_id              = NVL({38}, loan_id             ),"
               + "cert_no              = NVL({39}, cert_no             ),"
               + "bonus_amt            = NVL({40}, bonus_amt           ),"
               + "penal_intt_rt        = NVL({41}, penal_intt_rt       ),"
               + "bonus_intt_rt        = NVL({42}, bonus_intt_rt       ),"
               + "transfer_flag        = NVL({43}, transfer_flag       ),"
               + "transfer_dt          = NVL(to_date('{44}','dd-mm-yyyy' ), transfer_dt ),"
               + "agent_cd             = NVL({45}, agent_cd            ) "
               + "WHERE ardb_cd = {46} AND brn_cd = NVL({47}, brn_cd) AND acc_num = NVL({48},  acc_num ) AND ACC_TYPE_CD={49} AND DEL_FLAG = 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_type_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.renew_id, "'"),
                       string.Concat("'", dep.cust_cd, "'"),
                       string.Concat("'", dep.intt_trf_type, "'"),
                       string.Concat("'", dep.constitution_cd, "'"),
                       string.Concat("'", dep.oprn_instr_cd, "'"),
                       string.IsNullOrWhiteSpace(dep.opening_dt.ToString()) ? null : string.Concat("'", dep.opening_dt.Value.ToString("dd/MM/yyyy"), ","),
                       string.Concat("'", dep.prn_amt, "'"),
                       string.Concat("'", dep.intt_amt, "'"),
                       string.Concat("'", dep.dep_period, "'"),
                       string.Concat("'", dep.instl_amt, "'"),
                       string.Concat("'", dep.instl_no, "'"),
                       string.IsNullOrWhiteSpace(dep.mat_dt.ToString()) ? null : string.Concat("'", dep.mat_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.intt_rt, "'"),
                       string.Concat("'", dep.tds_applicable, "'"),
                       string.IsNullOrWhiteSpace(dep.last_intt_calc_dt.ToString()) ? null : string.Concat("'", dep.last_intt_calc_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.IsNullOrWhiteSpace(dep.acc_close_dt.ToString()) ? null : string.Concat("'", dep.acc_close_dt.Value.ToString("dd/MM/yyyy"), "'"),
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
                       string.IsNullOrWhiteSpace(dep.created_dt.ToString()) ? null : string.Concat("'", dep.created_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.modified_by, "'"),
                       string.IsNullOrWhiteSpace(dep.modified_dt.ToString()) ? null : string.Concat("'", dep.modified_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.approval_status, "'"),
                       string.Concat("'", dep.approved_by, "'"),
                       string.IsNullOrWhiteSpace(dep.approved_dt.ToString()) ? null : string.Concat("'", dep.approved_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.user_acc_num, "'"),
                       string.Concat("'", dep.lock_mode, "'"),
                       string.Concat("'", dep.loan_id, "'"),
                       string.Concat("'", dep.cert_no, "'"),
                       string.Concat("'", dep.bonus_amt, "'"),
                       string.Concat("'", dep.penal_intt_rt, "'"),
                       string.Concat("'", dep.bonus_intt_rt, "'"),
                       string.Concat("'", dep.transfer_flag, "'"),
                       string.IsNullOrWhiteSpace(dep.transfer_dt.ToString()) ? null : string.Concat("'", dep.transfer_dt.Value.ToString("dd/MM/yyyy"), "'"),
                       string.Concat("'", dep.agent_cd, "'"),
                       string.Concat("'", dep.ardb_cd, "'"),
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD", "'")
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
        internal int DeleteDeposit(tm_deposit dep)
        {
            int _ret = 0;

            string _query = " UPDATE TM_DEPOSIT SET DEL_FLAG = 'Y'  "
                          + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND ACC_TYPE_CD ={3} ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                             !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                             !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                             !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                             dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
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
        internal List<tm_deposit> GetDepositView(tm_deposit dep)
        {
            List<tm_deposit> depoList = new List<tm_deposit>();

            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE,  CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT, AGENT_CD,DEL_FLAG                                   "
                            + " FROM V_DEPOSIT                                                                  "
                            + " WHERE ARDB_CD = {0} AND ACC_TYPE_CD ={1} AND ACC_NUM={2}  AND DEL_FLAG = 'N'  "
                            + " AND   RENEW_ID = (SELECT MAX(RENEW_ID) FROM  V_DEPOSIT WHERE ARDB_CD = {3} AND ACC_TYPE_CD ={4} AND ACC_NUM={5}  AND DEL_FLAG = 'N')                                                      ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                           dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                           dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num"
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
                                    d.cert_no = UtilityM.CheckNull<string>(reader["CERT_NO"]);
                                    d.bonus_amt = UtilityM.CheckNull<decimal>(reader["BONUS_AMT"]);
                                    d.penal_intt_rt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RT"]);
                                    d.bonus_intt_rt = UtilityM.CheckNull<decimal>(reader["BONUS_INTT_RT"]);
                                    d.transfer_flag = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                    d.transfer_dt = UtilityM.CheckNull<DateTime>(reader["TRANSFER_DT"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                    d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }

        internal List<tm_depositall> GetDepositWithChild(tm_depositall dep)
        {
            List<tm_depositall> depoList = new List<tm_depositall>();

            string _query = " SELECT TD.ARDB_CD ARDB_CD,TD.BRN_CD BRN_CD, TD.ACC_TYPE_CD ACC_TYPE_CD, TD.ACC_NUM ACC_NUM, TD.RENEW_ID RENEW_ID, TD.CUST_CD CUST_CD, TD.INTT_TRF_TYPE INTT_TRF_TYPE, TD.CONSTITUTION_CD CONSTITUTION_CD, "
                            + " TD.OPRN_INSTR_CD OPRN_INSTR_CD, TD.OPENING_DT OPENING_DT, TD.PRN_AMT PRN_AMT, TD.INTT_AMT INTT_AMT, TD.DEP_PERIOD DEP_PERIOD, TD.INSTL_AMT INSTL_AMT, TD.INSTL_NO INSTL_NO,   "
                            + " TD.MAT_DT MAT_DT, TD.INTT_RT INTT_RT, TD.TDS_APPLICABLE TDS_APPLICABLE, TD.LAST_INTT_CALC_DT LAST_INTT_CALC_DT, TD.ACC_CLOSE_DT ACC_CLOSE_DT,                "
                            + " TD.CLOSING_PRN_AMT CLOSING_PRN_AMT, TD.CLOSING_INTT_AMT CLOSING_INTT_AMT, TD.PENAL_AMT PENAL_AMT, TD.EXT_INSTL_TOT EXT_INSTL_TOT, TD.MAT_STATUS MAT_STATUS, TD.ACC_STATUS ACC_STATUS, "
                            + " TD.CURR_BAL CURR_BAL, TD.CLR_BAL CLR_BAL, TD.STANDING_INSTR_FLAG STANDING_INSTR_FLAG, TD.CHEQUE_FACILITY_FLAG CHEQUE_FACILITY_FLAG,                         "
                            + " TD.CREATED_BY CREATED_BY, TD.CREATED_DT CREATED_DT, TD.MODIFIED_BY MODIFIED_BY, TD.MODIFIED_DT MODIFIED_DT, TD.APPROVAL_STATUS APPROVAL_STATUS, TD.APPROVED_BY APPROVED_BY,       "
                            + " TD.APPROVED_DT APPROVED_DT, TD.USER_ACC_NUM USER_ACC_NUM, TD.LOCK_MODE LOCK_MODE, TD.LOAN_ID LOAN_ID, TD.CERT_NO CERT_NO, TD.BONUS_AMT BONUS_AMT, TD.PENAL_INTT_RT PENAL_INTT_RT,     "
                            + " TD.BONUS_INTT_RT BONUS_INTT_RT, TD.TRANSFER_FLAG TRANSFER_FLAG, TD.TRANSFER_DT TRANSFER_DT, TD.AGENT_CD AGENT_CD,MC.CUST_NAME CUST_NAME,MC.PERMANENT_ADDRESS PERMANENT_ADDRESS,MC.PHONE PHONE,MC.GUARDIAN_NAME GUARDIAN_NAME,MC.DT_OF_BIRTH DT_OF_BIRTH,MC.SEX SEX,MC.CATG_CD CATG_CD,MC.OCCUPATION OCCUPATION,MC.PRESENT_ADDRESS PRESENT_ADDRESS,MC.CUST_TYPE CUST_TYPE,MC.AGE AGE  "
                           + " , MC.KYC_PHOTO_TYPE KYC_PHOTO_TYPE,MC.KYC_PHOTO_NO KYC_PHOTO_NO,MC.KYC_ADDRESS_TYPE KYC_ADDRESS_TYPE,MC.KYC_ADDRESS_NO KYC_ADDRESS_NO ,MCO.CONSTITUTION_DESC CONSTITUTION_DESC,MCO.CONSTITUTION_CD CONSTITUTION_CD,MCO.ACC_CD ACC_CD,MCO.INTT_ACC_CD INTT_ACC_CD,MCO.INTT_PROV_ACC_CD INTT_PROV_ACC_CD "
                            + " FROM TM_DEPOSIT TD,  MM_CUSTOMER MC,  MM_CONSTITUTION MCO   "
                            + " WHERE  TD.ARDB_CD={0} AND TD.ACC_NUM={1} AND TD.ACC_TYPE_CD={2} AND TD.ARDB_CD=MC.ARDB_CD AND TD.CUST_CD=MC.CUST_CD AND TD.CONSTITUTION_CD =MCO.CONSTITUTION_CD AND TD.ACC_TYPE_CD=MCO.ACC_TYPE_CD AND NVL(TD.ACC_STATUS,'O') = 'O' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",                                          
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD"
                                           );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var d = new tm_depositall();
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
                                        d.loan_id = Convert.ToString(UtilityM.CheckNull<decimal>(reader["LOAN_ID"]));
                                        d.cert_no = UtilityM.CheckNull<string>(reader["CERT_NO"]);
                                        d.bonus_amt = UtilityM.CheckNull<decimal>(reader["BONUS_AMT"]);
                                        d.penal_intt_rt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RT"]);
                                        d.bonus_intt_rt = UtilityM.CheckNull<decimal>(reader["BONUS_INTT_RT"]);
                                        d.transfer_flag = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                        d.transfer_dt = UtilityM.CheckNull<DateTime>(reader["TRANSFER_DT"]);
                                        d.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                        d.cust_type = UtilityM.CheckNull<string>(reader["CUST_TYPE"]);
                                        //d.title = UtilityM.CheckNull<string>(reader["TITLE"]);
                                        //d.first_name = UtilityM.CheckNull<string>(reader["FIRST_NAME"]);
                                        //d.middle_name = UtilityM.CheckNull<string>(reader["MIDDLE_NAME"]);
                                        //d.last_name = UtilityM.CheckNull<string>(reader["LAST_NAME"]);
                                        d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        d.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        //d.cust_dt = UtilityM.CheckNull<DateTime>(reader["CUST_DT"]);
                                        d.dt_of_birth = UtilityM.CheckNull<DateTime>(reader["DT_OF_BIRTH"]);
                                        d.age = UtilityM.CheckNull<decimal>(reader["AGE"]);
                                        d.sex = UtilityM.CheckNull<string>(reader["SEX"]);
                                        //d.marital_status = UtilityM.CheckNull<string>(reader["MARITAL_STATUS"]);
                                        d.catg_cd = UtilityM.CheckNull<int>(reader["CATG_CD"]);
                                        //d.community = UtilityM.CheckNull<decimal>(reader["COMMUNITY"]);
                                        //d.caste = UtilityM.CheckNull<decimal>(reader["CASTE"]);
                                        d.permanent_address = UtilityM.CheckNull<string>(reader["PERMANENT_ADDRESS"]);
                                        //d.ward_no = UtilityM.CheckNull<decimal>(reader["WARD_NO"]);
                                        //d.state = UtilityM.CheckNull<string>(reader["STATE"]);
                                        //d.dist = UtilityM.CheckNull<string>(reader["DIST"]);
                                        //d.pin = UtilityM.CheckNull<int>(reader["PIN"]);
                                        //d.vill_cd = UtilityM.CheckNull<string>(reader["VILL_CD"]);
                                        //d.block_cd = UtilityM.CheckNull<string>(reader["BLOCK_CD"]);
                                        //d.service_area_cd = UtilityM.CheckNull<string>(reader["SERVICE_AREA_CD"]);
                                        d.occupation = UtilityM.CheckNull<string>(reader["OCCUPATION"]);
                                        d.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        d.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        d.kyc_photo_type = UtilityM.CheckNull<string>(reader["KYC_PHOTO_TYPE"]);
                                        d.kyc_photo_no = UtilityM.CheckNull<string>(reader["KYC_PHOTO_NO"]);
                                        d.kyc_address_type = UtilityM.CheckNull<string>(reader["KYC_ADDRESS_TYPE"]);
                                        d.kyc_address_no = UtilityM.CheckNull<string>(reader["KYC_ADDRESS_NO"]);
                                        d.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                        d.constitution_cd = UtilityM.CheckNull<int>(reader["CONSTITUTION_CD"]);
                                        d.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        d.intt_acc_cd = UtilityM.CheckNull<int>(reader["INTT_ACC_CD"]);
                                        d.intt_prov_acc_cd = UtilityM.CheckNull<int>(reader["INTT_PROV_ACC_CD"]);

                                        depoList.Add(d);
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

            return depoList;
        }

        internal string ApproveAccountTranaction(p_gen_param pgp)
        {
            string _ret = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        //DepTransactionDL _dl1 = new DepTransactionDL();
                        //_ret = _dl1.P_UPDATE_TD_DEP_TRANS(connection, pgp);
                        //if (_ret == "0")
                        //{
                           // DenominationDL _dl2 = new DenominationDL();
                           // _ret = _dl2.P_UPDATE_DENOMINATION(connection, pgp);
                            //if (_ret == "0")
                            //{
                                string _query1 = "UPDATE TD_DEP_TRANS SET "
                                              + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                              + " MODIFIED_DT            =SYSDATE,"
                                              + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                              + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                              + " APPROVED_DT            =SYSDATE"
                                              + " WHERE (ARDB_CD = {3}) AND (BRN_CD = {4}) AND "
                                              + " (TRANS_DT = to_date('{5}','dd-mm-yyyy' )) AND  "
                                              + " (  TRANS_CD = {6} ) ";
                                _statement = string.Format(_query1,
                                        string.Concat("'", pgp.gs_user_id, "'"),
                                        string.Concat("'", "A", "'"),
                                        string.Concat("'", pgp.gs_user_id, "'"),
                                        string.Concat("'", pgp.ardb_cd, "'"),
                                        string.Concat("'", pgp.brn_cd, "'"),
                                        string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                        string.Concat(pgp.ad_trans_cd)
                                        );
                                using (var command = OrclDbConnection.Command(connection, _statement))
                                {
                                    command.ExecuteNonQuery();
                                }
                                string _query2 = "UPDATE TD_DEP_TRANS_TRF SET "
                                          + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                          + " MODIFIED_DT            =SYSDATE,"
                                          + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                          + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                          + " APPROVED_DT            =SYSDATE"
                                          + " WHERE (ARDB_CD = {3}) AND (BRN_CD = {4}) AND "
                                          + " (TRANS_DT = to_date('{5}','dd-mm-yyyy' )) AND  "
                                          + " (  TRANS_CD = {6} ) ";
                                _statement = string.Format(_query2,
                                        string.Concat("'", pgp.gs_user_id, "'"),
                                        string.Concat("'", "A", "'"),
                                        string.Concat("'", pgp.gs_user_id, "'"),
                                        string.Concat("'", pgp.ardb_cd, "'"),
                                        string.Concat("'", pgp.brn_cd, "'"),
                                        string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                        string.Concat(pgp.ad_trans_cd)
                                        );
                                using (var command = OrclDbConnection.Command(connection, _statement))
                                {
                                    command.ExecuteNonQuery();
                                }
                                string _query = "UPDATE TM_TRANSFER SET "
                                          + " APPROVAL_STATUS        =NVL({0},APPROVAL_STATUS),"
                                          + " APPROVED_BY            =NVL({1},APPROVED_BY    ),"
                                          + " APPROVED_DT            =SYSDATE"
                                          + " WHERE (ARDB_CD = {2}) AND (BRN_CD = {3}) AND "
                                          + " (TRF_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                          + " (  TRANS_CD = {5} ) ";
                                _statement = string.Format(_query,
                                        string.Concat("'", "A", "'"),
                                        string.Concat("'", pgp.gs_user_id, "'"),
                                        string.Concat("'", pgp.ardb_cd, "'"),
                                        string.Concat("'", pgp.brn_cd, "'"),
                                        string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                        string.Concat(pgp.ad_trans_cd)
                                        );
                                using (var command = OrclDbConnection.Command(connection, _statement))
                                {
                                    command.ExecuteNonQuery();
                                }

                        DepTransactionDL _dl1 = new DepTransactionDL();
                        _ret = _dl1.P_UPDATE_TD_DEP_TRANS(connection, pgp);
                        if (_ret == "0")
                        {

                            transaction.Commit();
                                return "0";

                            }
                            else
                            {
                                transaction.Rollback();
                                return _ret;
                            }
                        //}
                        //else
                        //{
                        //    transaction.Rollback();
                        //    return _ret;
                        //}
                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return ex.Message.ToString();
                    }

                }
            }
        }



 

        internal int isDormantAccount(tm_deposit dep)
        {
            int d = 0;

            string _query = " select  count(*)  CNT "
+ " from v_trans_dtls"
+ " where ardb_cd ={0} and  acc_type_cd IN (1,8)                                 "
+ " and acc_num = {1}                                  "
+ " and lower(particulars) not like '%interest%'       "
+ " and lower(particulars) not like '%insurance%'      "
+ " and lower(particulars) not like '%sms%' "
+ " and lower(particulars) not like '%dividend%'       "
+ " and lower(particulars) not like '%passbook%'       "
+ " and lower(particulars) not like '%insurance%'      "
+ " and lower(particulars) not like '%divident%' "
+ " and lower(particulars) not like '%TO SMS CHARGES%' "
+ " and lower(particulars) not like '%TO ATM CHARGES%' "
+ " and to_date(trans_dt,'dd/mm/yyyy') > to_date(to_date(f_getparamval('206'),'dd/mm/yyyy') - 365,'dd/mm/yyyy')  ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num"
                                           );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {

                                        d = (int)UtilityM.CheckNull<decimal>(reader["CNT"]);
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

            return d;
        }

        internal List<td_def_trans_trf> GetPrevTransaction(tm_deposit dep)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT V_TRANS_DTLS.TRANS_DT,V_TRANS_DTLS.TRANS_MODE, "
+ "V_TRANS_DTLS.ACC_TYPE_CD, "
+ "V_TRANS_DTLS.ACC_NUM, "
+ "V_TRANS_DTLS.TRANS_TYPE, "
+ "V_TRANS_DTLS.INSTRUMENT_NUM, "
+ "V_TRANS_DTLS.AMOUNT, "
+ "V_TRANS_DTLS.PARTICULARS, "
+ "V_TRANS_DTLS.TRANS_CD , "
+ "GM_SB_BALANCE.BALANCE_AMT "
+ " FROM V_TRANS_DTLS V_TRANS_DTLS, "
+ " GM_SB_BALANCE GM_SB_BALANCE "
+ " WHERE  V_TRANS_DTLS.ARDB_CD = GM_SB_BALANCE.ARDB_CD  and " 
+ "  V_TRANS_DTLS.ACC_TYPE_CD = GM_SB_BALANCE.ACC_TYPE_CD  and "
+ "  V_TRANS_DTLS.ACC_NUM = GM_SB_BALANCE.ACC_NUM  and "
+ "  GM_SB_BALANCE.BALANCE_DT = (SELECT MAX(BALANCE_DT) "
+ " FROM   GM_SB_BALANCE "
+ " WHERE ARDB_CD = {0} AND  ACC_TYPE_CD = {1} "
+ " AND ACC_NUM = {2} "
+ " AND BALANCE_DT  <> to_date(f_getparamval('779'),'dd/mm/yyyy hh24:mi:ss'))  "
+ " AND  V_TRANS_DTLS.ACC_TYPE_CD = {3}  AND "
+ "  V_TRANS_DTLS.ACC_NUM = {4}  AND "
+ "  V_TRANS_DTLS.APPROVAL_STATUS IN  ({5},{6})  AND "
+ " V_TRANS_DTLS.TRANS_TYPE <> {7} ORDER BY  V_TRANS_DTLS.TRANS_DT DESC,V_TRANS_DTLS.TRANS_CD DESC";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                            dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD",
                                            !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                            dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD",
                                            !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                            string.Concat("'", "A", "'"),
                                            string.Concat("'", "B", "'"),
                                            string.Concat("'", "I", "'")
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var tdtr = new td_def_trans_trf();
                                        tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                                        tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                                        tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                        tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                                        tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                        tdtr.remarks = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                        tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["BALANCE_AMT"]);
                                        tdtRets.Add(tdtr);
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

            return tdtRets;
        }
        internal int UpdateDepositLockUnlock(tm_deposit dep)
        {
            int _ret = 0;

            string _query = " UPDATE TM_DEPOSIT SET "
               + " modified_by          = NVL({0}, modified_by ),"
               + " modified_dt          = SYSDATE,"
               + " lock_mode            = NVL({1}, lock_mode ),"
               + " loan_id              = {2} "
               + " WHERE ardb_cd = {3} AND brn_cd = NVL({4}, brn_cd) "
               + " AND acc_num = {5} "
               + " AND ACC_TYPE_CD = {6} AND DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                        string.Concat("'", dep.modified_by, "'"),
                        string.Concat("'", dep.lock_mode, "'"),
                        string.Concat("'", dep.loan_id, "'"),
                        string.Concat("'", dep.ardb_cd, "'"),
                        string.Concat("'", dep.brn_cd, "'"),
                        string.Concat("'", dep.acc_num, "'"),
                        string.Concat("'", dep.acc_type_cd != 0 ? Convert.ToString(dep.acc_type_cd) : "ACC_TYPE_CD", "'")
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

        internal AccOpenDM GetDepositAddlInfo(tm_deposit td)
        {
            AccOpenDM AccOpenDMRet = new AccOpenDM();

            try
            {

                IntroducerDL introducerDL = new IntroducerDL();
                td_introducer td_Introducer = new td_introducer();
                td_Introducer.acc_num = td.acc_num;
                td_Introducer.acc_type_cd = td.acc_type_cd;
                td_Introducer.brn_cd = td.brn_cd;
                td_Introducer.ardb_cd = td.ardb_cd;

                AccOpenDMRet.tdintroducer = introducerDL.GetIntroducer(td_Introducer);

                NomineeDL nomineeDL = new NomineeDL();
                td_nominee td_Nominee = new td_nominee();
                td_Nominee.acc_num = td.acc_num;
                td_Nominee.acc_type_cd = td.acc_type_cd;
                td_Nominee.brn_cd = td.brn_cd;
                td_Nominee.ardb_cd = td.ardb_cd;

                AccOpenDMRet.tdnominee = nomineeDL.GetNominee(td_Nominee);

                SignatoryDL SignatoryDL = new SignatoryDL();
                td_signatory td_Signatory = new td_signatory();
                td_Signatory.acc_num = td.acc_num;
                td_Signatory.acc_type_cd = td.acc_type_cd;
                td_Signatory.brn_cd = td.brn_cd;
                td_Signatory.ardb_cd = td.ardb_cd;



                AccOpenDMRet.tdsignatory = SignatoryDL.GetSignatory(td_Signatory);

                AccholderDL accholderDL = new AccholderDL();
                td_accholder td_Accholder = new td_accholder();
                td_Accholder.acc_num = td.acc_num;
                td_Accholder.acc_type_cd = td.acc_type_cd;
                td_Accholder.brn_cd = td.brn_cd;
                td_Accholder.ardb_cd = td.ardb_cd;
                AccOpenDMRet.tdaccholder = accholderDL.GetAccholder(td_Accholder);
                return AccOpenDMRet;
            }
            catch (Exception ex)
            {
                // transaction.Rollback();
                return null;
            }


        }
        internal List<AccDtlsLov> GetAccDtls(p_gen_param prm)
        {
            List<AccDtlsLov> accDtlsLovs = new List<AccDtlsLov>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GET_ACC_DTLS"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.ardb_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("ad_acc_type_cd", OracleDbType.Decimal, ParameterDirection.Input);
                    parm.Value = prm.ad_acc_type_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_cust_name", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.as_cust_name;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("p_acc_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm);
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
                                        accDtlsLov.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        accDtlsLov.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        accDtlsLov.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        accDtlsLov.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        accDtlsLov.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        accDtlsLov.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);

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

        internal List<mm_customer> GetCustDtls(p_gen_param prm)
        {
            List<mm_customer> customers = new List<mm_customer>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GET_CUST_DTLS"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.ardb_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_cust_name", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.as_cust_name;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("p_acc_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm);
                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var mc = new mm_customer();
                                        mc.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        mc.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        mc.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                        mc.cust_type = UtilityM.CheckNull<string>(reader["CUST_TYPE"]);
                                        mc.title = UtilityM.CheckNull<string>(reader["TITLE"]);
                                        mc.first_name = UtilityM.CheckNull<string>(reader["FIRST_NAME"]);
                                        mc.middle_name = UtilityM.CheckNull<string>(reader["MIDDLE_NAME"]);
                                        mc.last_name = UtilityM.CheckNull<string>(reader["LAST_NAME"]);
                                        mc.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        mc.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        mc.cust_dt = UtilityM.CheckNull<DateTime>(reader["CUST_DT"]);
                                        mc.old_cust_cd = UtilityM.CheckNull<string>(reader["OLD_CUST_CD"]);
                                        mc.dt_of_birth = UtilityM.CheckNull<DateTime>(reader["DT_OF_BIRTH"]);
                                        mc.age = UtilityM.CheckNull<decimal>(reader["AGE"]);
                                        mc.sex = UtilityM.CheckNull<string>(reader["SEX"]);
                                        mc.marital_status = UtilityM.CheckNull<string>(reader["MARITAL_STATUS"]);
                                        mc.catg_cd = UtilityM.CheckNull<int>(reader["CATG_CD"]);
                                        mc.community = UtilityM.CheckNull<decimal>(reader["COMMUNITY"]);
                                        mc.caste = UtilityM.CheckNull<decimal>(reader["CASTE"]);
                                        mc.permanent_address = UtilityM.CheckNull<string>(reader["PERMANENT_ADDRESS"]);
                                        mc.ward_no = UtilityM.CheckNull<decimal>(reader["WARD_NO"]);
                                        mc.state = UtilityM.CheckNull<string>(reader["STATE"]);
                                        mc.dist = UtilityM.CheckNull<string>(reader["DIST"]);
                                        mc.pin = UtilityM.CheckNull<int>(reader["PIN"]);
                                        mc.vill_cd = UtilityM.CheckNull<string>(reader["VILL_CD"]);
                                        mc.block_cd = UtilityM.CheckNull<string>(reader["BLOCK_CD"]);
                                        mc.ps = UtilityM.CheckNull<int>(reader["PS"]);
                                        mc.po = UtilityM.CheckNull<int>(reader["PO"]);
                                        mc.service_area_cd = UtilityM.CheckNull<string>(reader["SERVICE_AREA_CD"]);
                                        mc.occupation = UtilityM.CheckNull<string>(reader["OCCUPATION"]);
                                        mc.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        mc.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        mc.farmer_type = UtilityM.CheckNull<string>(reader["FARMER_TYPE"]);
                                        mc.email = UtilityM.CheckNull<string>(reader["EMAIL"]);
                                        mc.monthly_income = UtilityM.CheckNull<decimal>(reader["MONTHLY_INCOME"]);
                                        mc.date_of_death = UtilityM.CheckNull<DateTime>(reader["DATE_OF_DEATH"]);
                                        mc.sms_flag = UtilityM.CheckNull<string>(reader["SMS_FLAG"]);
                                        mc.status = UtilityM.CheckNull<string>(reader["STATUS"]);
                                        mc.pan = UtilityM.CheckNull<string>(reader["PAN"]);
                                        mc.nominee = UtilityM.CheckNull<string>(reader["NOMINEE"]);
                                        mc.nom_relation = UtilityM.CheckNull<string>(reader["NOM_RELATION"]);
                                        mc.kyc_photo_type = UtilityM.CheckNull<string>(reader["KYC_PHOTO_TYPE"]);
                                        mc.kyc_photo_no = UtilityM.CheckNull<string>(reader["KYC_PHOTO_NO"]);
                                        mc.kyc_address_type = UtilityM.CheckNull<string>(reader["KYC_ADDRESS_TYPE"]);
                                        mc.kyc_address_no = UtilityM.CheckNull<string>(reader["KYC_ADDRESS_NO"]);
                                        mc.kyc_other_type = UtilityM.CheckNull<string>(reader["KYC_OTHER_TYPE"]);
                                        mc.kyc_other_no = UtilityM.CheckNull<string>(reader["KYC_OTHER_NO"]);
                                        mc.org_status = UtilityM.CheckNull<string>(reader["ORG_STATUS"]);
                                        mc.org_reg_no = UtilityM.CheckNull<decimal>(reader["ORG_REG_NO"]);
                                        mc.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                        mc.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                        mc.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                        mc.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                        mc.lbr_status = UtilityM.CheckNull<string>(reader["LBR_STATUS"]);
                                        mc.is_weaker = UtilityM.CheckNull<string>(reader["IS_WEAKER"]);
                                        mc.sb_folio_no = UtilityM.CheckNull<string>(reader["SB_FOLIO_NO"]);
                                        mc.td_folio_no = UtilityM.CheckNull<string>(reader["TD_FOLIO_NO"]);
                                        mc.fd_folio_no = UtilityM.CheckNull<string>(reader["FD_FOLIO_NO"]);
                                        mc.rd_folio_no = UtilityM.CheckNull<string>(reader["RD_FOLIO_NO"]);
                                        mc.mis_folio_no = UtilityM.CheckNull<string>(reader["MIS_FOLIO_NO"]);
                                        mc.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                        customers.Add(mc);
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

            return customers;
        }




       internal List<standing_instr> GetStandingInstr()
        {
            List<standing_instr> standingInstrList = new List<standing_instr>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT A.ACC_TYPE_FROM ACC_TYPE_FROM, A.ACC_NUM_FROM  ACC_NUM_FROM, "
                            + " B.ACC_TYPE_TO   ACC_TYPE_TO,   B.ACC_NUM_TO    ACC_NUM_TO,          "
                            + " A.INSTR_STATUS  INSTR_STATUS,  A.FIRST_TRF_DT  FIRST_TRF_DT,        "
                            + " A.PERIODICITY   PERIODICITY,   A.PRN_INTT_FLAG PRN_INTT_FLAG,       "
                            + " A.AMOUNT        AMOUNT,        A.SRL_NO        SRL_NO               "
                            + " FROM SM_STANDING_INSTRUCTION A, SD_STANDING_INSTRUCTION B           "
                            + " WHERE NVL(A.INSTR_STATUS, 'O') = 'O' AND A.SRL_NO = B.SRL_NO        "
                            + " ORDER BY A.ACC_TYPE_FROM, A.ACC_NUM_FROM                            ";
   
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
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var stInstr = new standing_instr();

                                        stInstr.acc_type_from = UtilityM.CheckNull<string>(reader["ACC_TYPE_FROM"]);
                                        stInstr.acc_num_from = UtilityM.CheckNull<string>(reader["ACC_NUM_FROM"]);
                                        stInstr.acc_type_to = UtilityM.CheckNull<string>(reader["ACC_TYPE_TO"]);
                                        stInstr.acc_num_to = UtilityM.CheckNull<string>(reader["ACC_NUM_TO"]);
                                        stInstr.instr_status = UtilityM.CheckNull<string>(reader["INSTR_STATUS"]);
                                        stInstr.first_trf_dt = UtilityM.CheckNull<DateTime>(reader["FIRST_TRF_DT"]);
                                        stInstr.periodicity = UtilityM.CheckNull<string>(reader["PERIODICITY"]);
                                        stInstr.prn_intt_flag = UtilityM.CheckNull<string>(reader["PRN_INTT_FLAG"]);
                                        stInstr.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                        stInstr.srl_no = UtilityM.CheckNull<int>(reader["SRL_NO"]);

                                        standingInstrList.Add(stInstr);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        standingInstrList = null;
                    }
                }
            }
            return standingInstrList;
        }


        internal List<standing_instr_exe> GetStandingInstrExe(p_report_param prp)
        {
            List<standing_instr_exe> standingInstrExeList = new List<standing_instr_exe>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;
            string _query = " SELECT TT_EXECUTED_SI.TRANS_DT,   "
                             + " TT_EXECUTED_SI.DR_ACC_TYPE,     "
                             + " TT_EXECUTED_SI.DR_ACC_NUM,      "
                             + " TT_EXECUTED_SI.CUST_CD,         "
                             + " TT_EXECUTED_SI.CR_ACC_TYPE,     "
                             + " TT_EXECUTED_SI.CR_ACC_NUM,      "
                             + " TT_EXECUTED_SI.AMOUNT           "
                             + " FROM TT_EXECUTED_SI             "
                             + " WHERE TT_EXECUTED_SI.TRANS_DT = {0} ";

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
                                                    string.Concat("to_date('", prp.to_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var stInstrExe = new standing_instr_exe();

                                        stInstrExe.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        stInstrExe.dr_acc_type = UtilityM.CheckNull<Int32>(reader["DR_ACC_TYPE"]);
                                        stInstrExe.dr_acc_num = UtilityM.CheckNull<string>(reader["DR_ACC_NUM"]);
                                        stInstrExe.cust_cd = UtilityM.CheckNull<Int32>(reader["CUST_CD"]);
                                        stInstrExe.cr_acc_type = UtilityM.CheckNull<Int32>(reader["CR_ACC_TYPE"]);
                                        stInstrExe.cr_acc_num = UtilityM.CheckNull<string>(reader["CR_ACC_NUM"]);
                                        stInstrExe.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);

                                        standingInstrExeList.Add(stInstrExe);
                                    }
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        standingInstrExeList = null;
                    }
                }
            }
            return standingInstrExeList;
        }



        internal decimal GetInttRate(p_gen_param pmc)
        {
            decimal inttrate = 0 ;

            string _query = "Select f_get_intt_rate({0},{1},{2},{3}) INTT_RATE "
                            + " From   Dual ";
            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                            pmc.ardb_cd,
                                            pmc.acc_type_cd,
                                            pmc.catg_cd,
                                            pmc.no_of_days
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                inttrate = UtilityM.CheckNull<decimal>(reader["INTT_RATE"]);
                            }
                        }
                    }
                }

            }

            return inttrate;

        }


        internal decimal GetInttRatePremature(p_gen_param pmc)
        {
            decimal inttrate = 0;

            string _query = "Select f_get_intt_rate_premature({0},{1},{2},{3},{4}) INTT_RATE "
                            + " From   Dual ";
            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                            pmc.ardb_cd,
                                            pmc.acc_type_cd,
                                            pmc.ad_intt_rt,
                                            pmc.catg_cd,
                                            pmc.no_of_days
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                inttrate = UtilityM.CheckNull<decimal>(reader["INTT_RATE"]);
                            }
                        }
                    }
                }

            }

            return inttrate;

        }





        internal List<tm_deposit> GetDepositDtls(mm_customer pmc)
        {
            List<tm_deposit> depo = new List<tm_deposit>();
            string _query =  " SELECT TM_DEPOSIT.ACC_TYPE_CD, TM_DEPOSIT.ACC_NUM, "
                           + " Decode(TM_DEPOSIT.ACC_TYPE_CD, 1,TM_DEPOSIT.CLR_BAL,7,TM_DEPOSIT.CLR_BAL,8,TM_DEPOSIT.CLR_BAL,6, f_get_rd_prn (TM_DEPOSIT.ACC_NUM,SYSDATE),TM_DEPOSIT.PRN_AMT) Balance, "
                           + " TM_DEPOSIT.CUST_CD,MM_CUSTOMER.CUST_NAME,TM_DEPOSIT.ACC_STATUS,  "
                           + " TM_DEPOSIT.PRN_AMT PRN_AMT, TM_DEPOSIT.INTT_AMT INTT_AMT, "
                           + " TM_DEPOSIT.INSTL_AMT INSTL_AMT "
                           + " FROM  TM_DEPOSIT,MM_CUSTOMER   "
                           + " WHERE  TM_DEPOSIT.CUST_CD = MM_CUSTOMER.CUST_CD   "
                           + " And    TM_DEPOSIT.CUST_CD = {0}  "
                           + " And    TM_DEPOSIT.BRN_CD = {1}  "
                           + " Order By TM_DEPOSIT.ACC_TYPE_CD  ";
            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                            !string.IsNullOrWhiteSpace(pmc.cust_cd.ToString()) ? string.Concat("'", pmc.cust_cd, "'") : "MM_CUSTOMER.CUST_CD",
                                            !string.IsNullOrWhiteSpace(pmc.brn_cd) ? string.Concat("'", pmc.brn_cd, "'") : "MM_CUSTOMER.BRN_CD"
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var mc = new tm_deposit();
                                mc.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                mc.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                mc.clr_bal = UtilityM.CheckNull<decimal>(reader["Balance"]);
                                mc.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                mc.created_by = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                mc.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                mc.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                mc.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);
                                mc.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);

                                depo.Add(mc);
                            }
                        }
                    }
                }
            }
            return depo;
        }

            internal List<tm_daily_deposit> GetDailyDeposit(tm_deposit dep)
        {
            List<tm_daily_deposit> depoList = new List<tm_daily_deposit>();

            string _query = " select d.brn_cd,d.acc_num,d.trans_type,d.paid_dt,d.paid_amt,d.agent_cd,d.approval_status,  "
                            +" d.balance_amt,d.trans_cd, a.agent_name from tm_daily_deposit d, mm_agent a where a.agent_cd = d.agent_cd and d.brn_cd={0} and d.acc_num={1}";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
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
                                    var d = new tm_daily_deposit();
                                    d.brn_cd = UtilityM.CheckNull<decimal>(reader["brn_cd"]).ToString();
                                    d.trans_type = UtilityM.CheckNull<string>(reader["trans_type"]);
                                    d.acc_num = UtilityM.CheckNull<string>(reader["acc_num"]);
                                    d.approval_status = UtilityM.CheckNull<string>(reader["approval_status"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["agent_cd"]);
                                    d.agent_name = UtilityM.CheckNull<string>(reader["agent_name"]);
                                    d.paid_dt = UtilityM.CheckNull<DateTime>(reader["paid_dt"]);
                                    d.paid_amt = UtilityM.CheckNull<decimal>(reader["paid_amt"]);
                                    d.balance_amt = UtilityM.CheckNull<decimal>(reader["balance_amt"]);
                                    d.trans_cd = UtilityM.CheckNull<decimal>(reader["trans_cd"]);

                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }



        internal List<mm_agent> GetAgentData(mm_agent dep)
        {
            List<mm_agent> depoList = new List<mm_agent>();

            string _query = " select a.ardb_cd,a.brn_cd,a.agent_cd,a.agent_name  "
                            + "  from mm_agent a where a.ardb_cd={0} and a.brn_cd={1} ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd");
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new mm_agent();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ardb_cd"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["brn_cd"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["agent_cd"]);
                                    d.agent_name = UtilityM.CheckNull<string>(reader["agent_name"]);
                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }


        internal List<export_data> GetExportData(export_data prm)
        {
            List<export_data> accDtlsLovs = new List<export_data>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_GENERATE_EXPFILE"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.ardb_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.brn_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_agent_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.agent_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("p_dds_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm);

                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        var accDtlsLov = new export_data();
                                        accDtlsLov.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        accDtlsLov.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        accDtlsLov.acc_num = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                        accDtlsLov.cust_name = UtilityM.CheckNull<string>(reader["PARTY_NAME"]);
                                        accDtlsLov.agent_cd = UtilityM.CheckNull<string>(reader["AS_BLOCK"]);
                                        accDtlsLov.opening_dt = UtilityM.CheckNull<string>(reader["TRF_DT"]);
                                        accDtlsLov.curr_bal_amt = UtilityM.CheckNull<Int64>(reader["PRN"]);
                                        accDtlsLov.interest = UtilityM.CheckNull<Int64>(reader["INTT"]);
                                        accDtlsLov.balance_amt = UtilityM.CheckNull<Int64>(reader["BALANCE_AMT"]);
                                        accDtlsLov.password = UtilityM.CheckNull<string>(reader["PASSWORD"]);
                                        accDtlsLov.agent_name = prm.agent_name;

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



        internal List<string> GetDataForFile(export_data prm)
        {
            List<string> accDtlsLovs = new List<string>();

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, "P_EXPORT_DDS_DATA"))
                {
                    // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.ardb_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.brn_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_agent_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.agent_cd;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("as_machine_type", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm.Value = prm.machine_type;
                    command.Parameters.Add(parm);
                    parm = new OracleParameter("p_dds_refcur", OracleDbType.RefCursor, ParameterDirection.Output);
                    command.Parameters.Add(parm);

                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        string accDtlsLov;

                                        accDtlsLov = UtilityM.CheckNull<string>(reader["EXP_SYNTAX"]);                                       

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


        internal int InsertImportDataFile(List<string> prp)
        {
            int _ret = 0;

            string _query1 = "DELETE FROM tt_impfile";

            string _query = "INSERT INTO TT_IMPFILE VALUES({0})";


            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query1);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

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
                using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < prp.Count; i++)
                        { 
                                _statement = string.Format(_query,
                            string.Concat("'", prp[i], "'")                           
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


        internal int PopulateImportData(export_data prm)
        {
            int _ret = 0;
            
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, "P_IMPORT_DDS_DATA"))
                            {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.brn_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_agent_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.agent_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_user_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.user_id;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_machine_type", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.machine_type;
                            command.Parameters.Add(parm);

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


        internal int UpdateUnapprovedAgentTrans(mm_agent_trans dep)
        {
            var d = new mm_agent_trans();

            string _query = " Update mm_agent_trans  "
                        + "  set trans_amt = {0} where ardb_cd={1} and brn_cd={2} and agent_cd = {3} and trans_cd ={4} and trans_dt = {5} and approval_status = 'U' and  del_flag='N' ";

            string _query1 = " Update td_dep_trans  "
                        + "  set amount = {0} where ardb_cd={1} and brn_cd={2} and trans_cd ={3} and trans_dt = {4} and approval_status = 'U' and  del_flag='N' ";


            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                          string.Concat("'", dep.trans_amt, "'"),
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.agent_cd) ? string.Concat("'", dep.agent_cd, "'") : "agent_cd",
                                          string.Concat(dep.trans_cd),
                                          string.Concat("to_date('", dep.trans_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }
                        _statement = string.Format(_query1,
                                          string.Concat("'", dep.trans_amt, "'"),
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          string.Concat(dep.trans_cd),
                                          string.Concat("to_date('", dep.trans_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();

                        }

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


        internal int UpdateUnapprovedDdsTrans(List<tm_daily_deposit> dep)
        {
            
            string _query = " Update tm_daily_deposit  "
                        + "  set paid_amt = {0} where ardb_cd={1} and brn_cd={2} and acc_num = {3} and trans_cd ={4} and paid_dt = {5} and approval_status = 'U' and  del_flag='N' ";


            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < dep.Count; i++)
                        {
                            _statement = string.Format(_query,
                                          string.Concat("'", dep[i].paid_amt, "'"),
                                          !string.IsNullOrWhiteSpace(dep[i].ardb_cd) ? string.Concat("'", dep[i].ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep[i].brn_cd) ? string.Concat("'", dep[i].brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep[i].acc_num) ? string.Concat("'", dep[i].acc_num, "'") : "acc_num",
                                          string.Concat(dep[i].trans_cd),
                                          string.Concat("to_date('", dep[i].paid_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();

                            }
                        }
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




        internal mm_agent_trans GetUnapprovedAgentTrans(mm_agent_trans dep)
        {
            var d = new mm_agent_trans();

            string _query = " select a.ardb_cd,a.brn_cd,a.agent_cd,a.agent_name,trans_cd,trans_dt,trans_type,approval_status,approved_dt,trans_amt,del_flag  "
                            + "  from mm_agent_trans a where a.ardb_cd={0} and a.brn_cd={1} and agent_cd = {2} and trans_dt = {3} and approval_status = 'U' and  del_flag='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.agent_cd) ? string.Concat("'", dep.agent_cd, "'") : "agent_cd",
                                          string.Concat("to_date('", dep.trans_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {

                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ardb_cd"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["brn_cd"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["agent_cd"]);
                                    d.agent_name = UtilityM.CheckNull<string>(reader["agent_name"]);
                                    d.trans_cd = UtilityM.CheckNull<Int64>(reader["trans_cd"]);
                                    d.trans_dt = UtilityM.CheckNull<DateTime>(reader["trans_dt"]);
                                    d.trans_type = UtilityM.CheckNull<string>(reader["trans_type"]);
                                    d.approval_status = UtilityM.CheckNull<string>(reader["approval_status"]);
                                    d.approved_dt = UtilityM.CheckNull<DateTime>(reader["approved_dt"]);
                                    d.trans_amt = UtilityM.CheckNull<decimal>(reader["trans_amt"]);
                                    d.del_flag = UtilityM.CheckNull<string>(reader["del_flag"]);
                                }
                            }
                        }
                    }
                }
            }

            return d;
        }




        internal List<tm_daily_deposit> GetUnapprovedDdsTrans(mm_agent_trans dep)
        {
            List<tm_daily_deposit> depoList = new List<tm_daily_deposit>();

            string _query = " select a.ardb_cd,a.brn_cd,a.acc_num,a.paid_dt,a.paid_amt,a.agent_cd,a.trans_type,a.approval_status,a.balance_amt,a.trans_cd,c.CUST_NAME    "
                          + "  from tm_daily_deposit a,TM_DEPOSIT b,MM_CUSTOMER c " 
                          + "  where a.ARDB_CD=b.ARDB_CD AND a.ARDB_CD=c.ARDB_CD AND a.ACC_NUM=b.ACC_NUM AND b.CUST_CD=c.CUST_CD AND b.ACC_TYPE_CD=11 and "
                          + " a.ardb_cd={0} and a.brn_cd={1} and a.agent_cd = {2} and a.trans_cd = {3} and a.paid_dt = {4} and a.approval_status = 'U' and a.del_flag = 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.agent_cd) ? string.Concat("'", dep.agent_cd, "'") : "agent_cd",
                                          string.Concat(dep.trans_cd),
                                          string.Concat("to_date('", dep.trans_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"));
                ;
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new tm_daily_deposit();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ardb_cd"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["brn_cd"]);
                                    d.acc_num = UtilityM.CheckNull<string>(reader["acc_num"]);
                                    d.paid_dt = UtilityM.CheckNull<DateTime>(reader["paid_dt"]);
                                    d.paid_amt = UtilityM.CheckNull<decimal>(reader["paid_amt"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["agent_cd"]);
                                    d.trans_type = UtilityM.CheckNull<string>(reader["trans_type"]);
                                    d.approval_status = UtilityM.CheckNull<string>(reader["approval_status"]);
                                    d.balance_amt = UtilityM.CheckNull<decimal>(reader["balance_amt"]);
                                    d.trans_cd = UtilityM.CheckNull<decimal>(reader["trans_cd"]);
                                    d.cust_name = UtilityM.CheckNull<string>(reader["cust_name"]);
                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }


        internal int ApproveDdsImportData(mm_agent_trans prm)
        {
            int _ret = 0;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, "P_APPROVE_DDS"))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.brn_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_agent_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.agent_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.trans_dt;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("ad_trans_cd", OracleDbType.Int64, ParameterDirection.Input);
                            parm.Value = prm.trans_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_user_id", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.user_id;
                            command.Parameters.Add(parm);

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


        internal decimal CheckDuplicateAgentData(mm_agent_trans dep)
        {
            decimal d = 0;
              string _query = " select count(*) no  "
                          + "  from mm_agent_trans a where a.ardb_cd={0} and a.brn_cd={1} and agent_cd ={2} and trans_dt={3} and trans_amt ={4} and del_flag='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.agent_cd) ? string.Concat("'", dep.agent_cd, "'") : "agent_cd",
                                          string.Concat("to_date('", dep.trans_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                          dep.trans_amt.ToString());

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    d = UtilityM.CheckNull<decimal>(reader["no"]);
                                 }
                            }
                        }
                    }
                }
            }

            return d;
        }


        internal List<tm_daily_deposit> GetDdsAccountData(export_data dep)
        {
            List<tm_daily_deposit> depoList = new List<tm_daily_deposit>();

            string _query = " select a.ardb_cd,a.brn_cd,a.agent_cd,a.acc_num,a.trans_cd,a.paid_dt,a.paid_amt,a.balance_amt  "
                            + "  from tm_daily_deposit a where a.ardb_cd={0} and a.brn_cd={1} and a.acc_num = {2} and del_flag='N' order by  paid_dt,trans_cd";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
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
                                    var d = new tm_daily_deposit();
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ardb_cd"]);
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["brn_cd"]);
                                    d.agent_cd = UtilityM.CheckNull<string>(reader["agent_cd"]);
                                    d.acc_num = UtilityM.CheckNull<string>(reader["acc_num"]);
                                    d.trans_cd = UtilityM.CheckNull<decimal>(reader["trans_cd"]);
                                    d.paid_dt = UtilityM.CheckNull<DateTime>(reader["paid_dt"]);
                                    d.paid_amt = UtilityM.CheckNull<decimal>(reader["paid_amt"]);
                                    d.balance_amt = UtilityM.CheckNull<decimal>(reader["balance_amt"]);
                                    depoList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return depoList;
        }



        internal List<accwisesubcashbook> PopulateSubCashBookDeposit(p_report_param prp)
        {
            List<accwisesubcashbook> tcaRet = new List<accwisesubcashbook>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_sub_csh_bk_dep";
            string _query1 = " SELECT (SELECT ACC_TYPE_DESC || ' - PRINCIPAL' FROM MM_ACC_TYPE WHERE ACC_TYPE_CD=TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD) ACC_TYPE_DESC,"
                            + " TT_DEP_SUB_CASH_BOOK.ACC_NUM,    "
                            + " TT_DEP_SUB_CASH_BOOK.CASH_DR,    "
                            + " TT_DEP_SUB_CASH_BOOK.TRF_DR,     "
                            + " TT_DEP_SUB_CASH_BOOK.CASH_CR,    "
                            + " TT_DEP_SUB_CASH_BOOK.TRF_CR,     "
                            + " TT_DEP_SUB_CASH_BOOK.BAL_AMT,    "
                            + " (SELECT CONSTITUTION_DESC FROM MM_CONSTITUTION WHERE ACC_TYPE_CD = TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD AND  CONSTITUTION_CD=TT_DEP_SUB_CASH_BOOK.CONSTITUTION_CD) CONSTITUTION_DESC,"
                            + " TT_DEP_SUB_CASH_BOOK.CUST_NAME, "
                            + " TT_DEP_SUB_CASH_BOOK.BRN_CD"
                            + " FROM TT_DEP_SUB_CASH_BOOK  "
                            + " WHERE TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD = {0} "
                            + " AND NVL(TT_DEP_SUB_CASH_BOOK.CATG_CD, '0') NOT IN ('1', '2') AND TT_DEP_SUB_CASH_BOOK.ACC_NUM <> '0000' "
                            + " ORDER BY TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD,TT_DEP_SUB_CASH_BOOK.ACC_NUM ";

            string _query2 = " SELECT DISTINCT a.ACC_TYPE_CD ACC_TYPE_CD, b.ACC_TYPE_DESC || ' - PRINCIPAL' ACC_TYPE_DESC "
                            + " FROM TT_DEP_SUB_CASH_BOOK a, MM_ACC_TYPE b "
                            + " WHERE a.ACC_TYPE_CD = b.ACC_TYPE_CD ORDER BY ACC_TYPE_CD ";

            string _query3 = " SELECT (SELECT ACC_TYPE_DESC || ' - INTEREST' FROM MM_ACC_TYPE WHERE ACC_TYPE_CD=TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD) ACC_TYPE_DESC,"
                            + " TT_DEP_SUB_CASH_BOOK.ACC_NUM,    "
                            + " TT_DEP_SUB_CASH_BOOK.CASH_DR,    "
                            + " TT_DEP_SUB_CASH_BOOK.TRF_DR,     "
                            + " TT_DEP_SUB_CASH_BOOK.CASH_CR,    "
                            + " TT_DEP_SUB_CASH_BOOK.TRF_CR,     "
                            + " TT_DEP_SUB_CASH_BOOK.BAL_AMT,    "
                            + " (SELECT CONSTITUTION_DESC FROM MM_CONSTITUTION WHERE ACC_TYPE_CD = TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD AND  CONSTITUTION_CD=TT_DEP_SUB_CASH_BOOK.CONSTITUTION_CD) CONSTITUTION_DESC, "
                            + " TT_DEP_SUB_CASH_BOOK.CUST_NAME,"
                            + " TT_DEP_SUB_CASH_BOOK.BRN_CD    "
                            + " FROM TT_DEP_SUB_CASH_BOOK "
                            + " WHERE TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD = {0} "
                            + " AND NVL(TT_DEP_SUB_CASH_BOOK.CATG_CD, '0') IN ('1', '2') AND TT_DEP_SUB_CASH_BOOK.ACC_NUM <> '0000' "
                            + " ORDER BY TT_DEP_SUB_CASH_BOOK.ACC_TYPE_CD,TT_DEP_SUB_CASH_BOOK.ACC_NUM ";


            string _query4  = " SELECT DISTINCT a.ACC_TYPE_CD ACC_TYPE_CD, b.ACC_TYPE_DESC || ' - INTEREST' ACC_TYPE_DESC "
                            + " FROM TT_DEP_SUB_CASH_BOOK a, MM_ACC_TYPE b "
                            + " WHERE a.ACC_TYPE_CD = b.ACC_TYPE_CD ORDER BY ACC_TYPE_CD ";





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
                            var parm3 = new OracleParameter("adt_as_on_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);
                            command.ExecuteNonQuery();
                            transaction.Commit();
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
                                        accwisesubcashbook tcaRet1 = new accwisesubcashbook();

                                        var tca1 = new acc_type();
                                        tca1.acc_type_cd = UtilityM.CheckNull<int>(reader1["ACC_TYPE_CD"]);
                                        tca1.acc_name = UtilityM.CheckNull<string>(reader1["ACC_TYPE_DESC"]);

                                        _statement = string.Format(_query1, UtilityM.CheckNull<int>(reader1["ACC_TYPE_CD"]));

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_dep_sub_cash_book();
                                                       // tca.prn_intt_flag = UtilityM.CheckNull<string>(reader["PRN_INTT_FLAG"]);
                                                        tca.acc_type_desc = UtilityM.CheckNull<string>(reader["ACC_TYPE_DESC"]);
                                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        tca.cash_dr = UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        tca.trf_dr = UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        tca.cash_cr = UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        tca.trf_cr = UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        tca.bal_amt = UtilityM.CheckNull<decimal>(reader["BAL_AMT"]);
                                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);

                                                        tca1.tot_acc_curr_prn_recov = tca1.tot_acc_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        tca1.tot_acc_curr_intt_recov = tca1.tot_acc_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        tca1.tot_acc_ovd_prn_recov = tca1.tot_acc_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        tca1.tot_acc_ovd_intt_recov = tca1.tot_acc_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        tca1.tot_count_acc = tca1.tot_count_acc + 1;

                                                        tcaRet1.ttdepsubcashbook.Add(tca);
                                                    }
                                                }
                                            }
                                        }
                                        tcaRet1.acctype = tca1;
                                        tcaRet.Add(tcaRet1);
                                    }
                                }
                            }
                        }

                        string _statement2 = string.Format(_query4);

                        using (var command2 = OrclDbConnection.Command(connection, _statement2))
                        {
                            using (var reader2 = command2.ExecuteReader())
                            {
                                if (reader2.HasRows)
                                {
                                    while (reader2.Read())
                                    {
                                        accwisesubcashbook tcaRet1 = new accwisesubcashbook();

                                        var tca1 = new acc_type();
                                        tca1.acc_type_cd = UtilityM.CheckNull<int>(reader2["ACC_TYPE_CD"]);
                                        tca1.acc_name = UtilityM.CheckNull<string>(reader2["ACC_TYPE_DESC"]);

                                        _statement = string.Format(_query3, UtilityM.CheckNull<int>(reader2["ACC_TYPE_CD"]));

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_dep_sub_cash_book();
                                                        //tca.prn_intt_flag = UtilityM.CheckNull<string>(reader["PRN_INTT_FLAG"]);
                                                        tca.acc_type_desc = UtilityM.CheckNull<string>(reader["ACC_TYPE_DESC"]);
                                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        tca.cash_dr = UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        tca.trf_dr = UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        tca.cash_cr = UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        tca.trf_cr = UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        tca.bal_amt = UtilityM.CheckNull<decimal>(reader["BAL_AMT"]);
                                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);

                                                        tca1.tot_acc_curr_prn_recov = tca1.tot_acc_curr_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_DR"]);
                                                        tca1.tot_acc_curr_intt_recov = tca1.tot_acc_curr_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_DR"]);
                                                        tca1.tot_acc_ovd_prn_recov = tca1.tot_acc_ovd_prn_recov + UtilityM.CheckNull<decimal>(reader["CASH_CR"]);
                                                        tca1.tot_acc_ovd_intt_recov = tca1.tot_acc_ovd_intt_recov + UtilityM.CheckNull<decimal>(reader["TRF_CR"]);
                                                        tca1.tot_count_acc = tca1.tot_count_acc + 1;

                                                        tcaRet1.ttdepsubcashbook.Add(tca);
                                                    }
                                                }
                                            }
                                        }
                                        tcaRet1.acctype = tca1;
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


        internal List<tt_sbca_dtl_list> PopulateDLSavings(p_report_param prp)
        {
            List<tt_sbca_dtl_list> tcaRet = new List<tt_sbca_dtl_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_sbca_detailed_list";
            string _query1 = " SELECT TT_SBCA_DTL_LIST.BRN_CD,    "
                            + " TT_SBCA_DTL_LIST.ACC_TYPE_CD,      "
                            + " TT_SBCA_DTL_LIST.ACC_NUM,          "
                            + " TT_SBCA_DTL_LIST.CUST_NAME,        "
                            + " TT_SBCA_DTL_LIST.OPENING_DT,       "
                            + " TT_SBCA_DTL_LIST.CONSTITUTION_DESC,"
                            + " TT_SBCA_DTL_LIST.BALANCE,          "
                            + " TT_SBCA_DTL_LIST.INTT_PROV_AMT, "
                            + " 1 rowmark, "
                            + " TT_SBCA_DTL_LIST.GUARDIAN_NAME,   "
                            + " TT_SBCA_DTL_LIST.PRESENT_ADDRESS, "
                            + " TT_SBCA_DTL_LIST.DT_OF_BIRTH, "
                            + " nvl(TT_SBCA_DTL_LIST.AGE,0) AGE,  "
                            + " TT_SBCA_DTL_LIST.CUST_CD  CUST_CD "
                            + " FROM TT_SBCA_DTL_LIST  ORDER BY TO_NUMBER(TT_SBCA_DTL_LIST.ACC_NUM)  ";

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
                            var parm2 = new OracleParameter("brncd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("acctypecd", OracleDbType.Int16, ParameterDirection.Input);
                            parm3.Value = prp.acc_type_cd;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("constcd", OracleDbType.Int32, ParameterDirection.Input);
                            parm4.Value = prp.const_cd;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm5.Value = prp.from_dt;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();
                            transaction.Commit();
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
                                        var tca = new tt_sbca_dtl_list();
                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                        tca.balance = UtilityM.CheckNull<decimal>(reader["BALANCE"]);
                                        tca.intt_prov_amt = UtilityM.CheckNull<decimal>(reader["INTT_PROV_AMT"]);
                                        tca.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        tca.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        tca.dt_of_birth = UtilityM.CheckNull<DateTime>(reader["DT_OF_BIRTH"]);
                                        tca.age = UtilityM.CheckNull<decimal>(reader["AGE"]);
                                        tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);

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

        internal List<conswise_sb_dl> PopulateDLSavingsAll(p_report_param prp)
        {
            List<conswise_sb_dl> tcaRet = new List<conswise_sb_dl>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_sbca_detailed_list_all";

            string _query1 = " SELECT TT_SBCA_DTL_LIST1.BRN_CD,    "
                            + " TT_SBCA_DTL_LIST1.ACC_TYPE_CD,      "
                            + " TT_SBCA_DTL_LIST1.ACC_NUM,          "
                            + " TT_SBCA_DTL_LIST1.CUST_NAME,        "
                            + " TT_SBCA_DTL_LIST1.OPENING_DT,       "
                            + " TT_SBCA_DTL_LIST1.CONSTITUTION_DESC,"
                            + " TT_SBCA_DTL_LIST1.BALANCE,          "
                            + " TT_SBCA_DTL_LIST1.INTT_PROV_AMT, "
                            + " TT_SBCA_DTL_LIST1.CONSTITUTION_CD"
                            + " FROM TT_SBCA_DTL_LIST1  "
                            + " WHERE TT_SBCA_DTL_LIST1.CONSTITUTION_CD = {0} ORDER BY TT_SBCA_DTL_LIST1.ACC_NUM  ";

            string _query2 = " SELECT DISTINCT TT_SBCA_DTL_LIST1.CONSTITUTION_CD,TT_SBCA_DTL_LIST1.CONSTITUTION_DESC "
                            + " FROM TT_SBCA_DTL_LIST1  ";

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
                            var parm2 = new OracleParameter("brncd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("acctypecd", OracleDbType.Int16, ParameterDirection.Input);
                            parm3.Value = prp.acc_type_cd;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.from_dt;
                            command.Parameters.Add(parm4);

                            command.ExecuteNonQuery();
                            transaction.Commit();
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
                                        conswise_sb_dl tcaRet1 = new conswise_sb_dl();

                                        var tca1 = new cons_type();
                                        tca1.constitution_cd = UtilityM.CheckNull<int>(reader1["CONSTITUTION_CD"]);
                                        tca1.constitution_desc = UtilityM.CheckNull<string>(reader1["CONSTITUTION_DESC"]);

                                       


                                        _statement = string.Format(_query1, UtilityM.CheckNull<int>(reader1["CONSTITUTION_CD"]));
                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_sbca_dtl_list();
                                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                                        tca.balance = UtilityM.CheckNull<decimal>(reader["BALANCE"]);
                                                        tca.intt_prov_amt = UtilityM.CheckNull<decimal>(reader["INTT_PROV_AMT"]);
                                                        tca.constitution_cd = UtilityM.CheckNull<int>(reader["CONSTITUTION_CD"]);
                                                        tca1.tot_cons_count = tca1.tot_cons_count + 1;
                                                        tca1.tot_cons_balance = tca1.tot_cons_balance + UtilityM.CheckNull<decimal>(reader["BALANCE"]);

                                                        tcaRet1.ttsbcadtllist.Add(tca);                                                        
                                                    }                                                    
                                                }
                                            }
                                        }
                                        tcaRet1.constype = tca1;
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



        internal List<standing_instr> PopulateActiveSIList(p_report_param prp)
        {
            List<standing_instr> tcaRet = new List<standing_instr>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            
            string _query1 = "SELECT(SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = a.acc_type_from) acc_type_from,"
                            + " a.acc_num_from acc_num_from,"
                            + " (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = b.acc_type_to) acc_type_to, "
                            + " b.acc_num_to acc_num_to,"
                            + " a.instr_status instr_status,"
                            + " a.first_trf_dt first_trf_dt,"
                            + " a.periodicity periodicity,"
                            + " decode(a.prn_intt_flag,'P','Principal','I','Interest') prn_intt_flag,"
                            + " a.amount amount,"
                            + " a.srl_no srl_no"
                            + " FROM sm_standing_instruction a,"
                            + " sd_standing_instruction b"
                            + " WHERE a.ARDB_CD = b.ARDB_CD AND a.BRN_CD = b.BRN_CD "
                            + " AND a.SRL_NO = b.SRL_NO "
                            + " AND NVL(a.instr_status, 'O') = 'O' AND a.ARDB_CD = {0} and a.brn_cd = {1} "
                            + " And a.srl_no = b.srl_no "
                            + " Order by a.acc_type_from,"
                            + " a.acc_num_from ";

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
                        
                        _statement = string.Format(_query1, prp.ardb_cd, prp.brn_cd);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new standing_instr();
                                        tca.acc_type_from = UtilityM.CheckNull<string>(reader["ACC_TYPE_FROM"]);
                                        tca.acc_num_from = UtilityM.CheckNull<string>(reader["ACC_NUM_FROM"]);
                                        tca.acc_type_to = UtilityM.CheckNull<string>(reader["ACC_TYPE_TO"]);
                                        tca.acc_num_to = UtilityM.CheckNull<string>(reader["ACC_NUM_TO"]);
                                        tca.instr_status = UtilityM.CheckNull<string>(reader["INSTR_STATUS"]);
                                        tca.first_trf_dt = UtilityM.CheckNull<DateTime>(reader["FIRST_TRF_DT"]);
                                        tca.periodicity = UtilityM.CheckNull<string>(reader["PERIODICITY"]);
                                        tca.prn_intt_flag = UtilityM.CheckNull<string>(reader["PRN_INTT_FLAG"]);
                                        tca.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                        tca.srl_no = UtilityM.CheckNull<decimal>(reader["SRL_NO"]);
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


        internal List<standing_instr> PopulateExecutedSIList(p_report_param prp)
        {
            List<standing_instr> tcaRet = new List<standing_instr>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            string _query = "p_si_rep"; 

            string _query1 = "SELECT(SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = a.DR_ACC_TYPE) acc_type_from,"
                            + " a.DR_ACC_NUM acc_num_from,"
                            + " (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = a.CR_ACC_TYPE) acc_type_to, "
                            + " a.CR_ACC_NUM acc_num_to,"
                            + " a.TRANS_DT,"
                            + " a.AMOUNT amount,"
                            + " (SELECT cust_name FROM mm_customer WHERE mm_customer.ARDB_CD = a.ardb_cd AND mm_customer.cust_CD = a.CUST_CD) cust_name"
                            + " FROM TT_EXECUTED_SI a ";                         



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
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.brn_cd;
                            command.Parameters.Add(parm3);
                            

                            command.ExecuteNonQuery();
                            transaction.Commit();
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
                                        var tca = new standing_instr();
                                        tca.acc_type_from = UtilityM.CheckNull<string>(reader["ACC_TYPE_FROM"]);
                                        tca.acc_num_from = UtilityM.CheckNull<string>(reader["ACC_NUM_FROM"]);
                                        tca.acc_type_to = UtilityM.CheckNull<string>(reader["ACC_TYPE_TO"]);
                                        tca.acc_num_to = UtilityM.CheckNull<string>(reader["ACC_NUM_TO"]);
                                        tca.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        tca.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
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



        internal List<tt_sbca_dtl_list> PopulateDLRecuring(p_report_param prp)
        {
            List<tt_sbca_dtl_list> tcaRet = new List<tt_sbca_dtl_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_rd_prov_intt_detail_list";
            string _query1 = " SELECT TT_RDPROV_INTT.ACC_TYPE_CD,"
                                + " TT_RDPROV_INTT.ACC_NUM,  "
                                + " (SELECT CONSTITUTION_DESC FROM MM_CONSTITUTION WHERE CONSTITUTION_CD = TT_RDPROV_INTT.CONSTITUTION_CD) CONSTITUTION_DESC,   "
                                + " TT_RDPROV_INTT.CUST_NAME,  "
                                + " TT_RDPROV_INTT.OPENING_DT, "
                                + " TT_RDPROV_INTT.MAT_DT,     "
                                + " TT_RDPROV_INTT.INSTL_AMT,  "
                                + " TT_RDPROV_INTT.INTT_RT,       "
                                + " TT_RDPROV_INTT.PRN_AMT,       "
                                + " TT_RDPROV_INTT.PROV_INTT_AMT, "
                                + " TM_DEPOSIT.CUST_CD   "
                                + " FROM TT_RDPROV_INTT, "
                                + " TM_DEPOSIT "
                                + " WHERE ( TT_RDPROV_INTT.ARDB_CD = TM_DEPOSIT.ARDB_CD ) and ( TT_RDPROV_INTT.ACC_TYPE_CD = TM_DEPOSIT.ACC_TYPE_CD ) and"
                                + " ( TT_RDPROV_INTT.ACC_NUM = TM_DEPOSIT.ACC_NUM ) and "
                                + " ( ( TT_RDPROV_INTT.ARDB_CD = {0} ) AND ( TT_RDPROV_INTT.BRN_CD = {1} ) ) ORDER BY TT_RDPROV_INTT.ACC_NUM ";
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
                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("asdt_dt", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.from_dt.ToString("dd/MM/yyyy");
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }
                        _statement = string.Format(_query1,
                                       string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"));
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_sbca_dtl_list();
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_DESC"]);
                                        tca.INSTL_AMT = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.INTT_RT = Convert.ToDecimal(UtilityM.CheckNull<float>(reader["INTT_RT"]));
                                        tca.PROV_INTT_AMT = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
                                        tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);

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


        internal List<tt_sbca_dtl_list> PopulateDLFixedDeposit(p_report_param prp)
        {
            List<tt_sbca_dtl_list> tcaRet = new List<tt_sbca_dtl_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_td_prov_intt_detail_list";
            string _query1 = " SELECT TT_TDPROV_INTT.ACC_TYPE_CD, "
                                + " TT_TDPROV_INTT.ACC_NUM,       "
                                + " TT_TDPROV_INTT.CUST_NAME,     "
                                + " TT_TDPROV_INTT.OPENING_DT,    "
                                + " TT_TDPROV_INTT.MAT_DT,        "
                                + " TT_TDPROV_INTT.PRN_AMT,       "
                                + " TT_TDPROV_INTT.INTT_RT,       "
                                + " TT_TDPROV_INTT.PROV_INTT_AMT, "
                                + " TT_TDPROV_INTT.CONSTITUTION_CD"
                                + " FROM TT_TDPROV_INTT "
                                + " WHERE ( TT_TDPROV_INTT.ARDB_CD = {0} ) AND  "
                                + "  ( TT_TDPROV_INTT.BRN_CD = {1} ) AND  "
                                + " ( TT_TDPROV_INTT.ACC_TYPE_CD = {2} ) AND   "
                                + " ( trim(substr(TT_TDPROV_INTT.CONSTITUTION_CD,1,2)) = {3} )  ORDER BY TT_TDPROV_INTT.ACC_NUM ";
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
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.brn_cd;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }
                        _statement = string.Format(_query1,
                                       string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                       prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                       prp.const_cd != 0 ? string.Concat("'", Convert.ToString(prp.const_cd), "'") : "'0'");
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_sbca_dtl_list();
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        //var x = UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2);
                                        //tca.constitution_cd = Convert.ToInt16(UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2).Trim());
                                        //tca.INSTL_AMT = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.INTT_RT = Convert.ToDecimal(UtilityM.CheckNull<float>(reader["INTT_RT"]));
                                        tca.PROV_INTT_AMT = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
                                       // tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
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


        internal List<conswise_sb_dl> PopulateDLFixedDepositAll(p_report_param prp)
        {
            List<conswise_sb_dl> tcaRet = new List<conswise_sb_dl>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_td_prov_intt_detail_list";
            string _query1 = " SELECT TT_TDPROV_INTT.ACC_TYPE_CD, "
                                + " TT_TDPROV_INTT.ACC_NUM,       "
                                + " TT_TDPROV_INTT.CUST_NAME,     "
                                + " TT_TDPROV_INTT.OPENING_DT,    "
                                + " TT_TDPROV_INTT.MAT_DT,        "
                                + " TT_TDPROV_INTT.PRN_AMT,       "
                                + " TT_TDPROV_INTT.INTT_RT,       "
                                + " TT_TDPROV_INTT.PROV_INTT_AMT, "
                                + " TT_TDPROV_INTT.CONSTITUTION_CD"
                                + " FROM TT_TDPROV_INTT "
                               + " WHERE ( TT_TDPROV_INTT.ARDB_CD = {0} ) AND  "
                                + "  ( TT_TDPROV_INTT.BRN_CD = {1} ) AND  "
                                + " ( TT_TDPROV_INTT.ACC_TYPE_CD = {2} ) AND   "
                                + " ( TT_TDPROV_INTT.CONSTITUTION_CD = {3} )  ORDER BY TT_TDPROV_INTT.ACC_NUM ";


            string _query2 = " SELECT  DISTINCT TT_TDPROV_INTT.CONSTITUTION_CD,MM_CONSTITUTION.CONSTITUTION_DESC "
                            + " FROM TT_TDPROV_INTT,MM_CONSTITUTION"
                            + " WHERE TT_TDPROV_INTT.CONSTITUTION_CD = MM_CONSTITUTION.CONSTITUTION_CD"
                            + " AND MM_CONSTITUTION.ACC_TYPE_CD = {0}";




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
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.brn_cd;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }


                        string _statement1 = string.Format(_query2, prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD");

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        conswise_sb_dl tcaRet1 = new conswise_sb_dl();

                                        var tca1 = new cons_type();
                                        tca1.constitution_cd = Convert.ToInt32(UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"]));
                                        tca1.constitution_desc = UtilityM.CheckNull<string>(reader1["CONSTITUTION_DESC"]);

                                        _statement = string.Format(_query1,
                                       string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                       prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                       Convert.ToInt32(UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"])));
                                      // prp.const_cd != 0 ? string.Concat("'", UtilityM.CheckNull<int>(reader1["CONSTITUTION_CD"]), "'") : "'0'");

                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_sbca_dtl_list();
                                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                                        //var x = UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2);
                                                       // tca.constitution_cd = Convert.ToInt16(UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2).Trim());
                                                        //tca.INSTL_AMT = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                                        tca.INTT_RT = Convert.ToDecimal(UtilityM.CheckNull<float>(reader["INTT_RT"]));
                                                        tca.PROV_INTT_AMT = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
                                                        //tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);

                                                        tca1.tot_cons_count = tca1.tot_cons_count + 1;
                                                        tca1.tot_cons_balance = tca1.tot_cons_balance + UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                                        tca1.tot_cons_intt_balance = tca1.tot_cons_intt_balance + UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);

                                                        tcaRet1.ttsbcadtllist.Add(tca);

                                                        //tcaRet.Add(tca);
                                                    }
                                                    
                                                }
                                            }
                                        }

                                        tcaRet1.constype = tca1;
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


        internal List<agentwiseDL> PopulateDLDds(p_report_param prp)
        {
            List<agentwiseDL> tcaRet = new List<agentwiseDL>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_dds_prov_intt_new";
            string _query1 = " SELECT TT_DDS_PROV_INTT.ACC_TYPE_CD,  "
                            + " TT_DDS_PROV_INTT.ACC_NUM,       "
                            + " TT_DDS_PROV_INTT.CUST_NAME ,     "
                            + " MM_AGENT.AGENT_NAME ,     "
                            + " TT_DDS_PROV_INTT.OPENING_DT, "
                            + " TT_DDS_PROV_INTT.MATURITY_DT, "
                            + " TT_DDS_PROV_INTT.PRN_AMT,    "
                            + " TT_DDS_PROV_INTT.INTT_AMT   "
                            + " FROM TT_DDS_PROV_INTT,MM_AGENT " 
                            + " WHERE  TT_DDS_PROV_INTT.AGENT_CD = MM_AGENT.AGENT_CD  and MM_AGENT.ARDB_CD = {0} " 
                            + "  AND TT_DDS_PROV_INTT.PRN_AMT > 0  AND MM_AGENT.AGENT_NAME = {1} ORDER BY TT_DDS_PROV_INTT.ACC_NUM ";

            string _query2 = " SELECT DISTINCT MM_AGENT.AGENT_NAME "
                            + " FROM TT_DDS_PROV_INTT,MM_AGENT "
                            + " WHERE  TT_DDS_PROV_INTT.AGENT_CD = MM_AGENT.AGENT_CD  and MM_AGENT.ARDB_CD = {0} AND TT_DDS_PROV_INTT.PRN_AMT > 0   ";


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
                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }


                        string _statement1 = string.Format(_query2,
                                         string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"));

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        agentwiseDL tcaRet1 = new agentwiseDL();

                                        tcaRet1.agentname = UtilityM.CheckNull<string>(reader1["AGENT_NAME"]);


                                        _statement = string.Format(_query1,
                                       string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"), string.Concat("'", UtilityM.CheckNull<string>(reader1["AGENT_NAME"]), "'"));
                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_sbca_dtl_list();
                                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                                        tca.agent_name = UtilityM.CheckNull<string>(reader["AGENT_NAME"]);
                                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MATURITY_DT"]);
                                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                                        tca.intt_prov_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);

                                                        tcaRet1.tot_prn = tcaRet1.tot_prn + tca.PRN_AMT;
                                                        tcaRet1.tot_count = tcaRet1.tot_count + 1;

                                                        tcaRet1.ttsbcadtllist.Add(tca);

                                                        //tcaRet.Add(tca);
                                                    }
                                                }
                                            }
                                        }
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



        internal List<agentwiseDL> PopulateSlabwiseDeposit(p_report_param prp)
        {
            List<agentwiseDL> tcaRet = new List<agentwiseDL>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_slabwise_deposit";
            string _query1 = " SELECT TT_SLABWISE_DEPOSIT_NEW.ACC_TYPE_CD ,  "
                            + " TT_SLABWISE_DEPOSIT_NEW.CONSTITUTION_CD ,       "
                            + " TT_SLABWISE_DEPOSIT_NEW.NO_OF_ACCOUNT ,     "
                            + " TT_SLABWISE_DEPOSIT_NEW.BALANCE_AMT     "
                            + " FROM TT_SLABWISE_DEPOSIT_NEW "
                            + " WHERE  TT_SLABWISE_DEPOSIT_NEW.ACC_TYPE_CD = {0} " ;

            string _query2 = " SELECT DISTINCT TT_SLABWISE_DEPOSIT_NEW.ACC_TYPE_CD "
                           + " FROM TT_SLABWISE_DEPOSIT_NEW " ;


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
                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            command.ExecuteNonQuery();
                            transaction.Commit();
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
                                        agentwiseDL tcaRet1 = new agentwiseDL();

                                        tcaRet1.agentname = UtilityM.CheckNull<string>(reader1["ACC_TYPE_CD"]);


                                        _statement = string.Format(_query1, string.Concat("'", UtilityM.CheckNull<string>(reader1["ACC_TYPE_CD"]), "'"));
                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_sbca_dtl_list();
                                                        tca.acc_type_desc = UtilityM.CheckNull<string>(reader["ACC_TYPE_CD"]);
                                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]);
                                                        tca.no_of_account = UtilityM.CheckNull<int>(reader["NO_OF_ACCOUNT"]);
                                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["BALANCE_AMT"]);

                                                        tcaRet1.tot_prn = tcaRet1.tot_prn + tca.PRN_AMT;
                                                        tcaRet1.tot_count = tcaRet1.tot_count + tca.no_of_account;

                                                        tcaRet1.ttsbcadtllist.Add(tca);

                                                        //tcaRet.Add(tca);
                                                    }
                                                }
                                            }
                                        }
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



        internal decimal GetDdsInterest(p_report_param prp)
        {
            decimal ld_intt = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_dds_intt_new";
            string _query1 = " SELECT   f_calc_interest_dds({0},{1},substr({2},1,10)) INTT_AMT From dual ";
                                
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
                            var parm2 = new OracleParameter("as_acc_num", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.acc_num;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);
                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }
                        _statement = string.Format(_query1,
                                       string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                       string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                       string.Concat("'", prp.from_dt, "'") );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {                                       

                                        ld_intt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);                                        
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        ld_intt = -1;
                    }
                }
            }
            return ld_intt;
        }



        internal List<tt_acc_stmt> PopulateASSaving(p_report_param prp)
        {
            List<tt_acc_stmt> tcaRet = new List<tt_acc_stmt>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_acc_stmt";
            string _query1 = " SELECT TT_ACC_STMT.TRANS_DT, "
                            + " TT_ACC_STMT.PARTICULARS,     "
                            + " TT_ACC_STMT.DR_AMT,  "
                            + " TT_ACC_STMT.CR_AMT,  "
                            + " TT_ACC_STMT.BALANCE, "
                            + " TT_ACC_STMT.SRL_NO,  "
                            + " TT_ACC_STMT.INSTRUMENT_NUM,  "
                            + " ' ' c_name, "
                            + " ' ' c_addr "
                            + "  FROM TT_ACC_STMT "
                            + " ORDER BY NVL(TT_ACC_STMT.SRL_NO,0) ASC";
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
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Decimal, ParameterDirection.Input);
                            parm1.Value = prp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("ad_acc_type_cd", OracleDbType.Decimal, ParameterDirection.Input);
                            parm2.Value = prp.acc_type_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("as_acc_num", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.acc_num;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.from_dt;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm5.Value = prp.to_dt;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();

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
                                        var tca = new tt_acc_stmt();
                                        tca.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        tca.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        tca.dr_amt = UtilityM.CheckNull<decimal>(reader["DR_AMT"]);
                                        tca.cr_amt = UtilityM.CheckNull<decimal>(reader["CR_AMT"]);
                                        tca.balance = UtilityM.CheckNull<decimal>(reader["BALANCE"]);
                                        tca.srl_no = UtilityM.CheckNull<decimal>(reader["SRL_NO"]);
                                        tca.instrument_num = Convert.ToDecimal(UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]));
                                        tca.c_name = UtilityM.CheckNull<string>(reader["C_NAME"]);
                                        tca.c_addr = UtilityM.CheckNull<string>(reader["C_ADDR"]);
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                        // transaction.Commit();
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


        internal List<tm_deposit> PopulateASRecuring(p_report_param prp)
        {
            List<tm_deposit> tcaRet = new List<tm_deposit>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = "SELECT ACC_TYPE_CD,ACC_NUM,CUST_NAME,INSTL_DT,PAID_AMT,MONTH,YEAR,OPNG_BAL "
                + " FROM (SELECT TM_DEPOSIT.ACC_TYPE_CD,"
                + " TM_DEPOSIT.ACC_NUM,           "
                + " MM_CUSTOMER.CUST_NAME,        "
                + " TD_RD_INSTALLMENT.INSTL_DT,   "
                + " TM_DEPOSIT.INSTL_AMT PAID_AMT,"
                + " TO_CHAR(TD_RD_INSTALLMENT.DUE_DT, 'MM') MONTH,     "
                + " TO_CHAR(TD_RD_INSTALLMENT.DUE_DT, 'YYYY') YEAR,    "
                + " TM_DEPOSIT.INSTL_AMT * COUNT(B.INSTL_NUM) OPNG_BAL "
                + " FROM MM_CUSTOMER,   "
                + " TD_RD_INSTALLMENT,  "
                + " TD_RD_INSTALLMENT B,"
                + " TM_DEPOSIT          "
                + " WHERE ( TD_RD_INSTALLMENT.ACC_NUM = TM_DEPOSIT.ACC_NUM ) and "
                + " ( TD_RD_INSTALLMENT.ACC_NUM = B.ACC_NUM ) and    "
                + " ( MM_CUSTOMER.CUST_CD = TM_DEPOSIT.CUST_CD ) and (MM_CUSTOMER.ARDB_CD = TM_DEPOSIT.ARDB_CD) and (TD_RD_INSTALLMENT.ARDB_CD = TM_DEPOSIT.ARDB_CD) and "
                + " ( TD_RD_INSTALLMENT.INSTL_DT BETWEEN to_date('{0}','dd-mm-yyyy' ) AND to_date('{1}','dd-mm-yyyy' ) )  AND "
                + " ( B.INSTL_DT <= to_date('{2}','dd-mm-yyyy' ) ) AND "
                + " ( B.STATUS   = 'P' ) AND ( TM_DEPOSIT.ARDB_CD   = {3} ) AND "
                + " ( ( TM_DEPOSIT.ACC_NUM = {4} ) )  AND "
                + " TM_DEPOSIT.ACC_TYPE_CD = 6      "
                + " GROUP BY TM_DEPOSIT.ACC_TYPE_CD,"
                + " TM_DEPOSIT.ACC_NUM,             "
                + " MM_CUSTOMER.CUST_NAME,          "
                + " TD_RD_INSTALLMENT.INSTL_DT,     "
                + " TM_DEPOSIT.INSTL_AMT,           "
                + " TD_RD_INSTALLMENT.DUE_DT ) X ORDER BY X.INSTL_DT ASC   ";
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
                        _statement = string.Format(_query1,
                                           prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "from_dt",
                                           prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "to_dt",
                                           prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "to_dt",
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'")
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tm_deposit();
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_DT"]);
                                        tca.prn_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                                        tca.month = UtilityM.CheckNull<string>(reader["MONTH"]);
                                        tca.year = UtilityM.CheckNull<string>(reader["YEAR"]);
                                        tca.clr_bal = UtilityM.CheckNull<decimal>(reader["OPNG_BAL"]);
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


        internal List<tt_dds_statement> PopulateASDds(p_report_param prp)
        {
            List<tt_dds_statement> tcaRet = new List<tt_dds_statement>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = "SELECT   TM_DAILY_DEPOSIT.ACC_NUM, "
                + " MM_CUSTOMER.CUST_NAME,"
                + " TM_DAILY_DEPOSIT.PAID_DT ,           "
                + " TM_DAILY_DEPOSIT.PAID_AMT, "
                + " TM_DAILY_DEPOSIT.AGENT_CD,   "
                + " (Select MIN(BALANCE_AMT) from TM_DAILY_DEPOSIT "
                + " WHERE ARDB_CD = {0}     "
                + " AND  BRN_CD= {1}    "
                + " AND  paid_dt = (Select MIN(PAID_DT) from TM_DAILY_DEPOSIT  "
                + " WHERE TM_DAILY_DEPOSIT.ARDB_CD={2}   "
                + " AND	TM_DAILY_DEPOSIT.BRN_CD={3}   "
                + " AND TM_DAILY_DEPOSIT.PAID_DT >= {4} "
                + " and TM_DAILY_DEPOSIT.acc_num = {5}  )      "
                + " and acc_num =  {6}) OPNG_BAL , "
                + " TM_DEPOSIT.CLR_BAL   "
                + " FROM TM_DAILY_DEPOSIT,"
                + " TM_DEPOSIT,  "
                + " MM_CUSTOMER "
                + " WHERE  TM_DAILY_DEPOSIT.ACC_NUM = TM_DEPOSIT.ACC_NUM "
                + " AND    TM_DEPOSIT.ARDB_CD = MM_CUSTOMER.ARDB_CD AND TM_DEPOSIT.CUST_CD = MM_CUSTOMER.CUST_CD "
                + " AND    TM_DAILY_DEPOSIT.ACC_NUM = {7}     "
                + " AND	  TM_DEPOSIT.ARDB_CD={8} "
                + " AND	  TM_DEPOSIT.ACC_TYPE_CD = 11 "
                + " AND    TM_DAILY_DEPOSIT.PAID_DT BETWEEN {9} AND {10} "
                + " AND    TM_DAILY_DEPOSIT.APPROVAL_STATUS = 'A'  ORDER BY TM_DAILY_DEPOSIT.PAID_DT,TM_DAILY_DEPOSIT.TRANS_CD ASC   ";
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
                        _statement = string.Format(_query1,
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                            string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                           prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "from_dt",
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           prp.from_dt != null ? string.Concat("'", prp.from_dt.ToString("dd/MM/yyyy"), "'") : "from_dt",
                                           prp.to_dt != null ? string.Concat("'", prp.to_dt.ToString("dd/MM/yyyy"), "'") : "to_dt"                                           
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_dds_statement();
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.paid_dt = UtilityM.CheckNull<DateTime>(reader["PAID_DT"]);
                                        tca.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                                        tca.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                        tca.opng_bal = UtilityM.CheckNull<decimal>(reader["OPNG_BAL"]);
                                        tca.clr_bal = UtilityM.CheckNull<decimal>(reader["CLR_BAL"]);
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


        internal List<tm_deposit> PopulateASFixedDeposit(p_report_param prp)
        {
            List<tm_deposit> tcaRet = new List<tm_deposit>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = " SELECT  V_TRANS_DTLS.TRANS_DT,     "
                                + " V_TRANS_DTLS.TRANS_CD,             "
                                + " V_TRANS_DTLS.ACC_TYPE_CD,          "
                                + " V_TRANS_DTLS.ACC_NUM,              "
                                + " V_TRANS_DTLS.TRANS_MODE,           "
                                + " V_TRANS_DTLS.TRF_TYPE, 	          "
                                + " (SELECT DISTINCT TM_DEPOSIT.CUST_CD"
                                + " FROM   TM_DEPOSIT                  "
                                + " WHERE  TM_DEPOSIT.ACC_TYPE_CD = {0}          "
                                + " AND    TM_DEPOSIT.ACC_NUM     = {1}              "
                                + " AND    TM_DEPOSIT.RENEW_ID    = {2} ) CUST_CD,  "
                                + " (SELECT TM_DEPOSIT.PRN_AMT                               "
                                + " FROM   TM_DEPOSIT                                        "
                                + " WHERE  TM_DEPOSIT.ACC_TYPE_CD = {3}          "
                                + " AND    TM_DEPOSIT.ACC_NUM     = {4}              "
                                + " AND    TM_DEPOSIT.RENEW_ID    = {5}) PRN_AMT,   "
                                + " (SELECT TM_DEPOSIT.OPENING_DT                            "
                                + " FROM   TM_DEPOSIT                                        "
                                + " WHERE  TM_DEPOSIT.ACC_TYPE_CD = {6}          "
                                + " AND    TM_DEPOSIT.ACC_NUM     = {7}              "
                                + " AND    TM_DEPOSIT.RENEW_ID    =	{8}) OPENING_DT,"
                                + " V_TRANS_DTLS.TRANS_TYPE,         	                     "
                                + " V_TRANS_DTLS.AMOUNT                                       "
                                + " FROM	  V_TRANS_DTLS                                       "
                                + " WHERE   V_TRANS_DTLS.ARDB_CD = {9} AND V_TRANS_DTLS.ACC_TYPE_CD = {10}        "
                                + " AND   V_TRANS_DTLS.ACC_NUM     = {11}              "
                                + " AND   V_TRANS_DTLS.TRANS_DT  BETWEEN to_date('{12}','dd-mm-yyyy' ) AND to_date('{13}','dd-mm-yyyy' )";
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
                        _statement = string.Format(_query1,
                                           prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           prp.const_cd != 0 ? Convert.ToString(prp.const_cd) : "0",
                                           prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           prp.const_cd != 0 ? Convert.ToString(prp.const_cd) : "0",
                                           prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           prp.const_cd != 0 ? Convert.ToString(prp.const_cd) : "0",
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           prp.acc_type_cd != 0 ? Convert.ToString(prp.acc_type_cd) : "ACC_TYPE_CD",
                                           string.IsNullOrWhiteSpace(prp.acc_num) ? "acc_num" : string.Concat("'", prp.acc_num, "'"),
                                           prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "from_dt",
                                           prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "to_dt"
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tm_deposit();
                                        tca.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        tca.trans_cd = Convert.ToInt64(UtilityM.CheckNull<Int64>(reader["TRANS_CD"]));
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                                        tca.intt_trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                        tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                        tca.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                        var x = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                        tca.clr_bal = Convert.ToDecimal(UtilityM.CheckNull<double>(reader["AMOUNT"]));
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


        internal List<tt_opn_cls_register> PopulateOpenCloseRegister(p_report_param prp)
        {
            List<tt_opn_cls_register> tcaRet = new List<tt_opn_cls_register>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_opn_cls_register";
            string _query1 = " SELECT TT_OPN_CLS_REGISTER.ACC_TYPE_CD,"
                            + " MM_ACC_TYPE.ACC_TYPE_DESC, "
                            + " TT_OPN_CLS_REGISTER.ACC_NUM,           "
                            + " TT_OPN_CLS_REGISTER.CUST_CD,           "
                            + " TT_OPN_CLS_REGISTER.OPN_CLS_DT,        "
                            + " TT_OPN_CLS_REGISTER.PRN_AMT,           "
                            + " TT_OPN_CLS_REGISTER.INTT_AMT,          "
                            + " TT_OPN_CLS_REGISTER.PENAL_AMT,         "
                            + " TT_OPN_CLS_REGISTER.STATUS,            "
                            + " TT_OPN_CLS_REGISTER.INTT_RT,           "
                            + " TT_OPN_CLS_REGISTER.DEP_PERIOD,        "
                            + " TT_OPN_CLS_REGISTER.AGENT_CD,           "
                            + " MM_CUSTOMER.CUST_NAME "
                            + " FROM TT_OPN_CLS_REGISTER  , MM_CUSTOMER, MM_ACC_TYPE "
                            + " WHERE MM_CUSTOMER.ARDB_CD = {0} "
                            + " AND TT_OPN_CLS_REGISTER.CUST_CD = MM_CUSTOMER.CUST_CD AND TT_OPN_CLS_REGISTER.ACC_TYPE_CD = MM_ACC_TYPE.ACC_TYPE_CD ";

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

                            var parm2 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.from_dt;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.to_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.brn_cd;
                            command.Parameters.Add(parm4);

                            var parm5 = new OracleParameter("as_flag", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm5.Value = prp.flag;
                            command.Parameters.Add(parm5);

                            command.ExecuteNonQuery();
                            //transaction.Commit();
                        }
                        _statement = string.Format(_query1, string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"));
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_opn_cls_register();
                                        tca.acc_type_cd = UtilityM.CheckNull<Int64>(reader["ACC_TYPE_CD"]);
                                        tca.acc_type_desc = UtilityM.CheckNull<String>(reader["ACC_TYPE_DESC"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_cd = UtilityM.CheckNull<Int64>(reader["CUST_CD"]);
                                        tca.opn_cls_dt = UtilityM.CheckNull<DateTime>(reader["OPN_CLS_DT"]);
                                        tca.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT_AMT"]);
                                        tca.penal_amt = UtilityM.CheckNull<decimal>(reader["PENAL_AMT"]);
                                        tca.status = UtilityM.CheckNull<string>(reader["STATUS"]);
                                        tca.intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                        tca.dep_period = UtilityM.CheckNull<string>(reader["DEP_PERIOD"]);
                                        tca.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
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


        internal List<tm_deposit> PopulateNearMatDetails(p_report_param prp)
        {
            List<tm_deposit> tcaRet = new List<tm_deposit>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = " SELECT MM_ACC_TYPE.ACC_TYPE_DESC,       "
                                + " TM_DEPOSIT.ACC_NUM,                     "
                                + " MM_CUSTOMER.CUST_NAME,                  "
                                + " TM_DEPOSIT.OPENING_DT,                  "
                                + " TM_DEPOSIT.MAT_DT,                      "
                                + " DECODE(TM_DEPOSIT.ACC_TYPE_CD , 6,TM_DEPOSIT.INSTL_AMT*TM_DEPOSIT.INSTL_NO,TM_DEPOSIT.PRN_AMT)  PRN_AMT,                     "
                                + " NVL(TM_DEPOSIT.LOCK_MODE,'U') LOCK_MODE, "
                                + " NVL(TM_DEPOSIT.INSTL_AMT,1) INSTL_AMT,  "
                                + " NVL(TM_DEPOSIT.INSTL_NO,1) INSTL_NO,    "
                                + " TM_DEPOSIT.INTT_RT,                     "
                                + " TM_DEPOSIT.INTT_TRF_TYPE,               "
                                + " TM_DEPOSIT.ACC_TYPE_CD,                 "
                                + " DECODE(TM_DEPOSIT.ACC_TYPE_CD , 6, f_calcrdintt_reg({0},TM_DEPOSIT.ACC_NUM, "
                                + " TM_DEPOSIT.INSTL_AMT,    "
                                + " TM_DEPOSIT.INSTL_NO,     "
                                + " TM_DEPOSIT.INTT_RT),     "
                                + " f_calctdintt_reg(TM_DEPOSIT.ACC_TYPE_CD,  "
                                + " TM_DEPOSIT.PRN_AMT,       "
                                + " TM_DEPOSIT.OPENING_DT,    "
                                + " TM_DEPOSIT.INTT_TRF_TYPE, "
                                + " TM_DEPOSIT.MAT_DT - TM_DEPOSIT.OPENING_DT, "
                                + " TM_DEPOSIT.INTT_RT)) INTT"
                                + " FROM TM_DEPOSIT,         "
                                + " MM_ACC_TYPE,             "
                                + " MM_CUSTOMER              "
                                + " WHERE TM_DEPOSIT.ARDB_CD = {1} and TM_DEPOSIT.BRN_CD = {2} and             "
                                + " TM_DEPOSIT.ACC_TYPE_CD = MM_ACC_TYPE.ACC_TYPE_CD and "
                                + " TM_DEPOSIT.ARDB_CD = MM_CUSTOMER.ARDB_CD and TM_DEPOSIT.CUST_CD = MM_CUSTOMER.CUST_CD and         "
                                + " NVL(TM_DEPOSIT.ACC_STATUS,'O') <> 'C' and            "
                                + " (TM_DEPOSIT.RENEW_ID <> 1  and                       "
                                + " TM_DEPOSIT.RENEW_ID = 0 ) and                        "
                                + " (TM_DEPOSIT.MAT_DT BETWEEN to_date('{3}','dd-mm-yyyy' ) AND to_date('{4}','dd-mm-yyyy' ) ) and "
                                + " TM_DEPOSIT.ACC_TYPE_CD not in ( 1, 7 ) ";

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
                        _statement = string.Format(_query1,
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                           prp.from_dt != null ? prp.from_dt.ToString("dd/MM/yyyy") : "from_dt",
                                           prp.to_dt != null ? prp.to_dt.ToString("dd/MM/yyyy") : "to_dt"
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tm_deposit();
                                        tca.acc_type_desc = UtilityM.CheckNull<string>(reader["ACC_TYPE_DESC"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                        tca.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.lock_mode = UtilityM.CheckNull<string>(reader["LOCK_MODE"]);
                                        tca.instl_amt = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                        tca.instl_no = UtilityM.CheckNull<decimal>(reader["INSTL_NO"]);
                                        tca.intt_rt = UtilityM.CheckNull<decimal>(reader["INTT_RT"]);
                                        tca.intt_trf_type = UtilityM.CheckNull<string>(reader["INTT_TRF_TYPE"]);
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.intt_amt = UtilityM.CheckNull<decimal>(reader["INTT"]);
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


        internal List<passbook_print> PassBookPrint(p_report_param prp)
        {
            List<passbook_print> passBookPrint = new List<passbook_print>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            // prp.from_dt = prp.from_dt.Date;
            // prp.to_dt = prp.to_dt.Date.AddHours(23).AddMinutes(59).AddSeconds(59);

            string _query = " SELECT MD_PASSBOOK_PRINT_STATUS.TRANS_DT,         "
                  + " MD_PASSBOOK_PRINT_STATUS.TRANS_CD,                        "
                  + " MD_PASSBOOK_PRINT_STATUS.ACC_TYPE_CD,                     "
                  + " MD_PASSBOOK_PRINT_STATUS.ACC_NUM,                         "
                  + " MD_PASSBOOK_PRINT_STATUS.TRANS_TYPE,                      "
                  + " MD_PASSBOOK_PRINT_STATUS.INSTRUMENT_NUM,                  "
                  + " MD_PASSBOOK_PRINT_STATUS.AMOUNT,                          "
                  + " UPPER(MD_PASSBOOK_PRINT_STATUS.PARTICULARS) PARTICULARS,  "
                  + " MD_PASSBOOK_PRINT_STATUS.PRINTED_FLAG,                    "
                  + " f_passbook_balance({0},{1}, {2}, to_date('{3}', 'dd-mm-yyyy hh24:mi:ss') ) BALANCE_AMT,            "
                  + " 0 RUNNING_BAL,                                                             "
                  + " 0 ROWCD                                                                    "
                  + " FROM MD_PASSBOOK_PRINT_STATUS                                              "
                  + " WHERE ((MD_PASSBOOK_PRINT_STATUS.ACC_TYPE_CD = {4} ) AND                   "
                  + "       (MD_PASSBOOK_PRINT_STATUS.ACC_NUM = '{5}' ))                         "
                  + " AND   (MD_PASSBOOK_PRINT_STATUS.ARDB_CD = '{6}') AND (MD_PASSBOOK_PRINT_STATUS.PRINTED_FLAG = 'N')                        "
                  + " AND   (MD_PASSBOOK_PRINT_STATUS.TRANS_DT BETWEEN to_date('{7}', 'dd-mm-yyyy hh24:mi:ss') AND to_date('{8}', 'dd-mm-yyyy hh24:mi:ss') ) "
                  + " ORDER BY MD_PASSBOOK_PRINT_STATUS.TRANS_DT, MD_PASSBOOK_PRINT_STATUS.TRANS_CD ";

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
                                                   prp.ardb_cd, 
                                                   prp.acc_type_cd.ToString(),
                                                   string.Concat("'", prp.acc_num, "'"),
                                                   prp.to_dt.ToString("dd/MM/yyyy HH:mm:ss"),
                                                   prp.acc_type_cd.ToString(),
                                                   prp.acc_num,
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
                                        var pb = new passbook_print();

                                        pb.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        pb.trans_cd = UtilityM.CheckNull<decimal>(reader["TRANS_CD"]);
                                        // pb.acc_type_cd = UtilityM.CheckNull<Int16>(reader["ACC_TYPE_CD"]);
                                        pb.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        pb.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                        pb.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                        pb.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        pb.printed_flag = UtilityM.CheckNull<string>(reader["PRINTED_FLAG"]);
                                        pb.balance_amt = UtilityM.CheckNull<decimal>(reader["BALANCE_AMT"]);
                                        pb.running_bal = UtilityM.CheckNull<decimal>(reader["RUNNING_BAL"]);
                                        pb.rowcd = UtilityM.CheckNull<decimal>(reader["ROWCD"]);

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



        internal List<passbook_print> GetUpdatePassbookData(p_report_param prp)
        {
            List<passbook_print> passBookPrint = new List<passbook_print>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            // prp.from_dt = prp.from_dt.Date;
            // prp.to_dt = prp.to_dt.Date.AddHours(23).AddMinutes(59).AddSeconds(59);

            string _query = " SELECT MD_PASSBOOK_PRINT_STATUS.TRANS_DT,         "
                  + " MD_PASSBOOK_PRINT_STATUS.TRANS_CD,                        "
                  + " MD_PASSBOOK_PRINT_STATUS.ACC_TYPE_CD,                     "
                  + " MD_PASSBOOK_PRINT_STATUS.ACC_NUM,                         "
                  + " MD_PASSBOOK_PRINT_STATUS.TRANS_TYPE,                      "
                  + " MD_PASSBOOK_PRINT_STATUS.INSTRUMENT_NUM,                  "
                  + " MD_PASSBOOK_PRINT_STATUS.AMOUNT,                          "
                  + " UPPER(MD_PASSBOOK_PRINT_STATUS.PARTICULARS) PARTICULARS,  "
                  + " MD_PASSBOOK_PRINT_STATUS.PRINTED_FLAG,                    "
                  + " f_passbook_balance({0},{1}, {2}, to_date('{3}', 'dd-mm-yyyy hh24:mi:ss') ) BALANCE_AMT,            "
                  + " 0 RUNNING_BAL,                                                             "
                  + " 0 ROWCD                                                                    "
                  + " FROM MD_PASSBOOK_PRINT_STATUS                                              "
                  + " WHERE ((MD_PASSBOOK_PRINT_STATUS.ACC_TYPE_CD = {4} ) AND                   "
                  + "       (MD_PASSBOOK_PRINT_STATUS.ACC_NUM = '{5}' ))                         "
                  + " AND   (MD_PASSBOOK_PRINT_STATUS.ARDB_CD = '{6}') AND (MD_PASSBOOK_PRINT_STATUS.PRINTED_FLAG IN ('N','Y'))                        "
                  + " AND   (MD_PASSBOOK_PRINT_STATUS.TRANS_DT BETWEEN to_date('{7}', 'dd-mm-yyyy hh24:mi:ss') AND to_date('{8}', 'dd-mm-yyyy hh24:mi:ss') ) "
                  + " ORDER BY MD_PASSBOOK_PRINT_STATUS.TRANS_DT, MD_PASSBOOK_PRINT_STATUS.TRANS_CD ";

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
                                                   prp.ardb_cd,
                                                   prp.acc_type_cd.ToString(),
                                                   prp.acc_num,
                                                   prp.to_dt.ToString("dd/MM/yyyy HH:mm:ss"),
                                                   prp.acc_type_cd.ToString(),
                                                   prp.acc_num,
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
                                        var pb = new passbook_print();

                                        pb.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        pb.trans_cd = UtilityM.CheckNull<decimal>(reader["TRANS_CD"]);
                                       // pb.acc_type_cd = UtilityM.CheckNull<Int16>(reader["ACC_TYPE_CD"]);
                                        pb.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        pb.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                        pb.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                        pb.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                        pb.printed_flag = UtilityM.CheckNull<string>(reader["PRINTED_FLAG"]);
                                        pb.balance_amt = UtilityM.CheckNull<decimal>(reader["BALANCE_AMT"]);
                                        pb.running_bal = UtilityM.CheckNull<decimal>(reader["RUNNING_BAL"]);
                                        pb.rowcd = UtilityM.CheckNull<decimal>(reader["ROWCD"]);

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


        internal int UpdatePassbookData(List<passbook_print> prp)
        {
            int _ret = 0;

           string _query = "UPDATE MD_PASSBOOK_PRINT_STATUS  "
                           + " SET PRINTED_FLAG = {0} "
                           + " WHERE ARDB_CD = {1} "
                           + " AND ACC_TYPE_CD = {2} "
                           + " AND ACC_NUM = {3} "
                           + " AND TRANS_DT = to_date({4},'DD/MM/YYYY') "
                           + " AND TRANS_CD = {5} "
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
                        prp[i].acc_type_cd,
                        string.Concat("'", prp[i].acc_num, "'"),
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


        internal int UpdatePassbookline(p_report_param prp)
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
                                                    prp.acc_type_cd,
                                                    string.Concat("'", prp.acc_num, "'"));

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



        internal int GetPassbookline(p_report_param prp)
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
                                                   prp.acc_type_cd,
                                                   prp.acc_num,
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
                        lines_printed = -1 ;
                    }
                }
            }

            return lines_printed;
        }



        internal List<tt_td_intt_cert> GetInterestCertificate(p_report_param prp)
        {
            List<tt_td_intt_cert> tcaRet = new List<tt_td_intt_cert>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_td_intt_cert";
            string _query1 = " SELECT  MM_ACC_TYPE.ACC_TYPE_DESC, "
                            + " TT_TD_INTT_CERT.ACC_NUM,           "
                            + " TT_TD_INTT_CERT.CUST_NAME,           "
                            + " TT_TD_INTT_CERT.GUARDIAN_NAME,           "
                            + " TT_TD_INTT_CERT.ADDRESS,        "
                            + " TT_TD_INTT_CERT.OPENING_DT,           "
                            + " TT_TD_INTT_CERT.MAT_DT,          "
                            + " TT_TD_INTT_CERT.PRN_AMT,         "
                            + " TT_TD_INTT_CERT.INTT_RT,           "
                            + " TT_TD_INTT_CERT.PROV_INTT_AMT,        "
                            + " TT_TD_INTT_CERT.ACC_CLOSE_DT           "
                            + " FROM TT_TD_INTT_CERT,MM_ACC_TYPE "
                            + " WHERE TT_TD_INTT_CERT.ACC_TYPE_CD = MM_ACC_TYPE.ACC_TYPE_CD ";

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

                            var parm2 = new OracleParameter("as_cust_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = prp.cust_cd;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm3.Value = prp.from_dt;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm4.Value = prp.to_dt;
                            command.Parameters.Add(parm4);                            

                            command.ExecuteNonQuery();
                            
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
                                        var tca = new tt_td_intt_cert();
                                        tca.acc_type_cd = UtilityM.CheckNull<string>(reader["ACC_TYPE_DESC"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                        tca.guardian_name = UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);
                                        tca.address = UtilityM.CheckNull<string>(reader["ADDRESS"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.mat_dt = UtilityM.CheckNull<DateTime>(reader["MAT_DT"]);
                                        tca.prn_amt = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.intt_rt = UtilityM.CheckNull<Single>(reader["INTT_RT"]);
                                        tca.prov_intt_amt = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
                                        tca.acc_close_dt = UtilityM.CheckNull<DateTime>(reader["ACC_CLOSE_DT"]);

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


        internal List<sb_intt_list> POPULATE_SB_INTT(p_gen_param prm)
        {
            List<sb_intt_list> tcaRet = new List<sb_intt_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = " SELECT  TT_SB_INTT.ACC_NUM, "
                            + " TT_SB_INTT.CONSTITUTION_CD,           "
                            + " TT_SB_INTT.INTT_AMT,           "
                            + " MM_CUSTOMER.CUST_NAME,           "
                            + " TM_DEPOSIT.CLR_BAL        "
                            + " FROM TT_SB_INTT,TM_DEPOSIT,MM_CUSTOMER "
                            + " WHERE TT_SB_INTT.ACC_NUM = TM_DEPOSIT.ACC_NUM "
                            + " AND TT_SB_INTT.CONSTITUTION_CD = TM_DEPOSIT.CONSTITUTION_CD "
                            + " AND TM_DEPOSIT.CUST_CD = MM_CUSTOMER.CUST_CD "
                            + " AND TM_DEPOSIT.ACC_TYPE_CD IN (1,8) ";

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

                        using (var command = OrclDbConnection.Command(connection, "P_SB_PRODUCT_PATCH_NEW"))
                        {
                            //prm.from_dt = prm.from_dt.AddDays(1);
                            // ad_acc_type_cd NUMBER,as_cust_name VARCHAR2
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("ad_acc_type", OracleDbType.Decimal, ParameterDirection.Input);
                            parm.Value = prm.ad_acc_type_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_acc_num", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value =  prm.as_acc_num;
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
                                using (var command2 = OrclDbConnection.Command(connection, "P_SB_INTT"))
                                {
                                    command2.CommandType = System.Data.CommandType.StoredProcedure;
                                    parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm.Value = prm.ardb_cd;
                                    command2.Parameters.Add(parm);
                                    
                                    using (var reader2 = command2.ExecuteReader())
                                    {
                                        _statement = string.Format(_query1);
                                        
                                        try
                                        {
                                            using (var command1 = OrclDbConnection.Command(connection, _statement))
                                            {
                                                using (var reader1 = command1.ExecuteReader())
                                                {
                                                    if (reader1.HasRows)
                                                    {
                                                        while (reader1.Read())
                                                        {
                                                            var tca = new sb_intt_list();
                                                            tca.acc_num = UtilityM.CheckNull<string>(reader1["ACC_NUM"]);
                                                            tca.name = UtilityM.CheckNull<string>(reader1["CUST_NAME"]);
                                                            tca.constitution_cd = UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"]);
                                                            tca.interest = UtilityM.CheckNull<decimal>(reader1["INTT_AMT"]);
                                                            tca.balance = UtilityM.CheckNull<decimal>(reader1["CLR_BAL"]);                                                           
                                                            tcaRet.Add(tca);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            transaction.Rollback();
                                            tcaRet = null;
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
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }


        internal int POST_SB_INTT(p_gen_param prm)
        {
            int _ret = 0;

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

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
                        using (var command = OrclDbConnection.Command(connection, "P_POP_SB_INTT"))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_effect_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.adt_trans_dt;
                            command.Parameters.Add(parm);                            
                            parm = new OracleParameter("as_user", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.gs_user_id;
                            command.Parameters.Add(parm);

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



        internal List<sb_intt_list> POPULATE_SMS_CHARGE(p_gen_param prm)
        {
            List<sb_intt_list> tcaRet = new List<sb_intt_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query1 = " SELECT  TT_SMS_CHARGE.ACC_NUM, "
                            + " TT_SMS_CHARGE.CONSTITUTION_CD,           "
                            + " TT_SMS_CHARGE.AMOUNT,           "
                            + " MM_CUSTOMER.CUST_NAME,           "
                            + " TM_DEPOSIT.CLR_BAL        "
                            + " FROM TT_SMS_CHARGE,TM_DEPOSIT,MM_CUSTOMER "
                            + " WHERE TT_SMS_CHARGE.ACC_NUM = TM_DEPOSIT.ACC_NUM "
                            + " AND TT_SMS_CHARGE.CONSTITUTION_CD = TM_DEPOSIT.CONSTITUTION_CD "
                            + " AND TM_DEPOSIT.CUST_CD = MM_CUSTOMER.CUST_CD "
                            + " AND TM_DEPOSIT.ACC_TYPE_CD IN (1,8) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {

                    using (var command = OrclDbConnection.Command(connection, _alter))
                    {
                        command.ExecuteNonQuery();
                    }

                    // transaction.Commit();
                    using (var command2 = OrclDbConnection.Command(connection, "P_DEDUCT_SMS"))
                    {
                        command2.CommandType = System.Data.CommandType.StoredProcedure;
                        var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                        parm.Value = prm.ardb_cd;
                        command2.Parameters.Add(parm);

                        using (var reader2 = command2.ExecuteReader())
                        {
                            _statement = string.Format(_query1);

                            try
                            {
                                using (var command1 = OrclDbConnection.Command(connection, _statement))
                                {
                                    using (var reader1 = command1.ExecuteReader())
                                    {
                                        if (reader1.HasRows)
                                        {
                                            while (reader1.Read())
                                            {
                                                var tca = new sb_intt_list();
                                                tca.acc_num = UtilityM.CheckNull<string>(reader1["ACC_NUM"]);
                                                tca.name = UtilityM.CheckNull<string>(reader1["CUST_NAME"]);
                                                tca.constitution_cd = UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"]);
                                                tca.interest = UtilityM.CheckNull<decimal>(reader1["AMOUNT"]);
                                                tca.balance = UtilityM.CheckNull<decimal>(reader1["CLR_BAL"]);
                                                tcaRet.Add(tca);
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                transaction.Rollback();
                                tcaRet = null;
                            }
                        }
                    }
                }                
            }
            return tcaRet;
        }


        internal int POST_SMS_CHARGE(p_gen_param prm)
        {
            int _ret = 0;

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

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
                        using (var command = OrclDbConnection.Command(connection, "P_POP_SMS_INTT"))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("adt_effect_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.adt_trans_dt;
                            command.Parameters.Add(parm);
                            
                            command.ExecuteNonQuery();

                            transaction.Commit();
                            //transaction.Rollback();

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



        internal List<tt_agent_comm> GetAgentCommission(p_gen_param prp)
        {
            List<tt_agent_comm> tcaRet = new List<tt_agent_comm>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_agent_commision";
            string _query1 = " SELECT MM_AGENT.AGENT_CD, "
                            + " MM_AGENT.AGENT_NAME,           "
                            + " TT_AGENT_COMM.ARDB_CD,           "
                            + " TT_AGENT_COMM.BRN_CD,           "
                            + " TT_AGENT_COMM.PAID_AMT1,        "
                            + " TT_AGENT_COMM.COMM1,           "
                            + " TT_AGENT_COMM.SEC_AMT1,          "
                            + " TT_AGENT_COMM.COMM3,         "
                            + " TT_AGENT_COMM.SEC_AMT3           "                           
                            + " FROM MM_AGENT,TT_AGENT_COMM "
                            + " WHERE MM_AGENT.ARDB_CD = TT_AGENT_COMM.ARDB_CD "
                            + " AND MM_AGENT.AGENT_CD = TT_AGENT_COMM.AGENT_CD "
                            + " AND MM_AGENT.BRN_CD = TT_AGENT_COMM.BRN_CD " 
                            + " AND TT_AGENT_COMM.BRN_CD  = {0} ";

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

                            var parm1 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm1.Value = prp.from_dt;
                            command.Parameters.Add(parm1);

                            var parm2 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm2.Value = prp.to_dt;
                            command.Parameters.Add(parm2);

                            var parm3 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm3.Value = prp.brn_cd;
                            command.Parameters.Add(parm3);

                            var parm4 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = prp.ardb_cd;
                            command.Parameters.Add(parm4);
                            

                            command.ExecuteNonQuery();

                        }
                        _statement = string.Format(_query1, string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tt_agent_comm();
                                        tca.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        tca.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        tca.agent_cd = UtilityM.CheckNull<string>(reader["AGENT_CD"]);
                                        tca.agent_name = UtilityM.CheckNull<string>(reader["AGENT_NAME"]);
                                        tca.deposited_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT1"]);
                                        tca.commission = UtilityM.CheckNull<decimal>(reader["COMM1"]);
                                        tca.interest = UtilityM.CheckNull<decimal>(reader["SEC_AMT1"]);
                                        tca.withdrawal = UtilityM.CheckNull<decimal>(reader["COMM3"]);
                                        tca.deduction = UtilityM.CheckNull<decimal>(reader["SEC_AMT3"]);
                                        

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



        internal int PostAgentCommission(p_gen_param prm)
        {
            int _ret = 0;

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

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
                        using (var command = OrclDbConnection.Command(connection, "P_COMM_TRF"))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.ardb_cd;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.brn_cd;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("adt_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm.Value = prm.adt_trans_dt;
                            command.Parameters.Add(parm);
                            parm = new OracleParameter("as_user", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = prm.gs_user_id;
                            command.Parameters.Add(parm);


                            command.ExecuteNonQuery();

                            //transaction.Rollback();
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



        internal int UpdateCertificateStatus(p_report_param prp)
        {
            int _ret = 0;

            string _query = "UPDATE TM_DEPOSIT  "
                            + " SET TRANSFER_FLAG = {0} "
                            + " WHERE ARDB_CD = {1} "
                            + " AND ACC_TYPE_CD = {2} "
                            + " AND ACC_NUM = {3} "
                            + " AND  RENEW_ID = '{4}' "
                            + " AND DEL_FLAG = 'N' ";



            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement = string.Format(_query,
                                                string.Concat("'", prp.print_status, "'"),
                                                string.Concat("'", prp.ardb_cd, "'"),
                                                prp.acc_type_cd,
                                                string.Concat("'", prp.acc_num, "'"),
                                                prp.renew_id);

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



        internal string GetCertificateStatus(p_report_param prp)
        {

            string lines_printed = null;

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            // prp.from_dt = prp.from_dt.Date;
            // prp.to_dt = prp.to_dt.Date.AddHours(23).AddMinutes(59).AddSeconds(59);

            string _query = " SELECT NVL(TRANSFER_FLAG,'N')    TRANSFER_FLAG     "
                  + " FROM TM_DEPOSIT                                              "
                  + " WHERE (TM_DEPOSIT.ACC_TYPE_CD = {0} ) AND                   "
                  + "       (TM_DEPOSIT.ACC_NUM = '{1}' )                         "
                  + " AND   (TM_DEPOSIT.ARDB_CD = '{2}') AND  (TM_DEPOSIT.RENEW_ID = '{3}') AND  (TM_DEPOSIT.DEL_FLAG = 'N')  ";

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
                                                   prp.acc_type_cd,
                                                   prp.acc_num,
                                                   prp.ardb_cd,
                                                   prp.renew_id);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        lines_printed = UtilityM.CheckNull<string>(reader["TRANSFER_FLAG"]);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        lines_printed = "Error";
                    }
                }
            }

            return lines_printed;
        }



        internal List<UserwisetransDM> GetUserwiseTransDepositDtls(p_report_param prp)
        {
            List<UserwisetransDM> loanDfltList = new List<UserwisetransDM>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _statement;

            string _query = " SELECT V_TRANS_DTLS.TRANS_DT,             "
                           + " (SELECT ACC_TYPE_DESC FROM MM_ACC_TYPE WHERE ACC_TYPE_CD = V_TRANS_DTLS.ACC_TYPE_CD) ACC_DESC,           "
                           + "  V_TRANS_DTLS.ACC_NUM LOAN_ID,              "
                           + " V_TRANS_DTLS.TRANS_CD,                "
                           + " Decode(V_TRANS_DTLS.TRANS_TYPE,'D','Deposit','Withdrawal') TRANS_TYPE,                  "
                           + " V_TRANS_DTLS.TRANS_MODE,                 "
                           + " V_TRANS_DTLS.AMOUNT,                     "
                           + "  Decode(V_TRANS_DTLS.TRF_TYPE,'C','Cash','Transfer') TRF_TYPE,                "
                           + " (V_TRANS_DTLS.CURR_PRN_RECOV) PRN_RECOV,                 "
                           + " (V_TRANS_DTLS.CURR_INTT_RECOV) INTT_RECOV,               "
                           + " V_TRANS_DTLS.CURR_INTT_RATE,             "
                           + " V_TRANS_DTLS.OVD_INTT_RATE," 
                           + " V_TRANS_DTLS.PARTICULARS,          "
                           + " (SELECT USER_FIRST_NAME || ' ' || nvl(USER_MIDDLE_NAME,'') || ' ' || USER_LAST_NAME FROM M_USER_MASTER WHERE USER_ID =SUBSTR(V_TRANS_DTLS.CREATED_BY,1, instr(V_TRANS_DTLS.CREATED_BY,'/',1)-1)) USER_NAME          "
                           + "  FROM V_TRANS_DTLS       "
                           + " WHERE(V_TRANS_DTLS.ARDB_CD = {0}) and  "
                           + " (V_TRANS_DTLS.TRANS_TYPE IN ('D','W')) and  "
                           + " (V_TRANS_DTLS.TRANS_DT  = substr({1},1,10)) and  "
                           + " (V_TRANS_DTLS.BRN_CD  = {2})  AND "
                           + " (V_TRANS_DTLS.CREATED_BY  = {3} ) AND "
                           + " (V_TRANS_DTLS.APPROVAL_STATUS = 'A' ) "
                           + " ORDER BY TRANS_TYPE,TRANS_CD " ;


            string _query1 = " SELECT  DISTINCT V_TRANS_DTLS.CREATED_BY USER_ID, "
                             + " (SELECT USER_FIRST_NAME || ' '|| nvl(USER_MIDDLE_NAME,'') || ' ' || USER_LAST_NAME  FROM M_USER_MASTER WHERE USER_ID =SUBSTR(V_TRANS_DTLS.CREATED_BY,1,instr(V_TRANS_DTLS.CREATED_BY,'/',1)-1)) USER_NAME"
                             + "  FROM V_TRANS_DTLS "
                             + "  WHERE(V_TRANS_DTLS.ARDB_CD = {0}) and "
                             + " (V_TRANS_DTLS.TRANS_TYPE IN ('D','W')) and "
                             + " (V_TRANS_DTLS.TRANS_DT  = substr({1},1,10)) and"
                             + " (V_TRANS_DTLS.BRN_CD  = {2}) and"
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

                                                        loanDtl.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                                        loanDtl.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                                        loanDtl.acc_desc = UtilityM.CheckNull<string>(reader["ACC_DESC"]);
                                                        loanDtl.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                                        loanDtl.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                                        loanDtl.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                                                        loanDtl.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                                        loanDtl.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
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





    }
}
