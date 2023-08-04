using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class DepositRenewTmpDL
    {
        string _statement;
        internal List<tm_deposit> GetDepositRenewTmp(tm_deposit dep)
        {
            List<tm_deposit> depoList = new List<tm_deposit>();

            string _query = " SELECT BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT,ARDB_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT_RENEW_TEMP                                                                  "
                            + " WHERE ARDB_CD = {0} AND BRN_CD={1} AND ACC_NUM={2}  AND ACC_TYPE_CD = {3} AND DEL_FLAG = 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
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
                                    d.instl_no = UtilityM.CheckNull<int>(reader["INSTL_NO"]);
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
                                    d.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
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
        internal decimal InsertDepositTemp(tm_deposit dep)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = " INSERT INTO TM_DEPOSIT_RENEW_TEMP ( BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                           + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                           + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                           + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                           + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                           + " BONUS_INTT_RT, TRANSFER_FLAG, TRANSFER_DT,ARDB_CD,DEL_FLAG   "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},to_date('{8}','dd-mm-yyyy' ),{9},{10},{11},{12},{13}, to_date('{14}','dd-mm-yyyy' ),"
                           + " {15},{16}, to_date('{17}','dd-mm-yyyy' ), to_date('{18}','dd-mm-yyyy' ),{19},{20},{21},{22},{23},{24}, "
                           + " {25},{26},{27},{28},{29}, to_date('{30}','dd-mm-yyyy' ),{31}, to_date('{32}','dd-mm-yyyy' ),{33}, "
                           + " to_date('{34}','dd-mm-yyyy' ),{35},{36}, to_date('{37}','dd-mm-yyyy' ),{38},{39},{40},{41},{42},{43},to_date('{44}','dd-mm-yyyy' ),{45},'N')";

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

            string _query = " UPDATE TM_DEPOSIT_RENEW_TEMP "
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
               + "WHERE ARDB_CD = {46} AND brn_cd = NVL({47}, brn_cd) AND acc_num = NVL('{48}',  acc_num ) AND  ACC_TYPE_CD = NVL('{49}',  ACC_TYPE_CD ) AND DEL_FLAG = 'N' ";

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
                       string.Concat("'", dep.ardb_cd, "'"),
                       string.Concat("'", dep.brn_cd, "'"),
                       string.Concat("'", dep.acc_num, "'"),
                       string.Concat("'", dep.acc_type_cd, "'")
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

            string _query = " UPDATE TM_DEPOSIT_RENEW_TEMP SET DEL_FLAG= 'Y'  "
                          + " WHERE ARDB_CD={0} AND brn_cd = {1} AND acc_num = {2} AND ACC_TYPE_CD={3} ";

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
                                              (dep.acc_type_cd > 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD"));

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
  }
}
