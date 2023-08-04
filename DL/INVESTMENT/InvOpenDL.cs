using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL.INVESTMENT
{
    public class InvOpenDL
    {
        string _statement;
        internal tm_deposit_inv GetInvDeposit(DbConnection connection, tm_deposit_inv dep)
        {
            tm_deposit_inv depRet = new tm_deposit_inv();
            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, BANK_CD, BRANCH_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT_INV  T1                                                                "
                            + " WHERE ARDB_CD = {0} AND BRN_CD={1} AND ACC_NUM={2} AND ACC_TYPE_CD={3} AND nvl(ACC_STATUS,'O') = 'O' AND DEL_FLAG = 'N' "
                            + " AND T1.RENEW_ID = ( SELECT MAX(RENEW_ID) FROM TM_DEPOSIT_INV T2 WHERE  T2.ARDB_CD = {4} AND  T1.BRN_CD = T2.BRN_CD AND T1.ACC_NUM = T2.ACC_NUM AND T1.ACC_TYPE_CD = T2.ACC_TYPE_CD )";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
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
                                var d = new tm_deposit_inv();
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
                                d.bank_cd = UtilityM.CheckNull<Int32>(reader["BANK_CD"]);
                                d.branch_cd = UtilityM.CheckNull<Int32>(reader["BRANCH_CD"]);
                                d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }

        internal tm_deposit_inv GetDepositRenewTemp(DbConnection connection, tm_deposit_inv dep)
        {
            tm_deposit_inv depRet = new tm_deposit_inv();
            string _query = " SELECT ARDB_CD,BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD, "
                            + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO,   "
                            + " MAT_DT, INTT_RT, TDS_APPLICABLE, LAST_INTT_CALC_DT, ACC_CLOSE_DT,                "
                            + " CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS, "
                            + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG,                         "
                            + " CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT, APPROVAL_STATUS, APPROVED_BY,       "
                            + " APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,     "
                            + " BONUS_INTT_RT, BANK_CD,BRANCH_CD,DEL_FLAG                                   "
                            + " FROM TM_DEPOSIT_RENEW_INV                                                                  "
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
                                var d = new tm_deposit_inv();
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
                                d.bank_cd = UtilityM.CheckNull<Int32>(reader["BANK_CD"]);
                                d.branch_cd = UtilityM.CheckNull<Int32>(reader["BRANCH_CD"]);
                                d.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }

        internal td_def_trans_trf GetDepTrans(DbConnection connection, tm_deposit_inv tdt)
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


        internal List<tm_transfer> GetTransfer(DbConnection connection, tm_deposit_inv tdt)
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
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }

            return tdtRets;
        }

        internal List<td_def_trans_trf> GetDepTransTrf(DbConnection connection, tm_deposit_inv tdt)
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


        internal InvOpenDM GetInvOpeningData(tm_deposit_inv td)
        {
            InvOpenDM InvOpenDMRet = new InvOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        InvOpenDMRet.tmdepositInv = GetInvDeposit(connection, td);
                        InvOpenDMRet.tmdepositrenewInv = GetDepositRenewTemp(connection, td);
                        InvOpenDMRet.tddeftrans = GetDepTrans(connection, td);
                        if (!String.IsNullOrWhiteSpace(InvOpenDMRet.tddeftrans.trans_cd.ToString()) && InvOpenDMRet.tddeftrans.trans_cd > 0)
                        {
                            td.trans_cd = InvOpenDMRet.tddeftrans.trans_cd;
                            td.trans_dt = InvOpenDMRet.tddeftrans.trans_dt;
                            InvOpenDMRet.tmtransfer = GetTransfer(connection, td);
                            InvOpenDMRet.tddeftranstrf = GetDepTransTrf(connection, td);
                        }
                        // transaction.Commit();
                        return InvOpenDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
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



        internal string InsertInvOpeningData(InvOpenDM acc)
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
                        _section = "InsertDepositInv";
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositInv.acc_num))
                            InsertDepositInv(connection, acc.tmdepositInv);
                        _section = "InsertDepositRenewInv";
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositrenewInv.acc_num))
                            InsertDepositRenewInv(connection, acc.tmdepositrenewInv, maxTransCD);
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


        internal bool InsertDepositInv(DbConnection connection, tm_deposit_inv dep)
        {
            string _query = " INSERT INTO TM_DEPOSIT_INV ( BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                           + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                           + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                           + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                           + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                           + " BONUS_INTT_RT, BANK_CD, BRANCH_CD,ARDB_CD,DEL_FLAG )  "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                           + " {15},{16}, {17}, {18},{19},{20},{21},{22},{23},{24}, "
                           + " {25},{26},{27},{28},{29}, SYSDATE,{30},SYSDATE,{31}, "
                           + " {32},{33},{34}, {35},{36},{37},{38},{39},{40},{41},{42},{43},'N')";

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
            string.Concat("'", dep.bank_cd, "'"),
            string.Concat("'", dep.branch_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'")
                                         );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }


        internal bool InsertDepositRenewInv(DbConnection connection, tm_deposit_inv dep, int transcd)
        {
            string _query = " INSERT INTO TM_DEPOSIT_RENEW_INV (BRN_CD, ACC_TYPE_CD, ACC_NUM, RENEW_ID, CUST_CD, INTT_TRF_TYPE, CONSTITUTION_CD,"
                        + " OPRN_INSTR_CD, OPENING_DT, PRN_AMT, INTT_AMT, DEP_PERIOD, INSTL_AMT, INSTL_NO, MAT_DT, INTT_RT, TDS_APPLICABLE,     "
                        + " LAST_INTT_CALC_DT, ACC_CLOSE_DT, CLOSING_PRN_AMT, CLOSING_INTT_AMT, PENAL_AMT, EXT_INSTL_TOT, MAT_STATUS, ACC_STATUS,"
                        + " CURR_BAL, CLR_BAL, STANDING_INSTR_FLAG, CHEQUE_FACILITY_FLAG, CREATED_BY, CREATED_DT, MODIFIED_BY, MODIFIED_DT,      "
                        + " APPROVAL_STATUS, APPROVED_BY, APPROVED_DT, USER_ACC_NUM, LOCK_MODE, LOAN_ID, CERT_NO, BONUS_AMT, PENAL_INTT_RT,      "
                        + " BONUS_INTT_RT, BANK_CD, BRANCH_CD,ARDB_CD,DEL_FLAG )  "
                        + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}, {14},"
                        + " {15},{16}, {17}, {18},{19},{20},{21},{22},{23},{24}, "
                        + " {25},{26},{27},{28},{29}, SYSDATE,{30},SYSDATE,{31}, "
                        + " {32},{33},{34}, {35},{36},{37},{38},{39},{40},{41},{42},{43},'N')";

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
            string.Concat("'", dep.bank_cd, "'"),
            string.Concat("'", dep.branch_cd, "'"),
            string.Concat("'", dep.ardb_cd, "'")
                                         );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
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


        internal int UpdateInvOpeningData(InvOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositInv.acc_num))
                            UpdateDepositInv(connection, acc.tmdepositInv);
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositrenewInv.acc_num))
                            UpdateDepositRenewInv(connection, acc.tmdepositrenewInv);
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


        internal bool UpdateDepositInv(DbConnection connection, tm_deposit_inv dep)
        {
            string _query = " UPDATE TM_DEPOSIT_INV SET "
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
                  + "bank_cd              = NVL({43}, bank_cd ),"
                  + "branch_cd             = NVL({44}, branch_cd            ) "
                  + "WHERE ardb_cd={45} and brn_cd = NVL({46}, brn_cd) AND acc_num = NVL({47},  acc_num ) AND acc_type_cd=NVL({48},  acc_type_cd ) ";

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
            string.Concat("'", dep.bank_cd, "'"),
            string.Concat("'", dep.branch_cd, "'"),
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


        internal bool UpdateDepositRenewInv(DbConnection connection, tm_deposit_inv dep)
        {
            string _query = " UPDATE TM_DEPOSIT_RENEW_INV SET "
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
                  + "bank_cd              = NVL({43}, bank_cd ),"
                  + "branch_cd             = NVL({44}, branch_cd            ) "
                  + "WHERE ardb_cd = {45} And brn_cd = NVL({46}, brn_cd) AND acc_num = NVL({47},  acc_num ) AND acc_type_cd=NVL({48},  acc_type_cd ) And Del_FLAG='N' And user_acc_num={49} ";

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
            string.Concat("'", dep.bank_cd, "'"),
            string.Concat("'", dep.branch_cd, "'"),
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


        internal int DeleteInvOpeningData(td_def_trans_trf acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.acc_num))
                        {
                            if (acc.trans_mode == "O") { 
                                DeleteDepositInv(connection, acc);
                                }
                            DeleteDepositRenewInv(connection, acc);

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


        internal bool DeleteDepositInv(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE TM_DEPOSIT_INV SET DEL_FLAG = 'Y'  "
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

        internal bool DeleteDepositRenewInv(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE  TM_DEPOSIT_RENEW_INV  SET DEL_FLAG = 'Y' "
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

        internal decimal F_CALCTDINTT_INV_REG(p_gen_param prp)
        {
            decimal amount = 0;
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "SELECT F_CALCTDINTT_INV_REG({0},{1},to_date('{2}','dd-mm-yyyy' ),{3},{4},{5}) AMOUNT FROM DUAL";
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


        internal string ApproveInvTranaction(p_gen_param pgp)
        {
            string _ret = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
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
                        _ret = _dl1.P_UPDATE_TD_DEP_TRANS_INVEST(connection, pgp);
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


        internal List<conswise_sb_dl> PopulateDLFixedDepositInvAll(p_report_param prp)
        {
            List<conswise_sb_dl> tcaRet = new List<conswise_sb_dl>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = " p_td_prov_intt_detail_list_inv";
            string _query1 = " SELECT DISTINCT  TT_TDPROV_INTT_INV.ACC_TYPE_CD, "
                                + " TT_TDPROV_INTT_INV.ACC_NUM,       "
                                + " TT_TDPROV_INTT_INV.OPENING_DT,    "
                                + " TT_TDPROV_INTT_INV.MAT_DT,        "
                                + " TT_TDPROV_INTT_INV.PRN_AMT,       "
                                + " TT_TDPROV_INTT_INV.INTT_RT,       "
                                + " TT_TDPROV_INTT_INV.PROV_INTT_AMT, "
                                + " TT_TDPROV_INTT_INV.CONSTITUTION_CD,"
                                + " TT_TDPROV_INTT_INV.BANK_CD,   "
                                + " TT_TDPROV_INTT_INV.BRANH_CD   "
                                + " FROM TT_TDPROV_INTT_INV"
                                + " WHERE  ( TT_TDPROV_INTT_INV.CONSTITUTION_CD = {0} ) AND  "
                                + "  ( TT_TDPROV_INTT_INV.BANK_CD = {1} ) AND  "
                                + " ( TT_TDPROV_INTT_INV.BRANH_CD = {2} )    "
                                + "  ORDER BY TT_TDPROV_INTT_INV.ACC_NUM ";


            string _query2 = " SELECT  DISTINCT TT_TDPROV_INTT_INV.CONSTITUTION_CD,TT_TDPROV_INTT_INV.BANK_CD,TT_TDPROV_INTT_INV.BRANH_CD "
                            + " FROM TT_TDPROV_INTT_INV ";




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
                           
                            command.ExecuteNonQuery();
                           // transaction.Commit();
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
                                        
                                        tca1.constitution_desc = UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"]);
                                        tca1.bank_cd = UtilityM.CheckNull<string>(reader1["BANK_CD"]);
                                        tca1.branch_cd = UtilityM.CheckNull<string>(reader1["BRANH_CD"]);

                                        _statement = string.Format(_query1,
                                      string.Concat("'", UtilityM.CheckNull<string>(reader1["CONSTITUTION_CD"]), "'"),
                                      string.Concat("'", UtilityM.CheckNull<string>(reader1["BANK_CD"]), "'"),
                                      string.Concat("'", tca1.branch_cd = UtilityM.CheckNull<string>(reader1["BRANH_CD"]),"'"));
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
                                                        tca.bank_name = UtilityM.CheckNull<string>(reader["BANK_CD"]);
                                                        tca.branch_name = UtilityM.CheckNull<string>(reader["BRANH_CD"]);
                                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                                        //var x = UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2);
                                                        // tca.constitution_cd = Convert.ToInt16(UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]).Substring(0, 2).Trim());
                                                        //tca.INSTL_AMT = UtilityM.CheckNull<decimal>(reader["INSTL_AMT"]);
                                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                                        tca.INTT_RT = Convert.ToDecimal(UtilityM.CheckNull<float>(reader["INTT_RT"]));
                                                        tca.PROV_INTT_AMT = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
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


        internal List<tt_sbca_dtl_list> PopulateNearInvReport(p_report_param prp)
        {
            List<tt_sbca_dtl_list> tcaRet = new List<tt_sbca_dtl_list>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "p_near_mat_inv";
            string _query1 = " SELECT DISTINCT  TT_TDPROV_INTT_INV.ACC_TYPE_CD, "
                                + " TT_TDPROV_INTT_INV.ACC_NUM,       "
                                + " TT_TDPROV_INTT_INV.OPENING_DT,    "
                                + " TT_TDPROV_INTT_INV.MAT_DT,        "
                                + " TT_TDPROV_INTT_INV.PRN_AMT,       "
                                + " TT_TDPROV_INTT_INV.INTT_RT,       "
                                + " TT_TDPROV_INTT_INV.PROV_INTT_AMT, "
                                + " TT_TDPROV_INTT_INV.CONSTITUTION_CD,"
                                + " TT_TDPROV_INTT_INV.BANK_CD,   "
                                + " TT_TDPROV_INTT_INV.BRANH_CD   "
                                + " FROM TT_TDPROV_INTT_INV";
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
                            var parm2 = new OracleParameter("adt_form_dt", OracleDbType.Date, ParameterDirection.Input);
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
                                        var tca = new tt_sbca_dtl_list();
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.bank_name = UtilityM.CheckNull<string>(reader["BANK_CD"]);
                                        tca.branch_name = UtilityM.CheckNull<string>(reader["BRANH_CD"]);
                                        tca.constitution_desc = UtilityM.CheckNull<string>(reader["CONSTITUTION_CD"]);
                                        tca.opening_dt = UtilityM.CheckNull<DateTime>(reader["OPENING_DT"]);
                                        tca.PRN_AMT = UtilityM.CheckNull<decimal>(reader["PRN_AMT"]);
                                        tca.INTT_RT = Convert.ToDecimal(UtilityM.CheckNull<float>(reader["INTT_RT"]));
                                        tca.PROV_INTT_AMT = UtilityM.CheckNull<decimal>(reader["PROV_INTT_AMT"]);
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



    }
}
