using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class DepTransactionDL
    {
        string _statement;

        internal td_def_trans_trf GetDepTransSingle(DbConnection connection,td_def_trans_trf tdt)
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
         + " BRN_CD,ARDB_CD,DEL_FLAG"
         + " FROM TD_DEP_TRANS"
         + " WHERE ARDB_CD = {0} AND BRN_CD = {1} "
        + " AND   TRANS_CD = {2} "
        + " AND TRANS_DT = to_date('{3}','dd-mm-yyyy' ) AND DEL_FLAG = 'N' " ;
            _statement = string.Format(_query,
                                              !string.IsNullOrWhiteSpace(tdt.ardb_cd) ? string.Concat("'", tdt.ardb_cd, "'") : "ardb_cd",
                                              !string.IsNullOrWhiteSpace(tdt.brn_cd) ? string.Concat("'", tdt.brn_cd, "'") : "brn_cd",
                                              tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd",
                                               (tdt.trans_dt != null && tdt.trans_dt != DateTime.MinValue) ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "trans_dt"
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
                            tdtRets = tdtr;
                        }
                    }
                }

            }
            return tdtRets;
        }


        internal List<td_def_trans_trf> GetDepTrans(td_def_trans_trf tdt)
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
         + " PENAL_INTT_RECOV,"
         + " ADV_PRN_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD,ARDB_CD,DEL_FLAG "
    + " FROM TD_DEP_TRANS"
    + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) "
    + " AND (  TRANS_CD = {2} )  "
    + " AND TRANS_TYPE = {3} AND DEL_FLAG='N'  ";

            if (tdt.trans_dt != null && tdt.trans_dt != DateTime.MinValue)
            {
                _query += " AND (TRANS_DT = to_date('{4}','dd-mm-yyyy' ))  ";
            }
            using (var connection = OrclDbConnection.NewConnection)
            {
                if (tdt.trans_dt != null && tdt.trans_dt != DateTime.MinValue)
                {
                    _statement = string.Format(_query,
                    string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                    string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                    tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd",
                    string.IsNullOrWhiteSpace(tdt.trans_type) ? "trans_type" : string.Concat("'", tdt.trans_type, "'"),
                    (tdt.trans_dt != null && tdt.trans_dt != DateTime.MinValue) ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "trans_dt"
                    );
                }
                else
                {
                    _statement = string.Format(_query,
                    string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                    string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                    tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd",
                    string.IsNullOrWhiteSpace(tdt.trans_type) ? "trans_type" : string.Concat("'", tdt.trans_type, "'")
                    );
                }
                // (tdt.trans_dt!= null && tdt.trans_dt != DateTime.MinValue) ? tdt.trans_dt.ToString("dd/MM/yyyy"): "trans_dt",
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
                                tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                                tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                                tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                                tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                                tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }

        internal int InsertDepTrans(List<td_def_trans_trf> tdt)
        {
            int _ret = 0;
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TD_DEP_TRANS (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                        + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                        + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                        + " BRN_CD,ARDB_CD,PENAL_INTT_RECOV,ADV_PRN_RECOV,DEL_FLAG)"
                        + " VALUES (to_date('{0}','dd-mm-yyyy'),{1},{2},{3},{4},{5},{6},to_date('{7}','dd-mm-yyyy'),{8},{9}, {10},{11},"
                        + " to_date('{12}','dd-mm-yyyy'),{13},to_date('{14}','dd-mm-yyyy'),{15},{16},to_date('{17}','dd-mm-yyyy'),{18},{19},{20},to_date('{21}','dd-mm-yyyy'),{22},{23}, {24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                        + " {38},to_date('{39}','dd-mm-yyyy'),{40},{41},{42},{43},{44}, {45},{46},{47},{48},to_date('{49}','dd-mm-yyyy'),"
                        + " {50},{51},{52},{53},'N')";

            //int VoucherIdMax=GetTVoucherDtlsMaxId(tdt[0]);
            using (var connection = OrclDbConnection.NewConnection)
            {

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < tdt.Count; i++)
                        {
                            _statement = string.Format(_query,
                                         string.Concat(tdt[i].trans_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat(tdt[i].trans_cd),
                                         string.Concat("'", tdt[i].acc_type_cd, "'"),
                                         string.Concat("'", tdt[i].acc_num, "'"),
                                         string.Concat("'", tdt[i].trans_type, "'"),
                                         string.Concat("'", tdt[i].trans_mode, "'"),
                                         string.Concat("'", tdt[i].amount, "'"),
                                         string.Concat(tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].instrument_num, "'"),
                                         string.Concat("'", tdt[i].paid_to, "'"),
                                         string.Concat("'", tdt[i].token_num, "'"),
                                         string.Concat("'", tdt[i].created_by, "'"),
                                         string.Concat(tdt[i].created_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].modified_by, "'"),
                                         string.Concat(tdt[i].modified_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].approval_status, "'"),
                                         string.Concat("'", tdt[i].approved_by, "'"),
                                         string.Concat(tdt[i].approved_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].particulars, "'"),
                                         string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                         string.Concat("'", tdt[i].tr_acc_num, "'"),
                                         string.Concat(tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy")),
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
                                         string.Concat(tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].periodicity, "'"),
                                         string.Concat("'", tdt[i].disb_id, "'"),
                                         string.Concat("'", tdt[i].comp_unit_no, "'"),
                                         string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                         string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                         string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                         string.Concat("'", tdt[i].sector_cd, "'"),
                                         string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                         string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                         string.Concat(tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].brn_cd, "'"),
                                         string.Concat("'", tdt[i].ardb_cd, "'"),
                                         string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                         string.Concat("'", tdt[i].adv_prn_recov, "'")
                                         );

                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();
                                transaction.Commit();
                                _ret = 0;
                            }
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
        internal int UpdateDepTrans(List<td_def_trans_trf> tdt)
        {
            int _ret = 0;
            string _query = "UPDATE TD_DEP_TRANS SET "
         + " TRANS_DT               =NVL(to_date('{0}','dd-mm-yyyy'),TRANS_DT       ),"
         + " TRANS_CD               =NVL({1},TRANS_CD       ),"
         + " ACC_TYPE_CD            =NVL({2},ACC_TYPE_CD    ),"
         + " ACC_NUM                =NVL({3},ACC_NUM        ),"
         + " TRANS_TYPE             =NVL({4},TRANS_TYPE     ),"
         + " TRANS_MODE             =NVL({5},TRANS_MODE     ),"
         + " AMOUNT                 =NVL({6},AMOUNT         ),"
         + " INSTRUMENT_DT          =NVL(to_date('{7}','dd-mm-yyyy'),INSTRUMENT_DT  ),"
         + " INSTRUMENT_NUM         =NVL({8},INSTRUMENT_NUM ),"
         + " PAID_TO                =NVL({9},PAID_TO        ),"
         + " TOKEN_NUM              =NVL({10},TOKEN_NUM      ),"
         + " CREATED_BY             =NVL({11},CREATED_BY     ),"
         + " CREATED_DT             =NVL(to_date('{12}','dd-mm-yyyy'),CREATED_DT     ),"
         + " MODIFIED_BY            =NVL({13},MODIFIED_BY    ),"
         + " MODIFIED_DT            =NVL(to_date('{14}','dd-mm-yyyy'),MODIFIED_DT    ),"
         + " APPROVAL_STATUS        =NVL({15},APPROVAL_STATUS),"
         + " APPROVED_BY            =NVL({16},APPROVED_BY    ),"
         + " APPROVED_DT            =NVL(to_date('{17}','dd-mm-yyyy'),APPROVED_DT    ),"
         + " PARTICULARS            =NVL({18},PARTICULARS    ),"
         + " TR_ACC_TYPE_CD         =NVL({19},TR_ACC_TYPE_CD ),"
         + " TR_ACC_NUM             =NVL({20},TR_ACC_NUM     ),"
         + " VOUCHER_DT             =NVL(to_date('{21}','dd-mm-yyyy'),VOUCHER_DT     ),"
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
         + " INSTL_START_DT         =NVL(to_date('{39}','dd-mm-yyyy'),INSTL_START_DT ),"
         + " PERIODICITY            =NVL({40},PERIODICITY    ),"
         + " DISB_ID                =NVL({41},DISB_ID        ),"
         + " COMP_UNIT_NO           =NVL({42},COMP_UNIT_NO   ),"
         + " ONGOING_UNIT_NO        =NVL({43},ONGOING_UNIT_NO),"
         + " MIS_ADVANCE_RECOV      =NVL({44},MIS_ADVANCE_RECOV),"
         + " AUDIT_FEES_RECOV       =NVL({45},AUDIT_FEES_RECOV),"
         + " PENAL_INTT_RECOV       =NVL({46},PENAL_INTT_RECOV),"
         + " ADV_PRN_RECOV          =NVL({47},ADV_PRN_RECOV),"
         + " SECTOR_CD              =NVL({48},SECTOR_CD      ),"
         + " SPL_PROG_CD            =NVL({49},SPL_PROG_CD    ),"
         + " BORROWER_CR_CD         =NVL({50},BORROWER_CR_CD ),"
         + " INTT_TILL_DT           =NVL(to_date('{51}','dd-mm-yyyy'),INTT_TILL_DT   ),"
         + " BRN_CD                 =NVL({52},BRN_CD         )"
    + " WHERE (ARDB_CD = {53}) AND (BRN_CD = {54}) AND "
    + " (TRANS_DT = to_date('{55}','dd-mm-yyyy' )) AND  "
    + " (  TRANS_CD = {56} ) AND  "
    + " ACC_TYPE_CD = {57} AND "
    + " ACC_NUM = {58}";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < tdt.Count; i++)
                        {
                            _statement = string.Format(_query,
                                         string.Concat(tdt[i].trans_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat(tdt[i].trans_cd),
                                         string.Concat("'", tdt[i].acc_type_cd, "'"),
                                         string.Concat("'", tdt[i].acc_num, "'"),
                                         string.Concat("'", tdt[i].trans_type, "'"),
                                         string.Concat("'", tdt[i].trans_mode, "'"),
                                         string.Concat("'", tdt[i].amount, "'"),
                                         string.Concat(tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].instrument_num, "'"),
                                         string.Concat("'", tdt[i].paid_to, "'"),
                                         string.Concat("'", tdt[i].token_num, "'"),
                                         string.Concat("'", tdt[i].created_by, "'"),
                                         string.Concat(tdt[i].created_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].modified_by, "'"),
                                         string.Concat(tdt[i].modified_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].approval_status, "'"),
                                         string.Concat("'", tdt[i].approved_by, "'"),
                                         string.Concat(tdt[i].approved_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].particulars, "'"),
                                         string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                         string.Concat("'", tdt[i].tr_acc_num, "'"),
                                         string.Concat(tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy")),
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
                                         string.Concat(tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].periodicity, "'"),
                                         string.Concat("'", tdt[i].disb_id, "'"),
                                         string.Concat("'", tdt[i].comp_unit_no, "'"),
                                         string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                         string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                         string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                         string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                         string.Concat("'", tdt[i].adv_prn_recov, "'"),
                                         string.Concat("'", tdt[i].sector_cd, "'"),
                                         string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                         string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                         string.Concat(tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat("'", tdt[i].brn_cd, "'"),
                                         string.Concat("'", tdt[i].ardb_cd, "'"),
                                         string.Concat("'", tdt[i].brn_cd, "'"),
                                         string.Concat(tdt[i].trans_dt.Value.ToString("dd/MM/yyyy")),
                                         string.Concat(tdt[i].trans_cd),
                                         string.Concat("'", tdt[i].acc_type_cd, "'"),
                                         string.Concat("'", tdt[i].acc_num, "'")
                                         );
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();
                                transaction.Commit();
                                _ret = 0;
                            }
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
        internal List<td_def_trans_trf> GetUnapprovedDepTrans(td_def_trans_trf tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " TRF_TYPE,"
         +  " REMARKS,"
         + " OVD_PRN_RECOV,"
         + " BRN_CD,ARDB_CD,DEL_FLAG"
    + " FROM TD_DEP_TRANS"
    + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
    + " NVL(APPROVAL_STATUS, 'U') = 'U' AND "
    + " ACC_TYPE_CD IN (SELECT acc_type_cd FROM   mm_acc_type WHERE  dep_loan_flag = {2})   AND"
    + " TRANS_TYPE <> 'T' AND DEL_FLAG='N' ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                            string.IsNullOrWhiteSpace(tdt.trans_type) ? string.Concat("'","D", "'") : string.Concat("'", tdt.trans_type, "'")
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
                                tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                                tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }


        internal List<td_def_trans_trf> GetapprovedDepTrans(td_def_trans_trf tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " TRF_TYPE,"
         + " REMARKS,"
         + " OVD_PRN_RECOV,"
         + " BRN_CD,ARDB_CD,DEL_FLAG"
    + " FROM TD_DEP_TRANS"
    + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
    + " NVL(APPROVAL_STATUS, 'U') = 'A' AND "
    + " ACC_TYPE_CD IN (SELECT acc_type_cd FROM   mm_acc_type WHERE  dep_loan_flag = {2})   AND"
    + " TRANS_TYPE <> 'T' AND DEL_FLAG='N' ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                            string.IsNullOrWhiteSpace(tdt.trans_type) ? string.Concat("'", "D", "'") : string.Concat("'", tdt.trans_type, "'")
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
                                tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                                tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }



        internal string P_UPDATE_TD_DEP_TRANS(DbConnection connection, p_gen_param prp)
        {
            string _alter="ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query="P_UPDATE_TD_DEP_TRANS";
                
                    try{    
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
                                    var parm3 = new OracleParameter("ad_trans_cd", OracleDbType.Int16, ParameterDirection.Input);
                                    parm3.Value = prp.ad_trans_cd;
                                    command.Parameters.Add(parm3);
                                    var parm4 = new OracleParameter("adt_trans_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm4.Value = prp.adt_trans_dt;
                                    command.Parameters.Add(parm4);
                                    var parm5 = new OracleParameter("ad_acc_type_cd", OracleDbType.Int16, ParameterDirection.Input);
                                    parm5.Value = prp.ad_acc_type_cd;
                                    command.Parameters.Add(parm5);
                                    var parm6 = new OracleParameter("as_acc_num", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm6.Value = prp.as_acc_num;
                                    command.Parameters.Add(parm6);
                                    
                                    command.ExecuteNonQuery();
                                    return "0";
                            }                             
                        }
                        catch (Exception ex)
                        {
                            return ex.Message.ToString();
                        }
             
    }



        internal string P_UPDATE_TD_DEP_TRANS_INVEST(DbConnection connection, p_gen_param prp)
        {
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_UPDATE_TD_DEP_TRANS_INVEST";

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
                    var parm3 = new OracleParameter("ad_trans_cd", OracleDbType.Int16, ParameterDirection.Input);
                    parm3.Value = prp.ad_trans_cd;
                    command.Parameters.Add(parm3);
                    var parm4 = new OracleParameter("adt_trans_dt", OracleDbType.Date, ParameterDirection.Input);
                    parm4.Value = prp.adt_trans_dt;
                    command.Parameters.Add(parm4);
                    var parm5 = new OracleParameter("ad_acc_type_cd", OracleDbType.Int16, ParameterDirection.Input);
                    parm5.Value = prp.ad_acc_type_cd;
                    command.Parameters.Add(parm5);
                    var parm6 = new OracleParameter("as_acc_num", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm6.Value = prp.as_acc_num;
                    command.Parameters.Add(parm6);
                    var parm7 = new OracleParameter("as_user", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm7.Value = prp.gs_user_id;
                    command.Parameters.Add(parm7);

                    command.ExecuteNonQuery();
                    return "0";
                }
            }
            catch (Exception ex)
            {
                return ex.Message.ToString();
            }

        }


        internal string P_LOCKER_UPDATE(DbConnection connection, p_gen_param prp)
        {
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query = "P_LOCKER_UPDATE";

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
                    var parm3 = new OracleParameter("as_locker_id", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm3.Value = prp.as_locker_id;
                    command.Parameters.Add(parm3);
                    var parm4 = new OracleParameter("ad_trans_cd", OracleDbType.Int16, ParameterDirection.Input);
                    parm4.Value = prp.ad_trans_cd;
                    command.Parameters.Add(parm4);
                    var parm5 = new OracleParameter("adt_trans_dt", OracleDbType.Date, ParameterDirection.Input);
                    parm5.Value = prp.adt_trans_dt;
                    command.Parameters.Add(parm5);
                    var parm6 = new OracleParameter("as_user_id", OracleDbType.Varchar2, ParameterDirection.Input);
                    parm6.Value = prp.gs_user_id;
                    command.Parameters.Add(parm6);

                    command.ExecuteNonQuery();
                    return "0";
                }
            }
            catch (Exception ex)
            {
                return ex.Message.ToString();
            }

        }





        internal string P_TD_DEP_TRANS_LOAN(DbConnection connection, p_gen_param prp)
        {
            string _alter="ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query="P_TD_DEP_TRANS_LOAN";
                
                    try{    
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
                                    var parm3 = new OracleParameter("ad_trans_cd", OracleDbType.Int16, ParameterDirection.Input);
                                    parm3.Value = prp.ad_trans_cd;
                                    command.Parameters.Add(parm3);
                                    var parm4 = new OracleParameter("adt_trans_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm4.Value = prp.adt_trans_dt;
                                    command.Parameters.Add(parm4);
                                    command.ExecuteNonQuery();
                                    return "0";
                            }                             
                        }
                        catch (Exception ex)
                        {
                            return ex.Message.ToString();
                        }
             
    }  

         internal int UpdateTransactionDetails(LoanOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                       AccountOpenDL _dac = new AccountOpenDL(); 
                       if (acc.tmdenominationtrans.Count > 0)
                            _dac.UpdateDenominationDtls(connection, acc.tmdenominationtrans);
                        if (!String.IsNullOrWhiteSpace(acc.tmdepositrenew.acc_num))
                            _dac.UpdateDepositRenewTemp(connection, acc.tmdepositrenew);
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

       
     
     
    }
}