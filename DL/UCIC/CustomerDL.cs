using System;
using System.Collections.Generic;
using System.Data.Common;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class CustomerDL
    {
        string _statement;

        internal List<mm_customer> GetCustomerDtls(mm_customer pmc)
        {
            List<mm_customer> custRets = new List<mm_customer>();
            string _query = "SELECT  MM_CUSTOMER.ARDB_CD,"
            + " MM_CUSTOMER.BRN_CD,"
         + " MM_CUSTOMER.CUST_CD,"
         + " MM_CUSTOMER.CUST_TYPE,"
         + " MM_CUSTOMER.TITLE,"
         + " MM_CUSTOMER.FIRST_NAME,"
         + " MM_CUSTOMER.MIDDLE_NAME,"
         + " MM_CUSTOMER.LAST_NAME,"
         + " MM_CUSTOMER.CUST_NAME,"
         + " MM_CUSTOMER.GUARDIAN_NAME,"
         + " MM_CUSTOMER.CUST_DT,"
         + " MM_CUSTOMER.OLD_CUST_CD,"
         + " MM_CUSTOMER.DT_OF_BIRTH,"
         + " MM_CUSTOMER.AGE,"
         + " MM_CUSTOMER.SEX,"
         + " MM_CUSTOMER.MARITAL_STATUS,"
         + " MM_CUSTOMER.CATG_CD,"
         + " MM_CUSTOMER.COMMUNITY,"
         + " MM_CUSTOMER.CASTE,"
         + " MM_CUSTOMER.PERMANENT_ADDRESS,"
         + " MM_CUSTOMER.WARD_NO,"
         + " MM_CUSTOMER.STATE,"
         + " MM_CUSTOMER.DIST,"
         + " MM_CUSTOMER.PIN,"
         + " MM_CUSTOMER.VILL_CD,"
         + " MM_CUSTOMER.BLOCK_CD,"
         + " MM_CUSTOMER.SERVICE_AREA_CD,"
         + " MM_CUSTOMER.PS,"
         + " MM_CUSTOMER.PO,"
         + " MM_CUSTOMER.OCCUPATION,"
         + " MM_CUSTOMER.PHONE,"
         + " MM_CUSTOMER.PRESENT_ADDRESS,"
         + " MM_CUSTOMER.FARMER_TYPE,"
         + " MM_CUSTOMER.EMAIL,"
         + " MM_CUSTOMER.MONTHLY_INCOME,"
         + " MM_CUSTOMER.DATE_OF_DEATH,"
         + " MM_CUSTOMER.SMS_FLAG,"
         + " MM_CUSTOMER.STATUS,"
         + " MM_CUSTOMER.PAN,"
         + " MM_CUSTOMER.NOMINEE,"
         + " MM_CUSTOMER.NOM_RELATION,"
         + " MM_CUSTOMER.KYC_PHOTO_TYPE,"
         + " MM_CUSTOMER.KYC_PHOTO_NO,"
         + " MM_CUSTOMER.KYC_ADDRESS_TYPE,"
         + " MM_CUSTOMER.KYC_ADDRESS_NO,"
         + " MM_CUSTOMER.KYC_OTHER_TYPE,"
         + " MM_CUSTOMER.KYC_OTHER_NO,"
         + " MM_CUSTOMER.ORG_STATUS,"
         + " MM_CUSTOMER.ORG_REG_NO,"
         + " MM_CUSTOMER.CREATED_BY,"
         + " MM_CUSTOMER.CREATED_DT,"
         + " MM_CUSTOMER.MODIFIED_BY,"
         + " MM_CUSTOMER.MODIFIED_DT,"
         + " MM_CUSTOMER.LBR_STATUS,"
         + " MM_CUSTOMER.IS_WEAKER,"
         + " MM_CUSTOMER.SB_FOLIO_NO,"
         + " MM_CUSTOMER.TD_FOLIO_NO,"
         + " MM_CUSTOMER.FD_FOLIO_NO,"
         + " MM_CUSTOMER.RD_FOLIO_NO,"
         + " MM_CUSTOMER.MIS_FOLIO_NO,"
         + " MM_CUSTOMER.DEL_FLAG"
         + " FROM  MM_CUSTOMER"
         + " WHERE MM_CUSTOMER.ARDB_CD ={0} "
         + " AND MM_CUSTOMER.BRN_CD={1}"
         + " AND MM_CUSTOMER.CUST_CD={2}"
         + " AND MM_CUSTOMER.DEL_FLAG= 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                           !string.IsNullOrWhiteSpace(pmc.ardb_cd) ? string.Concat("'", pmc.ardb_cd, "'") : "MM_CUSTOMER.ARDB_CD",
                                           !string.IsNullOrWhiteSpace(pmc.brn_cd) ? string.Concat("'", pmc.brn_cd, "'") : "MM_CUSTOMER.BRN_CD",
                                           pmc.cust_cd != 0 ? Convert.ToString(pmc.cust_cd) : "cust_cd"
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
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
                                // mc.ward_no = UtilityM.CheckNull<decimal>(reader["WARD_NO"]);
                                mc.state = UtilityM.CheckNull<string>(reader["STATE"]);
                                mc.dist = UtilityM.CheckNull<string>(reader["DIST"]);
                                mc.pin = UtilityM.CheckNull<int>(reader["PIN"]);
                                mc.vill_cd = UtilityM.CheckNull<string>(reader["VILL_CD"]);
                                mc.block_cd = UtilityM.CheckNull<string>(reader["BLOCK_CD"]);
                                mc.service_area_cd = UtilityM.CheckNull<string>(reader["SERVICE_AREA_CD"]);
                                mc.ps = UtilityM.CheckNull<int>(reader["PS"]);
                                mc.po = UtilityM.CheckNull<int>(reader["PO"]);
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
                                mc.sb_folio_no = UtilityM.CheckNull<string>(reader["SB_FOLIO_NO"]);
                                mc.td_folio_no = UtilityM.CheckNull<string>(reader["TD_FOLIO_NO"]);
                                mc.fd_folio_no = UtilityM.CheckNull<string>(reader["FD_FOLIO_NO"]);
                                mc.rd_folio_no = UtilityM.CheckNull<string>(reader["RD_FOLIO_NO"]);
                                mc.mis_folio_no = UtilityM.CheckNull<string>(reader["MIS_FOLIO_NO"]);
                                mc.is_weaker = UtilityM.CheckNull<string>(reader["IS_WEAKER"]);
                                mc.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);


                                custRets.Add(mc);
                            }
                        }
                    }
                }
            }


            return custRets;
        }



        internal List<mm_customer> GetMemberDtls(mm_customer pmc)
        {
            List<mm_customer> custRets = new List<mm_customer>();
            string _query = "SELECT  MM_CUSTOMER.ARDB_CD,"
            + " MM_CUSTOMER.BRN_CD,"
         + " MM_CUSTOMER.CUST_CD,"
         + " MM_CUSTOMER.CUST_TYPE,"
         + " MM_CUSTOMER.TITLE,"
         + " MM_CUSTOMER.FIRST_NAME,"
         + " MM_CUSTOMER.MIDDLE_NAME,"
         + " MM_CUSTOMER.LAST_NAME,"
         + " MM_CUSTOMER.CUST_NAME,"
         + " MM_CUSTOMER.GUARDIAN_NAME,"
         + " MM_CUSTOMER.CUST_DT,"
         + " MM_CUSTOMER.OLD_CUST_CD,"
         + " MM_CUSTOMER.DT_OF_BIRTH,"
         + " MM_CUSTOMER.AGE,"
         + " MM_CUSTOMER.SEX,"
         + " MM_CUSTOMER.MARITAL_STATUS,"
         + " MM_CATEGORY.CATG_DESC,"
         + " MM_CUSTOMER.COMMUNITY,"
         + " MM_CUSTOMER.CASTE,"
         + " MM_CUSTOMER.PERMANENT_ADDRESS,"
         + " MM_CUSTOMER.WARD_NO,"
         + " MM_CUSTOMER.STATE,"
         + " MM_CUSTOMER.DIST,"
         + " MM_CUSTOMER.PIN,"
         + " (SELECT min(VILL_NAME) FROM MM_VILL WHERE VILL_CD = MM_CUSTOMER.VILL_CD) VILL_CD,"
         + " MM_CUSTOMER.BLOCK_CD,"
         + " MM_CUSTOMER.SERVICE_AREA_CD,"
         + " MM_CUSTOMER.OCCUPATION,"
         + " MM_CUSTOMER.PHONE,"
         + " MM_CUSTOMER.PRESENT_ADDRESS,"
         + " MM_CUSTOMER.FARMER_TYPE,"
         + " MM_CUSTOMER.EMAIL,"
         + " MM_CUSTOMER.MONTHLY_INCOME,"
         + " MM_CUSTOMER.DATE_OF_DEATH,"
         + " MM_CUSTOMER.SMS_FLAG,"
         + " MM_CUSTOMER.STATUS,"
         + " MM_CUSTOMER.PAN,"
         + " MM_CUSTOMER.NOMINEE,"
         + " MM_CUSTOMER.NOM_RELATION,"
         + " MM_CUSTOMER.KYC_PHOTO_TYPE,"
         + " MM_CUSTOMER.KYC_PHOTO_NO,"
         + " MM_CUSTOMER.KYC_ADDRESS_TYPE,"
         + " MM_CUSTOMER.KYC_ADDRESS_NO,"
         + " MM_CUSTOMER.ORG_STATUS,"
         + " MM_CUSTOMER.ORG_REG_NO,"
         + " MM_CUSTOMER.CREATED_BY,"
         + " MM_CUSTOMER.CREATED_DT,"
         + " MM_CUSTOMER.MODIFIED_BY,"
         + " MM_CUSTOMER.MODIFIED_DT,"
         + " MM_CUSTOMER.LBR_STATUS,"
         + " MM_CUSTOMER.IS_WEAKER,"
         + " MM_CUSTOMER.DEL_FLAG"
         + " FROM  MM_CUSTOMER,MM_CATEGORY"
         + " WHERE MM_CUSTOMER.CATG_CD = MM_CATEGORY.CATG_CD AND MM_CUSTOMER.ARDB_CD ={0} "
         + " AND MM_CUSTOMER.BRN_CD={1}"
         + " AND MM_CUSTOMER.CUST_TYPE='M'"
         + " AND MM_CUSTOMER.DEL_FLAG= 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                           !string.IsNullOrWhiteSpace(pmc.ardb_cd) ? string.Concat("'", pmc.ardb_cd, "'") : "MM_CUSTOMER.ARDB_CD",
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
                                mc.catg_desc = UtilityM.CheckNull<string>(reader["CATG_DESC"]);
                                mc.community = UtilityM.CheckNull<decimal>(reader["COMMUNITY"]);
                                mc.caste = UtilityM.CheckNull<decimal>(reader["CASTE"]);
                                mc.permanent_address = UtilityM.CheckNull<string>(reader["PERMANENT_ADDRESS"]);
                                // mc.ward_no = UtilityM.CheckNull<decimal>(reader["WARD_NO"]);
                                mc.state = UtilityM.CheckNull<string>(reader["STATE"]);
                                mc.dist = UtilityM.CheckNull<string>(reader["DIST"]);
                                mc.pin = UtilityM.CheckNull<int>(reader["PIN"]);
                                mc.vill_cd = UtilityM.CheckNull<string>(reader["VILL_CD"]);
                                mc.block_cd = UtilityM.CheckNull<string>(reader["BLOCK_CD"]);
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
                                mc.org_status = UtilityM.CheckNull<string>(reader["ORG_STATUS"]);
                                mc.org_reg_no = UtilityM.CheckNull<decimal>(reader["ORG_REG_NO"]);
                                mc.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                mc.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                mc.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                mc.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                mc.lbr_status = UtilityM.CheckNull<string>(reader["LBR_STATUS"]);
                                mc.is_weaker = UtilityM.CheckNull<string>(reader["IS_WEAKER"]);
                                mc.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);


                                custRets.Add(mc);
                            }
                        }
                    }
                }
            }


            return custRets;
        }


        /*internal List<tm_deposit> GetDepositDtls(mm_customer pmc)
        {
            List<tm_deposit> depo = new List<tm_deposit>();
            string _query = " SELECT TM_DEPOSIT.ACC_TYPE_CD, TM_DEPOSIT.ACC_NUM, "
                           + " Decode(TM_DEPOSIT.ACC_TYPE_CD, 1,TM_DEPOSIT.CLR_BAL,7,TM_DEPOSIT.CLR_BAL,8,TM_DEPOSIT.CLR_BAL,6, f_get_rd_prn (TM_DEPOSIT.ACC_NUM,SYSDATE),TM_DEPOSIT.PRN_AMT) Balance, "
                           + " TM_DEPOSIT.CUST_CD,MM_CUSTOMER.CUST_NAME,TM_DEPOSIT.ACC_STATUS  "
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
                                depo.Add(mc);
                            }
                        }
                    }
                }
            }
            return depo;
        }*/

        internal List<tm_loan_all> GetLoanDtls(mm_customer pmc)
        {
            List<tm_loan_all> depo = new List<tm_loan_all>();
            string _query = " SELECT TM_LOAN_ALL.PARTY_CD,TM_LOAN_ALL.LOAN_ID,MM_CUSTOMER.CUST_NAME, "
                            + " Sum(TM_LOAN_ALL.CURR_PRN) CURR_PRN,Sum(TM_LOAN_ALL.OVD_PRN) OVD_PRN, "
                            + " Sum(TM_LOAN_ALL.CURR_INTT) CURR_INTT,Sum(TM_LOAN_ALL.OVD_INTT) OVD_INTT, Sum(TM_LOAN_ALL.PENAL_INTT) PENAL_INTT,"
                            + " Sum(TM_LOAN_ALL.CURR_PRN) + Sum(TM_LOAN_ALL.OVD_PRN) +Sum(TM_LOAN_ALL.CURR_INTT)+ Sum(TM_LOAN_ALL.OVD_INTT) + Sum(TM_LOAN_ALL.PENAL_INTT) out_standing, "
                            + " TM_LOAN_ALL.ACC_CD "
                            + " FROM   TM_LOAN_ALL,MM_CUSTOMER "
                            + " WHERE  TM_LOAN_ALL.ARDB_CD = MM_CUSTOMER.ARDB_CD AND TM_LOAN_ALL.PARTY_CD = MM_CUSTOMER.CUST_CD "
                            + " And TM_LOAN_ALL.PARTY_CD = To_Char({0}) AND TM_LOAN_ALL.ARDB_CD = {1} "
                            + " Group By TM_LOAN_ALL.PARTY_CD,TM_LOAN_ALL.LOAN_ID, "
                            + " MM_CUSTOMER.CUST_NAME,TM_LOAN_ALL.ACC_CD";
            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                            !string.IsNullOrWhiteSpace(pmc.cust_cd.ToString()) ? string.Concat("'", pmc.cust_cd, "'") : "MM_CUSTOMER.CUST_CD",
                                            !string.IsNullOrWhiteSpace(pmc.ardb_cd) ? string.Concat("'", pmc.ardb_cd, "'") : "MM_CUSTOMER.ARDB_CD"
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var mc = new tm_loan_all();
                                mc.party_cd = UtilityM.CheckNull<decimal>(reader["PARTY_CD"]);
                                mc.loan_id = UtilityM.CheckNull<string>(reader["LOAN_ID"]);
                                mc.created_by = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                mc.curr_prn = UtilityM.CheckNull<decimal>(reader["CURR_PRN"]);
                                mc.ovd_prn = UtilityM.CheckNull<decimal>(reader["OVD_PRN"]);
                                mc.curr_intt = UtilityM.CheckNull<decimal>(reader["CURR_INTT"]);
                                mc.ovd_intt = UtilityM.CheckNull<decimal>(reader["OVD_INTT"]);
                                mc.penal_intt = UtilityM.CheckNull<decimal>(reader["PENAL_INTT"]);
                                mc.instl_amt = UtilityM.CheckNull<decimal>(reader["out_standing"]);
                                mc.acc_cd = UtilityM.CheckNull<Int32>(reader["ACC_CD"]);

                                depo.Add(mc);
                            }
                        }
                    }
                }
            }
            return depo;
        }


        internal decimal InsertCustomerDtls(mm_customer pmp)
        {
            decimal _ret = 0;
            string _query = "INSERT INTO MM_CUSTOMER (ARDB_CD,BRN_CD,CUST_CD,CUST_TYPE,TITLE,FIRST_NAME, MIDDLE_NAME,LAST_NAME,CUST_NAME,GUARDIAN_NAME,CUST_DT,"
                        + " OLD_CUST_CD, DT_OF_BIRTH,AGE, SEX,MARITAL_STATUS,CATG_CD,COMMUNITY,CASTE,PERMANENT_ADDRESS,WARD_NO,STATE,DIST,PIN,VILL_CD,"
                        + " BLOCK_CD,SERVICE_AREA_CD,OCCUPATION,PHONE,PRESENT_ADDRESS,FARMER_TYPE,EMAIL,MONTHLY_INCOME,DATE_OF_DEATH,SMS_FLAG,STATUS,"
                        + " PAN,NOMINEE,NOM_RELATION,KYC_PHOTO_TYPE,KYC_PHOTO_NO,KYC_ADDRESS_TYPE,KYC_ADDRESS_NO,ORG_STATUS,ORG_REG_NO,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,LBR_STATUS,IS_WEAKER,DEL_FLAG,KYC_OTHER_TYPE,KYC_OTHER_NO,SB_FOLIO_NO,TD_FOLIO_NO,FD_FOLIO_NO,RD_FOLIO_NO,MIS_FOLIO_NO,PS,PO)"
                        + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},"
                        + " {11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},"
                        + " {36},{37},{38},{39},{40},{41},{42},{43},{44},{45},SYSDATE,"
                        + " {46},SYSDATE,{47},{48},{49},{50},{51},{52},{53},{54},{55},{56},{57},{58})";                        
            pmp.cust_cd = GetCustomerCdMaxId(pmp.ardb_cd,pmp.brn_cd);
            _ret = pmp.cust_cd;
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                         string.Concat("'", pmp.ardb_cd, "'"),
                                         string.Concat("'", pmp.brn_cd, "'"),
                                         string.Concat("'", pmp.cust_cd, "'"),
                                         string.Concat("'", pmp.cust_type, "'"),
                                         string.Concat("'", pmp.title, "'"),
                                         string.Concat("'", pmp.first_name, "'"),
                                         string.Concat("'", pmp.middle_name, "'"),
                                         string.Concat("'", pmp.last_name, "'"),
                                         string.Concat("'", pmp.cust_name, "'"),
                                         string.Concat("'", pmp.guardian_name, "'"),
                                         string.IsNullOrWhiteSpace(pmp.cust_dt.ToString()) ? null : string.Concat("to_date('", pmp.cust_dt.Value.ToString("dd/MM/yyyy"), "','dd/mm/yyyy')"),
                                         string.Concat("'", pmp.old_cust_cd, "'"),
                                         string.IsNullOrWhiteSpace(pmp.dt_of_birth.ToString()) ? string.Concat("'",null,"'")  : string.Concat("to_date('", pmp.dt_of_birth.Value.ToString("dd/MM/yyyy"), "','dd/mm/yyyy')"),
                                         string.Concat("'", pmp.age, "'"),
                                         string.Concat("'", pmp.sex, "'"),
                                         string.Concat("'", pmp.marital_status, "'"),
                                         string.Concat("'", pmp.catg_cd, "'"),
                                         string.Concat("'", pmp.community, "'"),
                                         string.Concat("'", pmp.caste, "'"),
                                         string.Concat("'", pmp.present_address, "'"),
                                         string.Concat("'", pmp.ward_no, "'"),
                                         string.Concat("'", pmp.state, "'"),
                                         string.Concat("'", pmp.dist, "'"),
                                         string.Concat("'", pmp.pin, "'"),
                                         string.Concat("'", pmp.vill_cd, "'"),
                                         string.Concat("'", pmp.block_cd, "'"),
                                         string.Concat("'", pmp.service_area_cd, "'"),
                                         string.Concat("'", pmp.occupation, "'"),
                                         string.Concat("'", pmp.phone, "'"),
                                         string.Concat("'", pmp.present_address, "'"),
                                         string.Concat("'", pmp.farmer_type, "'"),
                                         string.Concat("'", pmp.email, "'"),
                                         string.Concat("'", pmp.monthly_income, "'"),
                                         string.IsNullOrWhiteSpace(pmp.date_of_death.ToString()) ? string.Concat("'", null, "'") : string.Concat("to_date('", pmp.date_of_death.Value.ToString("dd/MM/yyyy"), "','dd/mm/yyyy')"),
                                         string.Concat("'", pmp.sms_flag, "'"),
                                         string.Concat("'", pmp.status, "'"),
                                         string.Concat("'", string.IsNullOrWhiteSpace(pmp.pan) ? string.Empty : pmp.pan.ToUpper(), "'"),
                                         string.Concat("'", pmp.nominee, "'"),
                                         string.Concat("'", pmp.nom_relation, "'"),
                                         string.Concat("'", pmp.kyc_photo_type, "'"),
                                         string.Concat("'", pmp.kyc_photo_no, "'"),
                                         string.Concat("'", pmp.kyc_address_type, "'"),
                                         string.Concat("'", pmp.kyc_address_no, "'"),
                                         string.Concat("'", pmp.org_status, "'"),
                                         string.Concat("'", pmp.org_reg_no, "'"),
                                         string.Concat("'", pmp.created_by, "'"),
                                         //string.IsNullOrWhiteSpace(pmp.created_dt.ToString()) ? null : string.Concat("to_date('",pmp.created_dt.Value.ToString("dd/MM/yyyy"),"')"),
                                         //string.IsNullOrWhiteSpace(pmp.created_dt.ToString()) ? null : string.Concat("sysdate"),
                                         string.Concat("'", pmp.modified_by, "'"),
                                         //string.IsNullOrWhiteSpace(pmp.modified_dt.ToString()) ? null : string.Concat("to_date('", pmp.modified_dt.Value.ToString("dd/MM/yyyy"), "')"),
                                         //string.IsNullOrWhiteSpace(pmp.modified_dt.ToString()) ? null : string.Concat("sysdate"),
                                         string.Concat("'", pmp.lbr_status, "'"),
                                         string.Concat("'", pmp.is_weaker, "'"),
                                         string.Concat("'", pmp.del_flag, "'"),
                                         string.Concat("'", pmp.kyc_other_type, "'"),
                                         string.Concat("'", pmp.kyc_other_no, "'"),
                                         string.Concat("'", pmp.sb_folio_no, "'"),
                                         string.Concat("'", pmp.td_folio_no, "'"),
                                         string.Concat("'", pmp.fd_folio_no, "'"),
                                         string.Concat("'", pmp.rd_folio_no, "'"),
                                         string.Concat("'", pmp.mis_folio_no, "'"),
                                         string.Concat("'", pmp.ps, "'"),
                                         string.Concat("'", pmp.po, "'")) ;

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            // _ret = 0;
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

        internal decimal GetCustomerCdMaxId(string ardbCd,string brnCd)
        {
            decimal maxCustCd = 0;
            string _query = "Select {0} || lpad( Nvl(max(to_number(substr(to_char(cust_cd),4))) + 1 , 1),7,'0') MAX_CUST_CD"
            + " From mm_customer"
            + " Where ardb_cd = {1}"
            + "AND brn_cd ={2}";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                             string.IsNullOrWhiteSpace(brnCd) ? "brn_cd" : string.Concat("'", brnCd, "'"),
                                             string.IsNullOrWhiteSpace(brnCd) ? "ardb_cd" : string.Concat("'", ardbCd, "'"),
                                             string.IsNullOrWhiteSpace(brnCd) ? "brn_cd" : string.Concat("'", brnCd, "'")
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                maxCustCd = Convert.ToDecimal(UtilityM.CheckNull<string>(reader["MAX_CUST_CD"]));
                            }
                        }
                    }
                }
            }

            return maxCustCd;
        }


        internal int Checkpancard(mm_customer pmp)
        {
            int d = 0;

            string _query = " select  count(*)  CNT "
                            + " from MM_CUSTOMER"
                            + " where ardb_cd = {0} "
                            + "  AND ((KYC_PHOTO_TYPE = 'P' And KYC_PHOTO_NO = {1}) OR (KYC_ADDRESS_TYPE = 'P' And KYC_ADDRESS_NO = {2}))";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          string.Concat("'", pmp.ardb_cd, "'"),
                                          string.Concat("'", pmp.kyc_photo_no, "'"),
                                          string.Concat("'", pmp.kyc_address_no, "'")
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




        internal int Checkaadharcard(mm_customer pmp)
        {
            int d = 0;

            string _query = " select  count(*)  CNT "
                            + " from MM_CUSTOMER"
                            + " where ardb_cd = {0} "
                            + "  AND ((KYC_PHOTO_TYPE = 'G' And KYC_PHOTO_NO = {1}) OR (KYC_ADDRESS_TYPE = 'G' And KYC_ADDRESS_NO = {2}))";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          string.Concat("'", pmp.ardb_cd, "'"),
                                          string.Concat("'", pmp.kyc_photo_no, "'"),
                                          string.Concat("'", pmp.kyc_address_no, "'")
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



        internal int UpdateCustomerDtls(mm_customer pmp)
        {
            int _ret = 0;
            string _query = "UPDATE MM_CUSTOMER "
         + " SET CUST_TYPE=NVL({0},CUST_TYPE),"
         + "  TITLE=NVL({1},TITLE),"
         + "  FIRST_NAME=NVL({2},FIRST_NAME),"
         + "  MIDDLE_NAME={3},"
         + "  LAST_NAME=NVL({4},LAST_NAME),"
         + "  CUST_NAME=NVL({5},CUST_NAME),"
         + "  GUARDIAN_NAME=NVL({6},GUARDIAN_NAME),"
         + "  CUST_DT=NVL(to_date('{7}','dd/mm/yyyy' ),CUST_DT),"
         + "  OLD_CUST_CD=NVL({8},OLD_CUST_CD),"
         + "  DT_OF_BIRTH=NVL(to_date('{9}','dd/mm/yyyy' ),DT_OF_BIRTH),"
         + "  AGE=NVL({10},AGE),"
         + "  SEX=NVL({11},SEX),"
         + "  MARITAL_STATUS=NVL({12},MARITAL_STATUS),"
         + "  CATG_CD=NVL({13},CATG_CD),"
         + "  COMMUNITY=NVL({14},COMMUNITY),"
         + "  CASTE=NVL({15},CASTE),"
         + "  PERMANENT_ADDRESS=NVL({16},PERMANENT_ADDRESS),"
         + "  WARD_NO=NVL({17},WARD_NO),"
         + "  STATE=NVL({18},STATE),"
         + "  DIST=NVL({19},DIST),"
         + "  PIN=NVL({20},PIN),"
         + "  VILL_CD=NVL({21},VILL_CD),"
         + "  BLOCK_CD=NVL({22},BLOCK_CD),"
         + "  SERVICE_AREA_CD=NVL({23},SERVICE_AREA_CD),"
         + "  OCCUPATION=NVL({24},OCCUPATION),"
         + "  PHONE=NVL({25},PHONE),"
         + "  PRESENT_ADDRESS=NVL({26},PRESENT_ADDRESS),"
         + "  FARMER_TYPE=NVL({27},FARMER_TYPE),"
         + "  EMAIL=NVL({28},EMAIL),"
         + "  MONTHLY_INCOME=NVL({29},MONTHLY_INCOME),"
         + "  DATE_OF_DEATH=to_date('{30}','dd/mm/yyyy' ),"
         + "  SMS_FLAG=NVL({31},SMS_FLAG),"
         + "  STATUS=NVL({32},STATUS),"
         + "  PAN=NVL({33},PAN),"
         + "  NOMINEE=NVL({34},NOMINEE),"
         + "  NOM_RELATION=NVL({35},NOM_RELATION),"
         + "  KYC_PHOTO_TYPE=NVL({36},KYC_PHOTO_TYPE),"
         + "  KYC_PHOTO_NO=NVL({37},KYC_PHOTO_NO),"
         + "  KYC_ADDRESS_TYPE=NVL({38},KYC_ADDRESS_TYPE),"
         + "  KYC_ADDRESS_NO=NVL({39},KYC_ADDRESS_NO),"
         + "  ORG_STATUS=NVL({40},ORG_STATUS),"
         + "  ORG_REG_NO=NVL({41},ORG_REG_NO),"
         + "  CREATED_BY=NVL({42},CREATED_BY),"
         + "  MODIFIED_BY=NVL({43},MODIFIED_BY),"
         + "  MODIFIED_DT=SYSDATE,"
         + "  LBR_STATUS =NVL({44}, LBR_STATUS),"
         + "  IS_WEAKER =NVL({45} ,IS_WEAKER),"
         + "  KYC_OTHER_TYPE =NVL({46} , KYC_OTHER_TYPE),"
         + "  KYC_OTHER_NO =NVL({47} , KYC_OTHER_NO),"
         + "  SB_FOLIO_NO =NVL({48} ,SB_FOLIO_NO),"
         + "  TD_FOLIO_NO =NVL({49} ,TD_FOLIO_NO),"
         + "  FD_FOLIO_NO =NVL({50} ,FD_FOLIO_NO),"
         + "  RD_FOLIO_NO =NVL({51} ,RD_FOLIO_NO),"
         + "  MIS_FOLIO_NO =NVL({52} ,MIS_FOLIO_NO),"
         + "  PS =NVL({53} ,PS),"
         + "  PO =NVL({54} ,PO)"
         + "  WHERE ARDB_CD = {55} AND  CUST_CD ={56} AND DEL_FLAG = 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                             string.Concat("'", pmp.cust_type, "'"),
                                             string.Concat("'", pmp.title, "'"),
                                             string.Concat("'", pmp.first_name, "'"),
                                             string.Concat("'", pmp.middle_name, "'"),
                                             string.Concat("'", pmp.last_name, "'"),
                                             string.Concat("'", pmp.cust_name, "'"),
                                             string.Concat("'", pmp.guardian_name, "'"),
                                             string.IsNullOrWhiteSpace(pmp.cust_dt.ToString()) ? null : string.Concat(pmp.cust_dt.Value.ToString("dd/MM/yyyy")),
                                             string.Concat("'", pmp.old_cust_cd, "'"),
                                             string.IsNullOrWhiteSpace(pmp.dt_of_birth.ToString()) ? null : string.Concat(pmp.dt_of_birth.Value.ToString("dd/MM/yyyy")),
                                             string.Concat("'", pmp.age, "'"),
                                             string.Concat("'", pmp.sex, "'"),
                                             string.Concat("'", pmp.marital_status, "'"),
                                             string.Concat("'", pmp.catg_cd, "'"),
                                             string.Concat("'", pmp.community, "'"),
                                             string.Concat("'", pmp.caste, "'"),
                                             string.Concat("'", pmp.present_address, "'"),
                                             string.Concat("'", pmp.ward_no, "'"),
                                             string.Concat("'", pmp.state, "'"),
                                             string.Concat("'", pmp.dist, "'"),
                                             string.Concat("'", pmp.pin, "'"),
                                             string.Concat("'", pmp.vill_cd, "'"),
                                             string.Concat("'", pmp.block_cd, "'"),
                                             string.Concat("'", pmp.service_area_cd, "'"),
                                             string.Concat("'", pmp.occupation, "'"),
                                             string.Concat("'", pmp.phone, "'"),
                                             string.Concat("'", pmp.present_address, "'"),
                                             string.Concat("'", pmp.farmer_type, "'"),
                                             string.Concat("'", pmp.email, "'"),
                                             string.Concat("'", pmp.monthly_income, "'"),
                                             string.IsNullOrWhiteSpace(pmp.date_of_death.ToString()) ? null : string.Concat(pmp.date_of_death.Value.ToString("dd/MM/yyyy")),
                                             string.Concat("'", pmp.sms_flag, "'"),
                                             string.Concat("'", pmp.status, "'"),
                                              //string.Concat("'", pmp.pan.ToUpper(), "'"),
                                             string.Concat("'", string.IsNullOrWhiteSpace(pmp.pan) ? string.Empty : pmp.pan.ToUpper(), "'"),
                                             string.Concat("'", pmp.nominee, "'"),
                                             string.Concat("'", pmp.nom_relation, "'"),
                                             string.Concat("'", pmp.kyc_photo_type, "'"),
                                             string.Concat("'", pmp.kyc_photo_no, "'"),
                                             string.Concat("'", pmp.kyc_address_type, "'"),
                                             string.Concat("'", pmp.kyc_address_no, "'"),
                                             string.Concat("'", pmp.org_status, "'"),
                                             string.Concat("'", pmp.org_reg_no, "'"),
                                             string.Concat("'", pmp.created_by, "'"),
                                             string.Concat("'", pmp.modified_by, "'"),
                                             string.Concat("'", pmp.lbr_status, "'"),
                                             string.Concat("'", pmp.is_weaker, "'"),
                                             string.Concat("'", pmp.kyc_other_type, "'"),
                                             string.Concat("'", pmp.kyc_other_no, "'"),
                                             string.Concat("'", pmp.sb_folio_no, "'"),
                                             string.Concat("'", pmp.td_folio_no, "'"),
                                             string.Concat("'", pmp.fd_folio_no, "'"),
                                             string.Concat("'", pmp.rd_folio_no, "'"),
                                             string.Concat("'", pmp.mis_folio_no, "'"),
                                             string.Concat("'", pmp.ps, "'"),
                                             string.Concat("'", pmp.po, "'"),                                            
                                             string.Concat("'", pmp.ardb_cd, "'"),
                                             string.Concat("'", pmp.cust_cd, "'")
                                             );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            _ret = command.ExecuteNonQuery();
                            transaction.Commit();
                            // _ret = 0;
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

        internal int DeleteCustomerDtls(mm_customer pmc)
        {
            int _ret = 0;
            string _query = "UPDATE MM_CUSTOMER"
                   + "  SET  DEL_FLAG = 'Y', MODIFIED_BY = {0} , MODIFIED_DT = SYSDATE"
                   + "  WHERE ARDB_CD = {1} AND  CUST_CD ={2} AND DEL_FLAG = 'N' ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                             string.Concat("'", pmc.modified_by, "'"),
                                             string.Concat("'", pmc.ardb_cd, "'"),
                                             string.Concat("'", pmc.cust_cd, "'")
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

        internal List<tm_deposit> GetDepositDtls(mm_customer pmc)
        {
            List<tm_deposit> depo = new List<tm_deposit>();
            string _query = " SELECT a.ACC_TYPE_CD, a.ACC_NUM, "
                           + " Decode(a.ACC_TYPE_CD, 1,a.CLR_BAL,7,a.CLR_BAL,8,a.CLR_BAL,9,a.CLR_BAL,6,f_get_rd_prn ({0},a.ACC_NUM,SYSDATE),a.PRN_AMT) Balance, "
                           + " a.CUST_CD,b.CUST_NAME, a.ACC_STATUS,  "
                           + " a.PRN_AMT PRN_AMT, a.INTT_AMT INTT_AMT, "
                           + " a.INSTL_AMT INSTL_AMT "
                           + " FROM  TM_DEPOSIT a , MM_CUSTOMER b   "
                           + " WHERE   a.ARDB_CD = b.ARDB_CD AND a.CUST_CD = b.CUST_CD   "
                           + " And     a.CUST_CD = {1} "
                           + " AND     a.ARDB_CD = {2} "
                           + " AND     a.RENEW_ID = (SELECT max(RENEW_ID) "
                           + "                      FROM TM_DEPOSIT c "
                           + "                      WHERE a.ARDB_CD = c.ARDB_CD"
                           + "                      AND a.ACC_TYPE_CD = c.ACC_TYPE_CD"
                           + "                      AND a.ACC_NUM = c.ACC_NUM) "
                           + " Order By a.ACC_TYPE_CD  ";

            using (var connection = OrclDbConnection.NewConnection)
            {

                _statement = string.Format(_query,
                                            !string.IsNullOrWhiteSpace(pmc.ardb_cd) ? string.Concat("'", pmc.ardb_cd, "'") : "MM_CUSTOMER.ARDB_CD",
                                            !string.IsNullOrWhiteSpace(pmc.cust_cd.ToString()) ? string.Concat("'", pmc.cust_cd, "'") : "MM_CUSTOMER.CUST_CD",
                                            !string.IsNullOrWhiteSpace(pmc.ardb_cd) ? string.Concat("'", pmc.ardb_cd, "'") : "MM_CUSTOMER.ARDB_CD"
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


    }
}