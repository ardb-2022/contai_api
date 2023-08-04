using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class VoucherPrintDL
    {
        string _statement;
        string narration_ret;
            
        internal List<t_voucher_narration> GetTVoucherDtlsForPrint(t_voucher_dtls tvd)
        {
            List<t_voucher_narration> tvnRets=new List<t_voucher_narration>();
            string _query="SELECT NARRATION,VOUCHER_DT,VOUCHER_ID,VOUCHER_TYPE,APPROVAL_STATUS FROM T_VOUCHER_NARRATION WHERE  "
                        + " T_VOUCHER_NARRATION.ardb_cd = {0} AND T_VOUCHER_NARRATION.brn_cd = {1} AND  T_VOUCHER_NARRATION.voucher_dt BETWEEN TO_DATE('{2}','dd-mm-yyyy') AND TO_DATE('{3}','dd-mm-yyyy') ";
        
            string _query1=" SELECT T_VOUCHER_DTLS.VOUCHER_DT,"   
         +" T_VOUCHER_DTLS.ACC_CD,"   
         +" T_VOUCHER_DTLS.AMOUNT,"   
         +" T_VOUCHER_DTLS.DEBIT_CREDIT_FLAG,"   
         +" T_VOUCHER_DTLS.TRANSACTION_TYPE,"  
         +" T_VOUCHER_DTLS.NARRATION,"   
         +" M_ACC_MASTER.ACC_NAME,"   
         +" T_VOUCHER_DTLS.APPROVAL_STATUS,"   
         +" T_VOUCHER_DTLS.VOUCHER_ID,"  
         +" T_VOUCHER_DTLS.APPROVED_BY,"   
         +" T_VOUCHER_DTLS.APPROVED_DT,"   
		 +" T_VOUCHER_DTLS.AMOUNT DEBIT,"   
		 +" 0.00 CREDIT,"
		 +" T_VOUCHER_DTLS.INSTRUMENT_NO"        
    	 +" FROM T_VOUCHER_DTLS,"   
         +" M_ACC_MASTER"  
   	     + " WHERE ( T_VOUCHER_DTLS.ARDB_CD = {0} )  AND "
         + " ( T_VOUCHER_DTLS.BRN_CD = {1} )  AND "
         + " ( T_VOUCHER_DTLS.ARDB_CD = m_acc_master.ardb_cd ) AND"
         + " ( T_VOUCHER_DTLS.ACC_CD = m_acc_master.acc_cd ) AND"   
         +" ( T_VOUCHER_DTLS.VOUCHER_DT = TO_DATE('{2}','dd-mm-yyyy')) AND" 
         +" ( T_VOUCHER_DTLS.VOUCHER_ID = {3} ) AND" 
		 +" ( T_VOUCHER_DTLS.DEBIT_CREDIT_FLAG = 'D' )"  
         +" UNION"   
         +" SELECT T_VOUCHER_DTLS.VOUCHER_DT,"   
         +" T_VOUCHER_DTLS.ACC_CD,"   
         +" T_VOUCHER_DTLS.AMOUNT,"   
         +" T_VOUCHER_DTLS.DEBIT_CREDIT_FLAG,"   
         +" T_VOUCHER_DTLS.TRANSACTION_TYPE,"  
         +" T_VOUCHER_DTLS.NARRATION,"   
         +" M_ACC_MASTER.ACC_NAME,"   
         +" T_VOUCHER_DTLS.APPROVAL_STATUS,"   
         +" T_VOUCHER_DTLS.VOUCHER_ID,"   
         +" T_VOUCHER_DTLS.APPROVED_BY,"    
         +" T_VOUCHER_DTLS.APPROVED_DT,"   
		 +" 0.00 DEBIT,"  
		 +" T_VOUCHER_DTLS.AMOUNT CREDIT,"
   		 +" T_VOUCHER_DTLS.INSTRUMENT_NO"
         +" FROM T_VOUCHER_DTLS,"   
         +" M_ACC_MASTER"
         + " WHERE ( T_VOUCHER_DTLS.ARDB_CD = {0} )  AND "
         + " ( T_VOUCHER_DTLS.BRN_CD = {1} )  AND "
         + " ( T_VOUCHER_DTLS.ARDB_CD = m_acc_master.ardb_cd ) AND"
         + " ( T_VOUCHER_DTLS.ACC_CD = m_acc_master.acc_cd ) AND"
         + " ( T_VOUCHER_DTLS.VOUCHER_DT = TO_DATE('{2}','dd-mm-yyyy')) AND"
         + " ( T_VOUCHER_DTLS.VOUCHER_ID = {3} ) AND"
         + " ( T_VOUCHER_DTLS.DEBIT_CREDIT_FLAG = 'C' )"  ;

             using (var connection = OrclDbConnection.NewConnection)
            {
                try{
              
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace( tvd.brn_cd) ? "brn_cd" : string.Concat("'",  tvd.brn_cd , "'"),
                                            tvd.from_dt!= null ?  tvd.from_dt.ToString("dd/MM/yyyy"): "from_dt",
                                             tvd.to_dt!= null ?  tvd.to_dt.ToString("dd/MM/yyyy"): "to_dt"
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {        
                              var tvn = new t_voucher_narration();                        
                              tvn.narration = UtilityM.CheckNull<string>(reader["NARRATION"]);
                              tvn.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                              tvn.voucher_id = UtilityM.CheckNull<int>(reader["VOUCHER_ID"]);
                              tvn.voucher_typ = UtilityM.CheckNull<string>(reader["VOUCHER_TYPE"]);
                              tvn.voucher_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                              tvnRets.Add(tvn);
                            }
                        }
                    }
                }
                if(tvnRets.Count>0)
                {
                for(int i=0; i<tvnRets.Count;i++)
                {
                    List<t_voucher_dtls> tvdRets=new List<t_voucher_dtls>();
                    _statement = string.Format(_query1,
                    string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                    string.IsNullOrWhiteSpace( tvd.brn_cd) ? "brn_cd" : string.Concat("'",  tvd.brn_cd , "'"),
                    tvnRets[i].voucher_dt!= null ?  tvnRets[i].voucher_dt.ToString("dd/MM/yyyy"): "voucher_dt",
                    tvnRets[i].voucher_id !=0 ? Convert.ToString( tvnRets[i].voucher_id) : "voucher_id"
                    );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                               
                                var tvdr = new t_voucher_dtls();
                                tvdr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                                tvdr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                tvdr.amount = UtilityM.CheckNull<decimal>(reader["AMOUNT"]);
                                tvdr.debit_credit_flag = UtilityM.CheckNull<string>(reader["DEBIT_CREDIT_FLAG"]);
                                tvdr.transaction_type = UtilityM.CheckNull<string>(reader["TRANSACTION_TYPE"]);
                                tvdr.narration = UtilityM.CheckNull<string>(reader["NARRATION"]);
                                tvdr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                tvdr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                tvdr.voucher_id = UtilityM.CheckNull<int>(reader["VOUCHER_ID"]);
                                tvdr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                tvdr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                tvdr.dr_amount = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                tvdr.cr_amount = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                tvdr.instrument_no = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NO"]);
                                tvdRets.Add(tvdr);
                            }
                        }
                    }
                }
                tvnRets[i].vd=tvdRets;
                }
                }           
            }
            catch (Exception ex)
            {
                int a=0;
            }
            }
            

            return tvnRets;
        }  
}
}