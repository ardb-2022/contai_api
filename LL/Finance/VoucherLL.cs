
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class VoucherLL
    {
       VoucherDL _dac = new VoucherDL(); 
        internal List<t_voucher_dtls> GetTVoucherDtls(t_voucher_dtls tvd)
        {         
           
            return _dac.GetTVoucherDtls(tvd);
        }   
        internal int DeleteInsertVoucherDtls(List<t_voucher_dtls> tvd)
        {   
            return _dac.DeleteInsertVoucherDtls(tvd);
        } 
        internal int DeleteVoucherDtls(List<t_voucher_dtls> tvd)
        {   
            return _dac.DeleteVoucherDtls(tvd);
        }   

        VoucherPrintDL _dacPrint = new VoucherPrintDL(); 
          internal List<t_voucher_narration> GetTVoucherDtlsForPrint(t_voucher_dtls tvd)
        {         
           
            return _dacPrint.GetTVoucherDtlsForPrint(tvd);
        }  
            
    }
}