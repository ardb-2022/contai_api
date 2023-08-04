
using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class DepTransactionLL
    {
       DepTransactionDL _dac = new DepTransactionDL(); 
        internal List<td_def_trans_trf> GetDepTrans(td_def_trans_trf pmc)
        {         
           
            return _dac.GetDepTrans(pmc);
        }  
        internal int InsertDepTrans(List<td_def_trans_trf> tdt)
        {         
           
            return _dac.InsertDepTrans(tdt);
        }
        internal int UpdateDepTrans(List<td_def_trans_trf> tdt)
        {         
           
            return _dac.UpdateDepTrans(tdt);
        }  
        
        internal List<td_def_trans_trf> GetUnapprovedDepTrans(td_def_trans_trf tdt)
        {
            return _dac.GetUnapprovedDepTrans(tdt);
        }

        
        internal List<td_def_trans_trf> GetapprovedDepTrans(td_def_trans_trf tdt)
        {
            return _dac.GetapprovedDepTrans(tdt);
        }
        internal int UpdateTransactionDetails(LoanOpenDM acc)
        {
            return _dac.UpdateTransactionDetails(acc);
        }

        
    }
}