
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class DepTransactionTrfLL
    {
       DepTransactionTrfDL _dac = new DepTransactionTrfDL(); 
        internal List<td_def_trans_trf> GetDepTransTrf(td_def_trans_trf pmc)
        {         
           
            return _dac.GetDepTransTrf(pmc);
        }  
        internal int InsertDepTransTrf(List<td_def_trans_trf> tdt)
        {         
           
            return _dac.InsertDepTransTrf(tdt);
        }
        internal int UpdateDepTransTrf(List<td_def_trans_trf> tdt)
        {         
           
            return _dac.UpdateDepTransTrf(tdt);
        }  
        internal int DeleteDepTransTrf(td_def_trans_trf tdt)
        {         
           
            return _dac.DeleteDepTransTrf(tdt);
        }    

        
    }
}