
using System;
using System.Collections.Generic;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class SignatoryLL
    {
       SignatoryDL _dac = new SignatoryDL(); 
      
internal List<td_signatory> GetSignatory(td_signatory pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.GetSignatoryTemp(pmc);
            else
            return _dac.GetSignatory(pmc);
        }  

        internal decimal InsertSignatory(td_signatory pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.InsertSignatoryTemp(pmc);
            else
            return _dac.InsertSignatory(pmc);
        } 

        internal int UpdateSignatory(td_signatory pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.UpdateSignatoryTemp(pmc);
            else
            return _dac.UpdateSignatory(pmc);
        } 

          internal int DeleteSignatory(td_signatory pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.DeleteSignatoryTemp(pmc);
            else
            return _dac.DeleteSignatory(pmc);
        } 

    }
}