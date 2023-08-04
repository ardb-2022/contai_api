
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
    public class AccountTransLL
    {
       AccountTransDL _dac = new AccountTransDL(); 
        internal decimal GetShadowBalance(tm_deposit td)
        {         
           
            return _dac.GetShadowBalance(td);
        } 
    
    }
}