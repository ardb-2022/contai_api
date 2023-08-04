
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
    public class DepositRenewTmpLL
    {
       DepositRenewTmpDL _dac = new DepositRenewTmpDL(); 
internal List<tm_deposit> GetDepositRenewTmp(tm_deposit pmc)
        {   
            return _dac.GetDepositRenewTmp(pmc);
            
        }  

    }
}