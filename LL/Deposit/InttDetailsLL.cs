
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
    public class InttDetailsLL
    {
       InttDetailsDL _dac = new InttDetailsDL(); 
internal List<td_intt_dtls> GetInttDetails(td_intt_dtls pmc)
        {         
            return _dac.GetInttDetails(pmc);
        }  

    }
}