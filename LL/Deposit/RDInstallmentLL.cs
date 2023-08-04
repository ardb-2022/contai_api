
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
    public class RDInstallmentLL
    {
       RDInstallmentDL _dac = new RDInstallmentDL(); 
internal List<td_rd_installment> GetRDInstallment(td_rd_installment pmc)
        {         
            return _dac.GetRDInstallment(pmc);
        } 

    }
}