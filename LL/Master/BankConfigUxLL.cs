using System.Collections.Generic;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.LL
{
    internal sealed class BankConfigUxLL
    {
        internal List<BankConfiguration> ReadBankConfigUx()
        {
            BankConfigUxDL obj = new BankConfigUxDL();
            return obj.ReadBankConfigUx();
        }

        internal void WriteBankConfigUx(List<BankConfiguration> bankConfig)
        {
            BankConfigUxDL obj = new BankConfigUxDL();
            obj.WriteBankConfigUx(bankConfig);
        }        
    }
}