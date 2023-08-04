using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.LL
{
    internal class BankConfigMstLL
    {
        internal BankConfigMst ReadAllConfiguration()
        {
            BankConfigMstDL obj = new BankConfigMstDL();
            return obj.ReadAllConfiguration();
        }

        internal void InsertUpdateBankConfig(BankConfigMst bankConfigMst)
        {
            BankConfigMstDL obj = new BankConfigMstDL();
            obj.InsertUpdateBankConfig(bankConfigMst);
        }
    }
}