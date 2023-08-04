
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
    public class AccountOpenLL
    {
       AccountOpenDL _dac = new AccountOpenDL(); 
        internal string InsertAccountOpeningData(AccOpenDM pmc)
        {         
           
            return _dac.InsertAccountOpeningData(pmc);
        } 
        
        internal string PopulateAccountNumber(p_gen_param pmc)
        {         
            return _dac.PopulateAccountNumber(pmc);
        }  

        internal decimal F_CALCRDINTT_REG(p_gen_param pmc)
        {         
            return _dac.F_CALCRDINTT_REG(pmc);
        } 
        internal decimal F_CALCTDINTT_REG(p_gen_param pmc)
        {         
            return _dac.F_CALCTDINTT_REG(pmc);
        }

        
        internal decimal F_CALC_SB_INTT(p_gen_param pmc)
        {         
            return _dac.F_CALC_SB_INTT(pmc);
        } 
        internal decimal F_CAL_RD_PENALTY(p_gen_param prp)
        {
             return _dac.F_CAL_RD_PENALTY(prp);
        }
        internal float GET_INT_RATE(p_gen_param pmc)
        {         
            return _dac.GET_INT_RATE(pmc);
        } 

        internal AccOpenDM GetAccountOpeningTempData(tm_deposit td)
        {
            return _dac.GetAccountOpeningTempData(td);
        }

        internal AccOpenDM GetAccountOpeningData(tm_deposit td)
        {
            return _dac.GetAccountOpeningData(td);
        }

         internal int UpdateAccountOpeningData(AccOpenDM td)
        {
            return _dac.UpdateAccountOpeningData(td);
        }


        internal int UpdateAccountOpeningDataOrg(AccOpenDM td)
        {
            return _dac.UpdateAccountOpeningDataOrg(td);
        }

        internal int DeleteAccountOpeningData(td_def_trans_trf td)
        {
            return _dac.DeleteAccountOpeningData(td);
        } 
         internal string GetCustMinSavingsAccNo(tm_deposit cust)
        {
            return _dac.GetCustMinSavingsAccNo(cust);
        }


    }
}