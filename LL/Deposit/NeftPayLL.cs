
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
    public class NeftPayLL
    {
       NeftPayDL _dac = new NeftPayDL(); 
      internal List<td_outward_payment> GetNeftOutDtls(td_outward_payment pmc)
        {         
              return _dac.GetNeftOutDtls(pmc);
            
        }
        
        internal List<neft_inward> NeftInward(p_report_param prp)
        {
            return _dac.NeftInward(prp);

        }
        
        internal List<td_outward_payment> NeftOutWard(p_report_param prp)
        {
            return _dac.NeftOutWard(prp);

        }

        internal int InsertNeftOutDtls(td_outward_payment nom)
        {
           return _dac.InsertNeftOutDtls(nom);
        }

        internal List<mm_ifsc_code> GetIfscCode(string ifsc)
        {
            return _dac.GetIfscCode(ifsc);
        }
         internal int UpdateNeftOutDtls(td_outward_payment nom)
        {
            return _dac.UpdateNeftOutDtls(nom);
        }
        internal int ApproveNeftPaymentTrans(td_outward_payment nom)
        {
            return _dac.ApproveNeftPaymentTrans(nom);
        }
        internal int DeleteNeftOutDtls(td_outward_payment nom)
        {
            return _dac.DeleteNeftOutDtls(nom);
        }   

        internal decimal GetNeftCharge(p_gen_param pgp)
        {
            return _dac.GetNeftCharge(pgp);
        }
    }
}