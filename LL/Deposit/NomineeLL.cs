
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
    public class NomineeLL
    {
       NomineeDL _dac = new NomineeDL(); 
      internal List<td_nominee> GetNominee(td_nominee pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.GetNomineeTemp(pmc);
            else
            return _dac.GetNominee(pmc);
        }  

        internal decimal InsertNominee(td_nominee pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.InsertNomineeTemp(pmc);
            else
            return _dac.InsertNominee(pmc);
        } 

        internal int UpdateNominee(td_nominee pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.UpdateNomineeTemp(pmc);
            else
            return _dac.UpdateNominee(pmc);
        } 

          internal int DeleteNominee(td_nominee pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.DeleteNomineeTemp(pmc);
            else
            return _dac.DeleteNominee(pmc);
        } 

    }
}