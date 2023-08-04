
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
    public class IntroducerLL
    {
       IntroducerDL _dac = new IntroducerDL(); 

internal List<td_introducer> GetIntroducer(td_introducer pmc)
        {         
            if (pmc.temp_flag==1)
            return _dac.GetIntroducerTemp(pmc);
            else
            return _dac.GetIntroducer(pmc);
        }  

        internal decimal InsertIntroducer(td_introducer pmc)
        {         
            if (pmc.temp_flag==1)
            return _dac.InsertIntroducerTemp(pmc);
            else
            return _dac.InsertIntroducer(pmc);
        } 

        internal int UpdateIntroducer(td_introducer pmc)
        {         
            if (pmc.temp_flag==1)
            return _dac.UpdateIntroducerTemp(pmc);
            else
            return _dac.UpdateIntroducer(pmc);
        } 

          internal int DeleteIntroducer(td_introducer pmc)
        {         
            if (pmc.temp_flag==1)
            return _dac.DeleteIntroducerTemp(pmc);
            else
            return _dac.DeleteIntroducer(pmc);
        } 

    }
}