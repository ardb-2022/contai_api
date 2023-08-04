
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
    public class AccholderLL
    {
       AccholderDL _dac = new AccholderDL(); 
internal List<td_accholder> GetAccholder(td_accholder pmc)
        {     
            if (pmc.temp_flag==1)
            return _dac.GetAccholderTemp(pmc);
            else           
            return _dac.GetAccholderTemp(pmc);
        }  

        internal decimal InsertAccholder(td_accholder pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.InsertAccholderTemp(pmc);
            else
            return _dac.InsertAccholder(pmc);
        } 

        internal int UpdateAccholder(td_accholder pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.UpdateAccholderTemp(pmc);
            else
            return _dac.UpdateAccholder(pmc);
        } 

          internal int DeleteAccholder(td_accholder pmc)
        {         
           if (pmc.temp_flag==1)
            return _dac.DeleteAccholderTemp(pmc);
            else
            return _dac.DeleteAccholder(pmc);
        } 

        
    }
}