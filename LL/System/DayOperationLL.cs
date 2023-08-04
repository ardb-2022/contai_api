
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class DayOperationLL
    {
       DayOperationDL _dac = new DayOperationDL(); 
        internal List<sd_day_operation> GetDayOperation(sd_day_operation pmc)
        {         
           
            return _dac.GetDayOperation(pmc);
        }

        internal int YearOpen(p_gen_param prp)
        {

            return _dac.YearOpen(prp);
        }
        internal p_gen_param W_DAY_CLOSE(p_gen_param pgp)
        {         
           
            return _dac.W_DAY_CLOSE(pgp);
        }  
        internal p_gen_param W_DAY_OPEN(p_gen_param pgp)
        {         
           
            return _dac.W_DAY_OPEN(pgp);
        }
                  
    }
}