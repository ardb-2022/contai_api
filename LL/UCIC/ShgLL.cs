
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
    public class ShgLL
    {
       ShgDL _dac = new ShgDL(); 
       internal ShgDM GetShgData(ShgDM td)
       {   
           
            return _dac.GetShgData(td);
        }  
         internal string InsertShgData(ShgDM acc)
        {        
           
            return _dac.InsertShgData(acc);
        }
        internal int UpdateShgData(ShgDM acc) 
        {       
           
            return _dac.UpdateShgData(acc);
        }  
        internal int DeleteShgData(ShgDM acc) 
        {         
           
            return _dac.DeleteShgData(acc);
        }   
         

        
    }
}