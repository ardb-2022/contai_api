using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSDepositApi.Models
{
    public sealed class ShgDM
    {
   
 public ShgDM()
        {
            this.mmshg = new mm_shg();
            this.mmshgmember = new List<mm_shg_member>();
        }
         public mm_shg mmshg { get; set; }
         public List<mm_shg_member> mmshgmember { get; set; }
        
   }
}