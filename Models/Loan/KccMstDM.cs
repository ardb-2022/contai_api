using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSDepositApi.Models
{
    public sealed class KccMstDM
    {
   
 public KccMstDM()
        {
            this.mmkccmemberdtls = new mm_kcc_member_dtls();
            this.mmlandregister = new List<mm_land_register>();
            this.tdkccsanctiondtls = new List<td_kcc_sanction_dtls>();
        }
         public mm_kcc_member_dtls mmkccmemberdtls { get; set; }
         public List<mm_land_register> mmlandregister { get; set; }
         public List<td_kcc_sanction_dtls> tdkccsanctiondtls { get; set; }
        
   }
}