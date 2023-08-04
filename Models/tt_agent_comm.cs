using System;


namespace SBWSFinanceApi.Models
{
    public class tt_agent_comm
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string agent_cd { get; set; }
        public string agent_name { get; set; }
        public decimal deposited_amt { get; set; }
        public decimal commission { get; set; }
        public decimal interest { get; set; }
        public decimal withdrawal { get; set; }
        public decimal deduction { get; set; }  
        

    }
}
