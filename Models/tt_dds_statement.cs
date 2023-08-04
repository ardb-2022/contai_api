using System;

namespace SBWSFinanceApi.Models
{
    public class tt_dds_statement
    {
        public DateTime paid_dt { get; set; }
        public string cust_name { get; set; }
        public string acc_num { get; set; }
        public string agent_cd { get; set; }
        public decimal paid_amt { get; set; }
        public decimal opng_bal { get; set; }
        public decimal clr_bal { get; set; }
        
    }
}
