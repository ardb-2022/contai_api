using System;

namespace SBWSDepositApi.Models
{
    public class tm_daily_deposit
        {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public DateTime paid_dt { get; set; }
        public decimal paid_amt { get; set; }
        public string agent_cd { get; set; }
        public string agent_name { get; set; }
        public string trans_type { get; set; }
        public string approval_status { get; set; }
        public decimal balance_amt { get; set; }
        public decimal trans_cd {get;set;}

        public string cust_name { get; set; }
    }
}