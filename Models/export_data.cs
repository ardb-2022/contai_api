using System;

namespace SBWSFinanceApi.Models
{
    public class export_data
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string agent_cd { get; set; }
        public string agent_name { get; set; }

        public string acc_num { get; set; }

        public string cust_name { get; set; }

        public string opening_dt { get; set; }

        public decimal curr_bal_amt { get; set; }
        public decimal interest { get; set; }
        public decimal balance_amt { get; set; }
        public string password { get; set; }
        public string machine_type { get; set; }
        public string user_id { get; set; }
       

    }
}