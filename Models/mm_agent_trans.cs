using System;

namespace SBWSFinanceApi.Models
{
    public class mm_agent_trans
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string agent_cd { get; set; }
        public string agent_name { get; set; }
        public DateTime trans_dt { get; set; }
        public decimal trans_amt { get; set; }
        public string trans_type { get; set; }
        public string approval_status { get; set; }
        public DateTime approved_dt { get; set; }
        public Int64 trans_cd { get; set; }
        public string del_flag { get; set; }

        public string user_id { get; set; }




    }
}
