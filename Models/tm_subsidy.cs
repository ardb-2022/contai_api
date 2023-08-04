using System;
using System.Collections.Generic;


namespace SBWSFinanceApi.Models
{
    public class tm_subsidy
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public Int64 loan_id { get; set; }
        public string loan_acc_no { get; set; }
        public DateTime? start_dt { get; set; }
        public DateTime? distribution_dt { get; set; }
        public  Decimal subsidy_amt { get; set; }
        public string subsidy_type { get; set; } 
        public Int64  subsidy { get; set; }
        public string modified_by { get; set; }
        public DateTime? modified_dt { get; set; }
        public string del_flag { get; set; }
        




    }
}
