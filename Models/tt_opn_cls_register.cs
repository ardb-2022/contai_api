using System;

namespace SBWSFinanceApi.Models
{
    public class tt_opn_cls_register
    {
        public decimal acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public decimal cust_cd { get; set; }
        public DateTime? opn_cls_dt { get; set; }
        public decimal prn_amt { get; set; }
        public decimal intt_amt { get; set; }
        public decimal penal_amt { get; set; }
        public string status { get; set; }
        public decimal intt_rt { get; set; }
        public string dep_period { get; set; }
        public string agent_cd { get; set; }
        public string acc_type_desc { get; set; }
        public string cust_name { get; set; }


    }
}
