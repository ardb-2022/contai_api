using System;

namespace SBWSFinanceApi.Models
{
    public class tt_loan_opn_cls
    {
        public string acc_desc { get; set; }
        public decimal acc_cd { get; set; }
        public decimal loan_id { get; set; }
        public decimal cust_cd { get; set; }
        public DateTime? trans_dt { get; set; }
        public DateTime? sanc_dt { get; set; }
        public decimal sanc_amt { get; set; }
        public decimal disb_amt { get; set; }
        public decimal instl_no { get; set; }
        public double curr_rt { get; set; }
        public double ovd_rt { get; set; }
        public decimal closing_amt { get; set; }
        public decimal closing_intt { get; set; }
        public string status { get; set; }
        public string cust_name { get; set; }


    }
}


