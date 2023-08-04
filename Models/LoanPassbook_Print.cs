using System;


namespace SBWSFinanceApi.Models
{
    public class LoanPassbook_Print
    {
        public DateTime trans_dt { get; set; }
        public int trans_cd { get; set; }
        public string loan_id { get; set; }
        public string trans_type { get; set; }
        public decimal issue_amt { get; set; }
        public decimal curr_prn_recov { get; set; }
        public decimal ovd_prn_recov { get; set; }
        public decimal adv_prn_recov { get; set; }
        public decimal curr_intt_recov { get; set; }
        public decimal ovd_intt_recov { get; set; }
        public decimal penal_intt_recov { get; set; }
        public decimal curr_prn_bal { get; set; }
        public decimal ovd_prn_bal { get; set; }
        public decimal curr_intt_bal { get; set; }
        public decimal ovd_intt_bal { get; set; }
        public decimal penal_intt_bal { get; set; }
        public string particulars { get; set; }
        public string printed_flag { get; set; }
        public string ardb_cd { get; set; }

    }
}
