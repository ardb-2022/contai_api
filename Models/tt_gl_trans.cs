using System;

namespace SBWSFinanceApi.Models
{
    public class tt_gl_trans
    {
        public int acc_cd { get; set; }
        public int voucher_id { get; set; }
        public string brn_cd { get; set; }
        public string narration { get; set; }
        public DateTime voucher_dt { get; set; }
        public string voucher_type { get; set; }
        public decimal dr_amt { get; set; }
        public decimal cr_amt { get; set; }
        public decimal cum_bal { get; set; }
        public decimal trans_month { get; set; }
        public decimal trans_year { get; set; }
        public decimal opng_bal { get; set; }
        public string trans_year_month { get; set; }
        public string acc_cd_desc { get; set; }
    }
}