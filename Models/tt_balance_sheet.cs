using System;

namespace SBWSFinanceApi.Models
{
    public class tt_balance_sheet
    {
        public string acc_type { get; set; }
        public int acc_cd { get; set; }
        public string curr_yr { get; set; }
        public string prev_yr { get; set; }
        public decimal curr_bal { get; set; }
        public decimal prev_bal { get; set; }
        public string acc_name { get; set; }
        public string type { get; set; }
    }
}
