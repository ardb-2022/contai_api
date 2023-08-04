using System;

namespace SBWSFinanceApi.Models
{
    public class tt_acc_stmt
    {
        public DateTime trans_dt { get; set; }
        public string particulars { get; set; }
        public string c_name { get; set; }
        public string c_addr { get; set; }
        public decimal instrument_num { get; set; }
        public decimal balance { get; set; }
        public decimal srl_no { get; set; }
        public decimal dr_amt { get; set; }
        public decimal cr_amt { get; set; }

    }
}
