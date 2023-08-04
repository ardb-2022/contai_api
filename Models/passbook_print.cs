using System;


namespace SBWSFinanceApi.Models
{
    public class passbook_print
    {
        public DateTime trans_dt { get; set; }
        public decimal trans_cd { get; set; }
        public Int16 acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public string trans_type { get; set; }
        public decimal instrument_num { get; set; }
        public decimal amount { get; set; }
        public string particulars { get; set; }
        public string printed_flag { get; set; }
        public decimal balance_amt { get; set; }
        public decimal running_bal { get; set; }
        public decimal rowcd { get; set; }
        public decimal deposit { get; set; }
        public decimal withdrawal { get; set; }
        public string ardb_cd { get; set; }

    }
}