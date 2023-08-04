using System;

namespace SBWSFinanceApi.Models
{
    public class tt_day_scroll
    {
        public DateTime trans_dt { get; set; }
        public Int64 voucher_id { get; set; }
        public Int64 trans_cd { get; set; }
        public int acc_cd { get; set; }
        public string acc_num { get; set; }
        public string cust_narration { get; set; }
        public string trans_type { get; set; }
        public string trf_type { get; set; }
        public string acc_type { get; set; }
        public decimal cash_recp { get; set; }
        public decimal trf_recp { get; set; }
        public decimal cash_pay { get; set; }
        public decimal trf_pay { get; set; }
        public decimal cash_opng_bal { get; set; }
        public decimal cash_clos_bal { get; set; }
        public Int32 cash_acc_cd { get; set; }
        public decimal cash_opng_bal_sl { get; set; }
        public decimal cash_clos_bal_sl { get; set; }
        public Int32 cash_acc_cd_sl { get; set; }
    }
}