using System;

namespace SBWSFinanceApi.Models
{
    public class tt_dep_sub_cash_book
    {
        public string prn_intt_flag { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public decimal cash_dr { get; set; }
        public decimal trf_dr { get; set; }
        public decimal cash_cr { get; set; }
        public decimal trf_cr { get; set; }
        public decimal bal_amt { get; set; }
        public int constitution_cd { get; set; }
        public string cust_name { get; set; }
        public string brn_cd { get; set; }
        public string ardb_cd { get; set; }
        public string acc_type_desc { get; set; }
        public string constitution_desc { get; set; }
    }
}