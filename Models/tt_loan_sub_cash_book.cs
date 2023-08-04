using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class tt_loan_sub_cash_book
    {
        public Int32 acc_type_cd { get; set; }
        public string acc_typ_dsc { get; set; }
        public string acc_num { get; set; }
        public decimal cash_dr { get; set; }
        public decimal trf_dr { get; set; }
        public decimal cash_cr { get; set; }
        public decimal trf_cr { get; set; }
        public string cust_name { get; set; }
    }
}
