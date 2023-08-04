using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class accwiseloansubcashbook
    {
        public acc_type acctype { get; set; }
        public List<tt_loan_sub_cash_book> ttloansubcashbook { get; set; }

        public accwiseloansubcashbook()
        {
            this.acctype = new acc_type();
            this.ttloansubcashbook = new List<tt_loan_sub_cash_book>();
        }
    }
}
