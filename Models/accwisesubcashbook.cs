using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class accwisesubcashbook
    {
        public acc_type acctype { get; set; }
        public List<tt_dep_sub_cash_book> ttdepsubcashbook { get; set; }

        public accwisesubcashbook()
        {
            this.acctype = new acc_type();
            this.ttdepsubcashbook = new List<tt_dep_sub_cash_book>();
        }
    }
}
