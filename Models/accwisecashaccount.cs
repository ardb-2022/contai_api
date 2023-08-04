using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class accwisecashaccount
    {
        public int acc_cd { get; set; }
        public string acc_name { get; set; }

        public decimal tot_dr_amt { get; set; }

        public decimal tot_cr_amt { get; set; }


    }
}
