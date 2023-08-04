using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class tt_int_subsidy
    {
        public string loan_id { get; set; }
        public string party_name { get; set; }
        public string block_name { get; set; }
        public decimal disb_amt { get; set; }
        public decimal loan_balance { get; set; }
        public float curr_intt_rate { get; set; }
        public decimal prn_recov { get; set; }
        public decimal intt_recov { get; set; }
        public decimal subsidy_amt { get; set; }
        
    }
}
