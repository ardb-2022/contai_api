using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class demand_notice
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string loan_case_no { get; set; }
        public string cust_name { get; set; }
        public string cust_address { get; set; }
        public string block_name { get; set; }
        public string activity_name { get; set; }
        public string loan_id { get; set; }
        public double curr_intt_rate { get; set; }
        public double ovd_intt_rate { get; set; }

        public decimal disb_amt { get; set; }
        public decimal due_prn { get; set; }
        public decimal curr_prn { get; set; }
        public decimal ovd_prn { get; set; }
        public decimal curr_intt { get; set; }
        public decimal ovd_intt { get; set; }
        public decimal penal_intt { get; set; }




    }
}
