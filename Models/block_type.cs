using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class block_type
    {
        public string block_cd { get; set; }
        public string block_name { get; set; }
        public decimal tot_block_curr_prn_recov { get; set; }
        public decimal tot__block_ovd_prn_recov { get; set; }
        public decimal tot__block_adv_prn_recov { get; set; }
        public decimal tot_block_curr_intt_recov { get; set; }
        public decimal tot_block_ovd_intt_recov { get; set; }
        public decimal tot_block_penal_intt_recov { get; set; }
        public decimal tot_block_recov { get; set; }
        public decimal tot_count_block { get; set; }

    }
}
