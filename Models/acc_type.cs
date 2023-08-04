using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class acc_type
    {
        public Int32 acc_cd { get; set; }
        public string acc_name { get; set; }
        public int acc_type_cd { get; set; }
        public decimal tot_acc_curr_prn_recov { get; set; }
        public decimal tot_acc_ovd_prn_recov { get; set; }
        public decimal tot_acc_adv_prn_recov { get; set; }
        public decimal tot_acc_curr_intt_recov { get; set; }
        public decimal tot_acc_ovd_intt_recov { get; set; }
        public decimal tot_acc_penal_intt_recov { get; set; }
        public decimal tot_acc_recov { get; set; }
        public decimal tot_count_acc { get; set; }
    }
}
