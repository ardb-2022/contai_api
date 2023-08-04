using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class gm_loan_trans
    {
        public string brn_cd { get; set; }
        public Int32 acc_cd { get; set; }
        public string acc_typ_dsc { get; set; }
        public string loan_id { get; set; }
        public decimal party_cd { get; set; }
        public string cust_name { get; set; } //
        public DateTime? trans_dt { get; set; }
        public Int64 trans_cd { get; set; } //
        public decimal disb_amt { get; set; }
        public decimal curr_prn_recov { get; set; } //
        public decimal ovd_prn_recov { get; set; }
        public decimal adv_prn_recov { get; set; }
        public decimal curr_intt_recov { get; set; }
        public decimal ovd_intt_recov { get; set; }
        public decimal penal_intt_recov { get; set; }
        public DateTime? last_intt_calc_dt { get; set; }
        public decimal curr_intt_calculated { get; set; }
        public decimal ovd_intt_calculated { get; set; }
        public decimal penal_intt_calculated { get; set; }
        public decimal prn_trf { get; set; }
        public decimal intt_trf { get; set; }
        public decimal prn_trf_revert { get; set; }
        public decimal intt_trf_revert { get; set; }
        public decimal curr_prn { get; set; }
        public decimal ovd_prn { get; set; }
        public decimal curr_intt { get; set; }
        public decimal ovd_intt { get; set; }
        public decimal penal_intt { get; set; }
        public decimal recov_amt { get; set; }
        public string acc_name { get; set; }
    }
}
