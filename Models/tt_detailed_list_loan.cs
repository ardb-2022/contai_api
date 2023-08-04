using System;

namespace SBWSFinanceApi.Models
{
    public class tt_detailed_list_loan
    {
         public Int32 acc_cd {get; set;}
         public string party_name {get; set;}
         public float curr_intt_rate {get; set;}
         public float ovd_intt_rate {get; set;}
         public decimal curr_prn {get; set;}
         public decimal ovd_prn {get; set;}
         public decimal curr_intt {get; set;}
         public decimal ovd_intt {get; set;}
        public decimal penal_intt { get; set; }
        public string acc_name {get; set;}
         public string acc_num {get; set;}
         public string block_name {get; set;}
        public string activity_name { get; set; }
        public DateTime computed_till_dt {get; set;}
        public DateTime list_dt { get; set; }

        public string ledger_folio_no { get; set; }
    }
}