using System;


namespace SBWSFinanceApi.Models
{
    public class tt_npa
    {
        public string acc_desc { get; set; }
        public decimal acc_cd { get; set; }
        public string loan_id { get; set; }
        public string case_no { get; set; }

        public string party_name { get; set; }
        public decimal intt_due { get; set; }
        public decimal stan_prn { get; set; }
        public decimal substan_prn { get; set; }
        public decimal d1_prn { get; set; }
        public decimal d2_prn { get; set; }
        public decimal d3_prn { get; set; }
        public DateTime? npa_dt { get; set; }
        public Int64 default_no { get; set; }

        public string block_name { get; set; }
        public string activity { get; set; }

        public DateTime? disb_dt { get; set; }
        public decimal disb_amt { get; set; }
        public decimal instl_amt1 { get; set; }

        public string periodicity { get; set; }

        public decimal ovd_prn { get; set; }
        public decimal ovd_intt { get; set; }

        public decimal penal_intt { get; set; }

        public decimal prn_due { get; set; }

        public decimal provision { get; set; }


        public DateTime? recov_dt { get; set; }

    }
}





