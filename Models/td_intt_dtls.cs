using System;

namespace SBWSDepositApi.Models
{
    public class td_intt_dtls
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public int renew_id { get; set; }   
         public double intt_amt { get; set; }
        public Int64 trans_cd { get; set; }
        public DateTime? calc_dt { get; set; }
        public DateTime? paid_dt { get; set; }
        public string paid_status { get; set; }
        public string del_flag { get; set; }

    }
}