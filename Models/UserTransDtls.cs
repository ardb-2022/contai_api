using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class UserTransDtls
    {
        public string acc_desc { get; set; }
        public DateTime trans_dt { get; set; }

        public string loan_id { get; set; }
        public Int64 trans_cd { get; set; }
        public string trans_type { get; set; }
        public string trans_mode { get; set; }

        public string trf_type { get; set; }
        public double amount { get; set; }
        public decimal prn_recov { get; set; }
        public decimal intt_recov { get; set; }
        public double curr_intt_rt { get; set; }
        public double ovd_intt_rt { get; set; }

        public string user_name { get; set; }

        public string particulars { get; set; }


    }
}
