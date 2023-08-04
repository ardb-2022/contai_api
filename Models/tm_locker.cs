using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class tm_locker
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public decimal cust_cd { get; set; }
        public string cust_name { get; set; }

        public string key_no { get; set; }

        public string agreement_no { get; set; }

        public DateTime agreement_dt { get; set; }

        public string locker_id { get; set; }

        public int acc_type_cd { get; set; }

        public string acc_num { get; set; }

        public int ind_acc_type_cd { get; set; }

        public string ind_acc_num { get; set; }

        public string narration { get; set; }

        public string nominee_no { get; set; }

        public DateTime rented_till { get; set; }

        public string acc_status { get; set; }

        public string approval_status { get; set; }

        public string name { get; set; }

        public string present_address { get; set; }

        public string occupation { get; set; }
        public string phone { get; set; }
        public decimal amt_recv { get; set; }
        public DateTime? trans_dt { get; set; }
        public Int64 trans_cd { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }
        public string approved_by { get; set; }
        public string trans_mode { get; set; }


    }
}
