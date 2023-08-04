using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class locker_dtls_report
    {
        public string locker_type { get; set; }
        public string locker_id { get; set; }

        public string locker_status { get; set; }

        public string name { get; set; }

        public DateTime rented_till { get; set; }

        public string agreement_no { get; set; }

        public DateTime agreement_dt { get; set; }

        public string key_no { get; set; }


    }
}
