using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class td_user_transfer
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string new_brn_cd { get; set; }
        public string user_id { get; set; }
        public DateTime transfer_dt { get; set; }
        public string user_type { get; set; }
        public string approval_status { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }
        public string approved_by { get; set; }
        public DateTime created_dt { get; set; }
        public DateTime modified_dt { get; set; }
        public DateTime approved_dt { get; set; }

    }
}
