using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class mm_locker
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string locker_type { get; set; }
        public string locker_id { get; set; }
        public string locker_status { get; set; }
        public string created_by { get; set; }
        public DateTime created_dt { get; set; }
        public string modified_by { get; set; }
        public DateTime modified_dt { get; set; }


    }
}
