using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class mm_bank_inv
    {
        public string ardb_cd { get; set; }
        public Int32 bank_cd { get; set; }
        public string bank_name { get; set; }
        public string bank_addr { get; set; }
        public string phone_no { get; set; }
        public string created_by { get; set; }
        public DateTime created_dt { get; set; }
        public string modified_by { get; set; }
        public DateTime modified_dt { get; set; }

    }
}
