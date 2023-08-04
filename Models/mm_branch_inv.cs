using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class mm_branch_inv
    {
        public string ardb_cd { get; set; }
        public Int32 bank_cd { get; set; }
        public Int32 branch_cd { get; set; }
        public string branch_name { get; set; }
        public string branch_addr { get; set; }
        public string branch_phone { get; set; }
        public string created_by { get; set; }
        public DateTime created_dt { get; set; }
        public string modified_by { get; set; }
        public DateTime modified_dt { get; set; }
    }
}
