using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class cons_type
    {
        public int constitution_cd { get; set; }

        public string bank_cd { get; set; }
        public string branch_cd { get; set; }
        public string constitution_desc { get; set; }
        public decimal tot_cons_count { get; set; }
        public decimal tot_cons_balance { get; set; }
        public decimal tot_cons_intt_balance { get; set; }

    }
}
