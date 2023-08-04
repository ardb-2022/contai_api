using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class weekly_return
    {
        public string brn_cd { get; set; }
        public string ardb_cd { get; set; }
        public Int32 srl_no { get; set; }
        public string cr_particulars { get; set; }
        public decimal cr_amt { get; set; }
        public string dr_particulars { get; set; }
        public decimal dr_amt { get; set; }
        public Int64 cr_acc_cd { get; set; }
        public Int64 dr_acc_cd { get; set; }

    }
}