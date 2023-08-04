using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class agentwiseDL
    {
        public string agentname { get; set; }
        public decimal tot_prn { get; set; }
        public decimal tot_count { get; set; }
        public List<tt_sbca_dtl_list> ttsbcadtllist { get; set; }

        public agentwiseDL()
        {
            this.ttsbcadtllist = new List<tt_sbca_dtl_list>();
        }
    }
}