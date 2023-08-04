using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class mm_po
    {
        public string ardb_cd { get; set; }
        public string state_cd { get; set; }
        public string dist_cd { get; set; }
        public string block_cd { get; set; }
        public string pin { get; set; }
        public string po_name { get; set; }
        public string service_area_cd { get; set; }
        public int ps_id { get; set; }
        public int po_id { get; set; }
    }
}
