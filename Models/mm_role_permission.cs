using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class mm_role_permission
    {
        public string ardb_cd { get; set; }
        public Int64 role_cd { get; set; }
        public string role_type { get; set; }
        public string module { get; set; }
        public string sub_module { get; set; }
        public string first_sub_module_item { get; set; }
        public string second_sub_module_item { get; set; }
        public string identification { get; set; }
        public string permission { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }

    }
}
