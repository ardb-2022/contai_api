using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class accwisegl
    {
        public acc_type acctype { get; set; }
        public List<tt_gl_trans> ttgltrans { get; set; }

        public accwisegl()
        {
            this.acctype = new acc_type();
            this.ttgltrans = new List<tt_gl_trans>();
        }
    }
}
