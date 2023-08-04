using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class accwiserecovery_type
    {
        public acc_type acctype { get; set; }
        public List<blockwiserecovery_type> blockwiserecovery { get; set; }

        public accwiserecovery_type()
        {
            this.acctype = new acc_type();
            this.blockwiserecovery = new List<blockwiserecovery_type>();
        }
    }
}
