using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class blockwiserecovery_type
    {
        public block_type blocktype { get; set; }
        public List<gm_loan_trans> gmloantrans { get; set; }

        public blockwiserecovery_type()
        {
            this.blocktype = new block_type();
            this.gmloantrans = new List<gm_loan_trans>();
        }
    }
}
