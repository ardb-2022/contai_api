using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class blockwisedisb_type
    {
        public block_type blocktype { get; set; }
        public List<tm_loan_all> tmloanall { get; set; }

        public blockwisedisb_type()
        {
            this.blocktype = new block_type();
            this.tmloanall = new List<tm_loan_all>();
        }
    }
}



