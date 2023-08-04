using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class blockwise_type
    {
        public demandblock_type blockwisetype { get; set; }
        public List<demand_list> demandlist { get; set; }

        public blockwise_type()
        {
            this.blockwisetype = new demandblock_type();
            this.demandlist = new List<demand_list>();
        }
    }
}