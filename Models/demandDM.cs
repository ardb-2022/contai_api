using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Models
{
    public class demandDM
    {
        public demandblock_type demandblock { get; set; }
        public List<activitywise_type> demandactivity { get; set; }
        //public List<demand_list> demandlist { get; set; }

        public demandDM()
        {
            this.demandblock = new demandblock_type();
            this.demandactivity = new List<activitywise_type>();
           // this.demandlist = new List<demand_list>();
        }

    }
}

