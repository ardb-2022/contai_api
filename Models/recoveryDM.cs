using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Models
{
    public class recoveryDM
    {
        public demandblock_type recoveryblock { get; set; }
        public List<activitywiserecovery_type> recoveryactivity { get; set; }
        

        public recoveryDM()
        {
            this.recoveryblock = new demandblock_type();
            this.recoveryactivity = new List<activitywiserecovery_type>();            
        }

    }
}
