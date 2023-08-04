using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class activitywiserecovery_type
    {
        public demandactivity_type activitytype { get; set; }
        public List<recovery_list> recoverylist { get; set; }

        public activitywiserecovery_type()
        {
            this.activitytype = new demandactivity_type();
            this.recoverylist = new List<recovery_list>();
        }
    }
}
