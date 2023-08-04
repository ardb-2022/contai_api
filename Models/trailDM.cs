using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Models
{
    public class trailDM
    {
        public trail_type acc_type { get; set; }
        public List<tt_trial_balance> trailbalance { get; set; }

        public trailDM()
        {
            this.acc_type = new trail_type();
            this.trailbalance = new List<tt_trial_balance>();
        }

    }
}



