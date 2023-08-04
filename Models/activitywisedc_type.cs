using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class activitywisedc_type
    {
        public string activity_cd { get; set; }
        public  string sex { get; set; }
        public decimal tot_pronote { get; set; }
        public decimal tot_disb { get; set; }

        public decimal tot_sanc { get; set; }
        public decimal tot_project { get; set; }

        public decimal tot_own_contribution { get; set; }

        public List<activitywiseloan> dclist { get; set; }

        public activitywisedc_type()
        {
            this.dclist = new List<activitywiseloan>();
        }
    }
}