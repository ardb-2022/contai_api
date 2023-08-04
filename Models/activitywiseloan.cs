using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class activitywiseloan
    {
        public string loan_id { get; set; }
        public List<dcstatement> dc_statement { get; set; }

        public activitywiseloan()
        {
            this.dc_statement = new List<dcstatement>();
        }

    }
}