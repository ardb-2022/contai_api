using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class dcstatement
    {
        public string loan_id { get; set; }
        public string party_name { get; set; }

        public string block_name { get; set; }

        public DateTime application_dt { get; set; }
        public string loan_case_no { get; set; }

        public DateTime sanction_dt { get; set; }

        public decimal sanction_amt { get; set; }

        public decimal disb_amt { get; set; }
        public DateTime disb_dt { get; set; }
        public string activity { get; set; }

        public decimal land_value { get; set; }

        public string land_area { get; set; }

        public string addl_land_area { get; set; }

        public decimal project_cost { get; set; }

        public decimal net_income_gen { get; set; }

        public string bond_no { get; set; }

        public DateTime bond_dt { get; set; }

        public string operator_name { get; set; }

        public decimal deposit_amt { get; set; }

        public decimal own_contribution { get; set; }

        public DateTime lso_dt { get; set; }

        public string lso_no { get; set; }

        public string sex { get; set; }

        public string culti_area { get; set; }

        public decimal culti_val { get; set; }

        public decimal pronote_amt { get; set; }

        public string machine { get; set; }



    }
}
