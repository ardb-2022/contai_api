using System;

namespace SBWSFinanceApi.Models
{
    public class td_loan_charges
    {
        public string ardb_cd { get; set; }
        public string loan_id { get; set; }
        public int charge_id { get; set; }
        public string charge_type { get; set; }
        public DateTime charge_dt { get; set; }

        public decimal charge_amt { get; set; }

        public string remarks { get; set; }

        public string approval_status { get; set; }

        public string del_flag { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }
        public string approved_by { get; set; }
        public DateTime created_dt { get; set; }
        public DateTime modified_dt { get; set; }
        public DateTime approved_dt { get; set; }

    }
}



