using System;

namespace SBWSFinanceApi.Models
{
    public class tt_rep_sch
    {
        public DateTime due_dt { get; set; }
        public string loan_id { get; set; }
        public string status { get; set; }
         public decimal rep_id { get; set; }
        public decimal instl_prn { get; set; }
        public decimal instl_paid { get; set; }
       
        
    }
}