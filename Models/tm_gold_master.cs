using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class tm_gold_master
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public string member_no { get; set; }
        public string cust_cd { get; set; }
        public Int64 valuation_no { get; set; }
        public DateTime value_dt { get; set; }
        public string loan_id { get; set; }
        public string lge_page { get; set; }
        public DateTime due_dt { get; set; }
        public string cust_name { get; set; }
        public string present_address { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }


    }
}
