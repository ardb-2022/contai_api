using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SBWSFinanceApi.Models
{
    public class neft_inward
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public DateTime trans_dt { get; set; }
        public Int64 trans_cd { get; set; }
        public string receive_type { get; set; }
        public string bank_cr_acc_no { get; set; }
        public decimal amount { get; set; }
        public DateTime date_of_receive { get; set; }
        public string payment_ref_no { get; set; }
        public string sender_acc_no { get; set; }
        public string sender_ifsc_code { get; set; }
        public string sender_name { get; set; }
        public Int64 cr_acc_no { get; set; }
        public string bank_cr_acc_name { get; set; }
        public string bank_name { get; set; }
        public string status { get; set; }
        public string rejection_reason { get; set; }
        public string cust_ref_no { get; set; }
        public DateTime value_date { get; set; }
    }
}