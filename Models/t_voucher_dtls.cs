using System;

namespace SBWSFinanceApi.Models
{
    public class t_voucher_dtls
    {
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }
        public string cust_cd { get; set; }
        public string voucher_type { get; set; }
        public string created_by { get; set; }
        public DateTime created_dt { get; set; }
        public string brn_cd {get; set;}
         public DateTime voucher_dt {get; set;}   
         public int acc_cd {get; set;}   
        public decimal amount {get; set;}   
        public string debit_credit_flag {get; set;}   
        public string transaction_type {get; set;}  
        public Int64 instrument_no {get;set;} 
        public DateTime instrument_dt {get; set;}   
        public string bank_name {get; set;}   
        public string branch_name {get; set;} 
        public string narration {get; set;}   
        public string acc_name {get; set;}   
        public string approval_status {get; set;}   
        public int voucher_id {get; set;}   
        public string approved_by {get; set;}   
        public DateTime approved_dt {get; set;}   
        public decimal dr_amount {get; set;}
        public decimal cr_amount {get; set;}
        public string narrationdtl {get;set;}
        public DateTime from_dt {get; set;}   
        public DateTime to_dt {get; set;}   
   
    }
}