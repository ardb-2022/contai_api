using System;

namespace SBWSFinanceApi.Models
{
    public class tt_cash_account
    {
         public int srl_no {get; set;}
         public int dr_acc_cd {get; set;}
        public string brn_cd { get; set; }
        public string dr_particulars {get; set;}   
        public decimal dr_amt {get; set;}   
        public int cr_acc_cd {get; set;}   
         public string cr_particulars {get; set;}   
        public decimal cr_amt {get; set;} 
         public decimal cr_amt_tr {get; set;} 
         public decimal dr_amt_tr {get; set;} 
    }
}