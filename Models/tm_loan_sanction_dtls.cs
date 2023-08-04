using System;
using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class tm_loan_sanction_dtls
    {
                 public string loan_id {get;set;}  
         public decimal  sanc_no {get;set;}  
         public string sector_cd {get;set;}  
         public string activity_cd {get;set;}  
         public string crop_cd {get;set;}  
         public decimal sanc_amt {get;set;}  
         public DateTime? due_dt {get;set;}  
         public string sanc_status {get;set;}  
         public decimal srl_no {get;set;}  
         public string approval_status {get;set;}  

         public string crop_desc {get;set;}
         public string activity_desc {get;set;}
         public string sector_desc {get;set;}
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }

    }
}
