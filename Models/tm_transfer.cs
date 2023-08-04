using System;

namespace SBWSFinanceApi.Models
{
    public class tm_transfer : BaseModel
    {
         public DateTime? trf_dt {get;set;}  
          public Int32 trf_cd {get;set;}  
         public decimal trans_cd {get;set;}   
         public string created_by {get;set;}   
         public DateTime? created_dt {get;set;}   
         public string approval_status {get;set;}   
         public string approved_by {get;set;}   
         public DateTime? approved_dt {get;set;}   
         public string brn_cd {get;set;}
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }
    }
}