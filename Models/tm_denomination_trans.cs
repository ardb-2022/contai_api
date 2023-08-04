using System;

namespace SBWSFinanceApi.Models
{
    public class tm_denomination_trans : BaseModel
    {
         public string brn_cd {get; set;}
         public Int64 trans_cd {get; set;}
        public DateTime? trans_dt {get; set;} 
        public double rupees {get; set;}   
        public Int64 count {get; set;}   
         public double total {get; set;}
         public DateTime? created_dt {get; set;} 
         public string created_by {get; set;}   
        
    }
}