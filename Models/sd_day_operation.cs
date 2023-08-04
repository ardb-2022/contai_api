using System;

namespace SBWSFinanceApi.Models
{
public class sd_day_operation
{
        public string ardb_cd { get; set; }
        public string brn_cd {get; set;}   
public decimal cls_bal {get; set;}  
public string cls_flg {get; set;} 
public string closed_by {get; set;} 
public DateTime? closed_dt {get; set;} 
public DateTime? operation_dt {get; set;}
   }
}
