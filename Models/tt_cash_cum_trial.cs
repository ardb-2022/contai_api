using System;

namespace SBWSFinanceApi.Models
{
    public class tt_cash_cum_trial
    {
         public int acc_cd {get; set;}   
         public string acc_name {get; set;}   
        public string acc_type {get; set;} 
        public decimal opng_dr {get; set;}   
        public decimal opng_cr {get; set;} 
        public decimal dr {get; set;}   
        public decimal cr {get; set;}  
        public decimal clos_dr {get; set;}   
        public decimal clos_cr {get; set;}    
        
    }
}