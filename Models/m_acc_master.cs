using System;

namespace SBWSFinanceApi.Models
{
    public class m_acc_master
    {
        public string ardb_cd { get; set; }
        public int schedule_cd {get; set;} 
         public int sub_schedule_cd {get; set;} 
         public int acc_cd {get; set;} 
        public string acc_name {get; set;}   
        public string acc_type {get; set;}  
        public string impl_flag {get; set;}  
        public string online_flag {get; set;}  
        public int mis_acc_cd {get; set;}        
        public string trading_flag {get; set;}   

        public int stock_cd {get; set;} 

        public int n_trial_cd {get; set;} 
    }
}