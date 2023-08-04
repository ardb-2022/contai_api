using System;

namespace SBWSFinanceApi.Models
{
    public class m_user_master
    {   
        public string ardb_cd {get; set;}
        public string brn_cd {get; set;}
        public string user_id {get; set;}  
        public string password {get; set;}  
        public string login_status {get; set;}  
        public string user_type {get; set;}        
        public string user_first_name {get; set;} 
        public string user_middle_name {get; set;} 
        public string user_last_name {get; set;} 
        public string ip { get; set; }

    }
}