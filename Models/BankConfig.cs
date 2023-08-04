using System.Collections.Generic;

namespace SBWSAdminApi.Models
{
    public class BankConfig
    {
        public decimal bank_config_id { get; set; }
        public string bank_id { get; set; }
        public string bank_name { get; set; }
        public string bank_desc { get; set; }
        public string branch_cd { get; set; }
        public string branch_name { get; set; }
        public string branch_desc { get; set; }
        public string server_ip { get; set; }
        public string db_server_ip { get; set; }
        public string sms_provider { get; set; }
        public string active_flag { get; set; }
        public string del_flag { get; set; }
        public string user1 { get; set; }
        public string pass1 { get; set; }
        public string user2 { get; set; }
        public string pass2 { get; set; }
        public System.DateTime updated_dt { get; set; }
        public string created_by {get; set;}
        public string updated_by {get; set;}

    }
}