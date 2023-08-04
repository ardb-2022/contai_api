using System.Collections.Generic;

namespace SBWSAdminApi.Models
{
    public class UserLoginStat
    {
        public string login_id { get; set; }
        public string password { get; set; }
        public string MPIN { get; set; }
        public string active_flag { get; set; }
        public string del_flag { get; set; }
        public int is_found { get; set; }
        public decimal cust_cd { get; set; }
        public decimal bank_config_id { get; set; }
        public string bank_name { get; set; }
        public string sms_provider { get; set; }
        public string bank_desc { get; set; }
        public string server_ip { get; set;}

    }
}