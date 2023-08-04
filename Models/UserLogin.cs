using System.Collections.Generic;

namespace SBWSAdminApi.Models
{
    public class UserLogin
    {
        public string login_id { get; set; }
        public string password { get; set; }
        public decimal cust_cd { get; set; }
        public decimal bank_config_id { get; set; }
        public string access_type { get; set; }
        public string active_flag { get; set; }
        public string del_flag { get; set; }
        public string brn_cd { get; set; }
        public string MPIN { get; set; }
    }
}