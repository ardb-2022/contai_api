using System.Collections.Generic;

namespace SBWSAdminApi.Models
{
    public class MenuConfig
    {
        public MenuConfig()
        {
            this.ChildMenuConfigs = new List<MenuConfig>();
        }
        public decimal bank_config_id { get; set; }
        public decimal parent_menu_id { get; set; }
        public decimal menu_id { get; set; }
        public decimal level_no { get; set; }
        public string menu_name { get; set; }
        public string ref_page { get; set; }
        public string is_screen { get; set; }
        public string active_flag { get; set; }
        public string del_flag { get; set; }
        public List<MenuConfig> ChildMenuConfigs { get; set; }
    }
}