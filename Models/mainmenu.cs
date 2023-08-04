using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class mainmenu
    {
        public string name { get; set; } // UCIC
        public List<submenu> menu { get; set; }
        public string activeflag { get; set; }
    }
}