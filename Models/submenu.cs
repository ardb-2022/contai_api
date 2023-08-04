using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class submenu
    {
        public string name { get; set; } // TRANSACTION
        public List<screenlist> menu { get; set; }
        public string activeflag { get; set; }

    }
}