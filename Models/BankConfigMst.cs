using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class BankConfigMst
    {
        public string bankname { get; set; }
        // public string connstring { get; set; }
        public List<mainmenu> menu { get; set; }

        public connstring connstring { get; set; }
    }
}