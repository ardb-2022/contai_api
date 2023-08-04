using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class cashaccountDM
    {
        public accwisecashaccount acccashaccount { get; set; }
        public List<tt_cash_account> cashaccount { get; set; }

        public cashaccountDM()
        {
            this.acccashaccount = new accwisecashaccount();
            this.cashaccount = new List<tt_cash_account>();
        }
    }
}
