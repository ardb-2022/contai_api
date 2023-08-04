using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class UserwisetransDM
    {
        public UserType utype { get; set; }
        public List<UserTransDtls> utransdtls { get; set; }

        public UserwisetransDM()
        {
            this.utype = new UserType();
            this.utransdtls = new List<UserTransDtls>();
        }
    }
}


