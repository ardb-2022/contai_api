using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class locker_rent
    {
        
        public string locker_type { get; set; }
        public double rentamt { get; set; }
        public DateTime eff_date { get; set; }
        
    }
}
