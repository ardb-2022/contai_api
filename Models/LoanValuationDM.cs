using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class LoanValuationDM
    {
        public LoanValuationDM()
        {
            this.tmgoldmaster = new tm_gold_master();
            this.tmgoldmasterdtls = new List<tm_gold_master_dtls>();            
        }
        public tm_gold_master tmgoldmaster { get; set; }
        public List<tm_gold_master_dtls> tmgoldmasterdtls { get; set; }
        
    }
}
