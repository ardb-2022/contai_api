using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class InvOpenDM
    {
        public InvOpenDM()
        {
            this.tmdepositInv = new tm_deposit_inv();
            this.tmdepositrenewInv = new tm_deposit_inv();
            //this.tmdenominationtrans = new List<tm_denomination_trans>();
            this.tmtransfer = new List<tm_transfer>();
            this.tddeftranstrf = new List<td_def_trans_trf>();
            this.tddeftrans = new td_def_trans_trf();
        }
        public tm_deposit_inv tmdepositInv { get; set; }
        public tm_deposit_inv tmdepositrenewInv { get; set; }
       // public List<tm_denomination_trans> tmdenominationtrans { get; set; }
        public List<tm_transfer> tmtransfer { get; set; }
        public List<td_def_trans_trf> tddeftranstrf { get; set; }
        public td_def_trans_trf tddeftrans { get; set; }
    }
}

