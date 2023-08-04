using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class LockerOpenDM
    {
        public LockerOpenDM()
        {
            this.tmlocker = new tm_locker();
            this.tdnominee = new List<td_nominee_locker>();
            this.tdaccholder = new List<td_accholder_locker>();
            this.tmtransfer = new List<tm_transfer>();
            this.tddeftranstrf = new List<td_def_trans_trf>();
            this.tddeftrans = new td_def_trans_trf();
        }
        public tm_locker tmlocker { get; set; }
        public List<td_nominee_locker> tdnominee { get; set; }
        public List<td_accholder_locker> tdaccholder { get; set; }
        public List<tm_transfer> tmtransfer { get; set; }
        public List<td_def_trans_trf> tddeftranstrf { get; set; }
        public td_def_trans_trf tddeftrans { get; set; }
    }
}
