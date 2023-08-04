using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSDepositApi.Models
{
    public sealed class TransferDM
    {
   
 public TransferDM()
        {
            this.tmtransfer = new tm_transfer();
            this.tddeftranstrf = new List<td_def_trans_trf>();
            this.tddeftrans = new td_def_trans_trf();
        }
        public tm_transfer tmtransfer { get; set; }
        public List<td_def_trans_trf> tddeftranstrf { get; set; }
        public td_def_trans_trf tddeftrans { get; set; }
   }
}