using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSDepositApi.Models
{
    public sealed class AccOpenDM
    {
   
 public AccOpenDM()
        {
            this.tmdeposit = new tm_deposit();
            this.tmdepositrenew = new tm_deposit();
            this.tdintroducer = new List<td_introducer>();
            this.tdnominee = new List<td_nominee>();
            this.tdsignatory = new List<td_signatory>();
            this.tdaccholder = new List<td_accholder>();
            this.tmdenominationtrans = new List<tm_denomination_trans>();
            this.tmtransfer = new List<tm_transfer>();
            this.tddeftranstrf = new List<td_def_trans_trf>();
            this.tddeftrans = new td_def_trans_trf();
        }
        public tm_deposit tmdeposit { get; set; }
        public tm_deposit tmdepositrenew { get; set; }
        public List<td_introducer> tdintroducer { get; set; }
        public List<td_nominee> tdnominee { get; set; }
        public List<td_signatory> tdsignatory { get; set; }
        public List<td_accholder> tdaccholder{get;set;}
        public List<tm_denomination_trans> tmdenominationtrans { get; set; }
        public List<tm_transfer> tmtransfer { get; set; }
        public List<td_def_trans_trf> tddeftranstrf { get; set; }
        public td_def_trans_trf tddeftrans { get; set; }
   }
}