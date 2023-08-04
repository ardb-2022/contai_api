using System;
using System.Collections.Generic;
using SBWSFinanceApi.Models;

namespace SBWSDepositApi.Models
{
    public sealed class LoanOpenDM
    {
   
 public LoanOpenDM()
        {
            this.tmloanall = new tm_loan_all();
            this.tmdepositrenew = new tm_deposit();
            this.tmguaranter = new tm_guaranter();
            this.tmlaonsanction = new List<tm_loan_sanction>();
            this.tmlaonsanctiondtls = new List<tm_loan_sanction_dtls>();
            this.tdaccholder = new List<td_accholder>();
            this.tmdenominationtrans = new List<tm_denomination_trans>();
            this.tmtransfer = new List<tm_transfer>();
            this.tddeftranstrf = new List<td_def_trans_trf>();
            this.tddeftrans = new td_def_trans_trf();
            this.tdloansancsetlist = new List<td_loan_sanc_set>();

        }
        public tm_loan_all tmloanall { get; set; }
        public tm_deposit tmdepositrenew { get; set; }
        public tm_guaranter tmguaranter { get; set; }
        public List<tm_loan_sanction> tmlaonsanction { get; set; }
        public List<tm_loan_sanction_dtls> tmlaonsanctiondtls { get; set; }
        public List<td_accholder> tdaccholder{get;set;}
        public List<tm_denomination_trans> tmdenominationtrans { get; set; }
        public List<tm_transfer> tmtransfer { get; set; }
        public List<td_def_trans_trf> tddeftranstrf { get; set; }
        public td_def_trans_trf tddeftrans { get; set; }
        public List<td_loan_sanc_set> tdloansancsetlist { get; set; }
   }
}