using System;
using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class t_voucher_narration
    {
         public string brn_cd {get; set;}
         public DateTime voucher_dt {get; set;}   
        public string narration {get; set;}   
        public int voucher_id {get; set;}   
        public string voucher_typ {get; set;}   
        public string voucher_status {get; set;}  
        public List<t_voucher_dtls> vd {get;set;} 
    }
}