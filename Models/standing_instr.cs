using System;

namespace SBWSFinanceApi.Models
{
    public class standing_instr
    {         
       public string    acc_type_from {get; set;}
       public string acc_num_from {get; set;}
       public string    acc_type_to {get; set;}
       public string acc_num_to {get; set;}
       public string instr_status {get; set;}
       public DateTime first_trf_dt {get; set;}
       public string periodicity {get; set;}
       public string prn_intt_flag {get; set;}
       public decimal amount {get; set;}
       public decimal srl_no  {get; set;}
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }
        public DateTime trans_dt { get; set; }
        public string cust_name { get; set; }
    }
}