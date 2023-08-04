using System;

namespace SBWSFinanceApi.Models
{
    public class p_loan_param
    {
         public string ardb_cd { get; set; }
         public string brn_cd {get; set;}
         public DateTime? intt_dt {get; set;}  
         public string acc_cd {get;set;}
         public string loan_id {get;set;}
         public int acc_type_cd {get;set;}
         public decimal recov_amt {get;set;}       
        public decimal curr_intt_rate {get;set;}         

        public decimal curr_prn_recov {get;set;}
        public decimal  ovd_prn_recov {get;set;}
		public decimal 	curr_intt_recov {get;set;}
        public decimal   ovd_intt_recov {get;set;}
        public decimal penal_intt_recov { get; set; }
        public decimal adv_prn_recov { get; set; }
        public string gs_user_type {get;set;}
        public string gs_user_id {get;set;}
        public string output {get;set;}
        public int commit_roll_flag {get;set;}

        public string crop_cd {get;set;}
        public int cust_cd {get; set;}
        public DateTime? due_dt {get; set;}

        public int status {get; set;}
        public string del_flag { get; set; }

    }
}