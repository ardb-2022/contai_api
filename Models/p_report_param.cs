using System;

namespace SBWSFinanceApi.Models
{
    public class p_report_param
    {
        public string ardb_cd { get; set; }

        public string acc_num { get; set; }

        public string loan_id { get; set; }
        public int acc_cd {get; set;}
         public string brn_cd {get; set;}

        public string flag { get; set; }
        public DateTime from_dt {get; set;}
        public DateTime adt_as_on_dt { get; set; }
        public DateTime to_dt {get; set;} 
         public DateTime trial_dt {get; set;}
        public DateTime adt_dt { get; set; }
        public int pl_acc_cd {get; set;}
         public int gp_acc_cd {get; set;}         
         public int ad_from_acc_cd {get; set;}
         public int ad_to_acc_cd {get; set;}
        public int const_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string fund_type { get; set; }
        public int lines_printed { get; set; }
        public string cust_cd { get; set; }
        public string print_status { get; set; }
        public int renew_id { get; set; }
        public int ad_cash_acc_cd { get; set; }
        public string sex { get; set; }
        public string locker_id { get; set; }
        public string activity_cd { get; set; }


    }
}