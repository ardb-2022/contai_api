using System;

namespace SBWSFinanceApi.Models
{
    public class p_gen_param
    {   public string ardb_cd { get; set; }
        public int acc_cd { get; set; }
        public string brn_cd { get; set; }
        public DateTime from_dt { get; set; }
        public DateTime to_dt { get; set; }
        public DateTime trial_dt { get; set; }
        public int pl_acc_cd { get; set; }
        public int gp_acc_cd { get; set; }
        public int ad_from_acc_cd { get; set; }
        public int ad_to_acc_cd { get; set; }
        public int gs_acc_type_cd { get; set; }
        public int ls_catg_cd { get; set; }
        public int ls_cons_cd { get; set; }
        public int ad_acc_type_cd { get; set; }
        public decimal ad_prn_amt { get; set; }
        public DateTime adt_temp_dt { get; set; }
        public string as_intt_type { get; set; }
        public int ai_period { get; set; }
        public decimal ad_intt_rt { get; set; }
        public string as_acc_num { get; set; }
        public decimal ad_instl_amt { get; set; }
        public int an_instl_no { get; set; }
        public decimal an_intt_rate { get; set; }
        public DateTime adt_trans_dt { get; set; }
        public int ad_trans_cd { get; set; }
        public string flag { get; set; }
        public string gs_user_type { get; set; }
        public string gs_user_id { get; set; }
        public string output { get; set; }
        public string as_cust_name { get; set; }

        public string as_locker_id { get; set; }

        public int acc_type_cd { get; set; }

        public int catg_cd { get; set; }

        public int no_of_days { get; set; }

        public int role_cd { get; set; }
    }
}