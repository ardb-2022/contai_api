using System;

namespace SBWSFinanceApi.Models
{
    public class tt_sbca_dtl_list
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public decimal cust_cd { get; set; }
        public int constitution_cd { get; set; }
        public string constitution_desc { get; set; }
        public string cust_name { get; set; }
        public DateTime? opening_dt { get; set; }
        public DateTime? mat_dt { get; set; }
        public decimal balance { get; set; }
        public decimal INSTL_AMT { get; set; }
        public decimal PRN_AMT { get; set; }
        public decimal intt_prov_amt { get; set; }
        public Int32 rowmark { get; set; }
        public decimal INTT_RT { get; set; }
        public decimal PROV_INTT_AMT { get; set; }
        public string guardian_name { get; set; }
        public string present_address { get; set; }
        public DateTime? dt_of_birth { get; set; }
        public decimal age { get; set; }
        public string acc_type_desc { get; set; }

        public string agent_name { get; set; }

        public string bank_name { get; set; }

        public string branch_name { get; set; }

        public int no_of_account { get; set; }

    }

}

