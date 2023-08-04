using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class tm_gold_master_dtls
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public Int64 valuation_no { get; set; }
        public Int16 sl_no { get; set; }
        public string desc_val { get; set; }
        public Int16 desc_no { get; set; }
        public decimal gross_we { get; set; }
        public decimal alloy_stone_we { get; set; }
        public decimal net_we { get; set; }
        public decimal purity_we { get; set; }
        public decimal act_we { get; set; }
        public decimal act_rate { get; set; }
        public decimal net_value { get; set; }
        public string created_by { get; set; }
        public string modified_by { get; set; }

    }
}



