using System;


namespace SBWSFinanceApi.Models
{
    public class sb_intt_list
    {
        public string ardb_cd { get; set; }
        public string acc_num { get; set; }
        public string constitution_cd { get; set; }
        public string name { get; set; }

        public Int16 acc_type_cd { get; set; }

        public decimal interest { get; set; }

        public decimal balance { get; set; }
    }
}
