using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class td_accholder_locker
    {
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public string acc_holder { get; set; }
        public string relation { get; set; }
        public decimal cust_cd { get; set; }
        public string cust_name { get; set; }
        // public int    temp_flag    {get; set;}
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }
        public string upd_ins_flag { get; set; }
    }
}
