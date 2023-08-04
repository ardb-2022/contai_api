using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class td_nominee_locker
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public Int16 nom_id { get; set; }
        public string nom_name { get; set; }
        public string nom_addr1 { get; set; }
        public string nom_addr2 { get; set; }
        public string phone_no { get; set; }
        public Single percentage { get; set; }
        public string relation { get; set; }
        public string del_flag { get; set; }
        public string upd_ins_flag { get; set; }
    }
}
