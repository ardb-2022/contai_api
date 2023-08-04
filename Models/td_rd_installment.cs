using System;

namespace SBWSDepositApi.Models
{
    public class td_rd_installment
    {
        public string acc_num { get; set; }
        public Int16 instl_no { get; set; }
        public DateTime? due_dt { get; set; }
        public DateTime? instl_dt { get; set; }
        public string status { get; set; }
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }

    }
}