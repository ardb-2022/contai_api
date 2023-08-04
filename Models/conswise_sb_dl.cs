using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
    public class conswise_sb_dl
    {
        public cons_type constype { get; set; }
        public List<tt_sbca_dtl_list> ttsbcadtllist { get; set; }

        public conswise_sb_dl()
        {
            this.constype = new cons_type();
            this.ttsbcadtllist = new List<tt_sbca_dtl_list>();
        }
    }
}
