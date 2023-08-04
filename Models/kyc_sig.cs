using System;

namespace SBWSFinanceApi.Models
{
    public class kyc_sig
    {
        public Int64 cust_cd { get; set; }
        public string created_by { get; set; }
        public DateTime? created_dt { get; set; }
        public string img_typ { get; set; }
        public string img_cont { get; set; }
      //  public byte[] img_cont_byte { get; set; }
        public string status { get; set; }

        public string ardb_cd { get; set; }
    }
}