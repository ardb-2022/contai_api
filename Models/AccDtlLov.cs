using System;
namespace SBWSFinanceApi.Models
{
    public sealed class AccDtlsLov
    {
        public string loan_id { get; set; }
        public Int32 acc_cd { get; set; }
        public string acc_num { get; set; }
        public string cust_name { get; set; }
        public string guardian_name { get; set; }
        public string present_address { get; set; }
        public string phone { get; set; }
        public DateTime? opening_dt { get; set; }
        public DateTime? disb_dt { get; set; }
        public Decimal Outstanding { get; set; }
    }
}