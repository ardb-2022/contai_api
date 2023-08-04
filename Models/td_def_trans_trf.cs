using System;

namespace SBWSFinanceApi.Models
{
    public class td_def_trans_trf : BaseModel
    {
        
         public DateTime? trans_dt {get;set;}   
         public Int64 trans_cd {get;set;}   
         public Int32 acc_type_cd {get;set;}   
         public string acc_num {get;set;}   
         public string trans_type {get;set;}   
         public string trans_mode {get;set;}   
         public double amount {get;set;}   
         public DateTime? instrument_dt {get;set;}   
         public Int64 instrument_num {get;set;}   
         public string paid_to {get;set;}   
         public string token_num {get;set;}   
         public string created_by {get;set;}   
         public DateTime? created_dt {get;set;}   
         public string modified_by {get;set;}   
         public DateTime? modified_dt {get;set;}   
         public string approval_status {get;set;}   
         public string approved_by {get;set;}   
         public DateTime? approved_dt {get;set;}   
         public string particulars {get;set;}   
         public Int32 tr_acc_type_cd {get;set;}   
         public string tr_acc_num {get;set;}   
         public DateTime? voucher_dt {get;set;}   
         public decimal voucher_id {get;set;}   
         public string trf_type {get;set;}   
         public int tr_acc_cd {get;set;}   
         public int acc_cd {get;set;}   
         public decimal share_amt {get;set;}   
         public decimal sum_assured {get;set;}   
         public decimal paid_amt {get;set;}   
         public decimal curr_prn_recov {get;set;}   
         public decimal ovd_prn_recov {get;set;}   
         public decimal curr_intt_recov {get;set;}   
         public decimal ovd_intt_recov {get;set;}   
         public string remarks {get;set;}   
         public string crop_cd {get;set;}   
         public string activity_cd {get;set;}   
         public double curr_intt_rate {get;set;}   
         public double ovd_intt_rate {get;set;}   
         public Int32 instl_no {get;set;}   
         public DateTime? instl_start_dt {get;set;}   
         public Int16 periodicity {get;set;}   
         public decimal disb_id {get;set;}   
         public decimal comp_unit_no {get;set;}   
         public decimal ongoing_unit_no {get;set;}   
         public decimal mis_advance_recov {get;set;}   
         public decimal audit_fees_recov {get;set;}
        public decimal penal_intt_recov { get; set; }
        public decimal adv_prn_recov { get; set; }
        public string sector_cd {get;set;}   
         public string spl_prog_cd {get;set;}   
         public string borrower_cr_cd {get;set;}   
         public DateTime? intt_till_dt {get;set;}   
         public string acc_name  {get;set;}  
         public string brn_cd {get;set;}
        public string home_brn_cd { get; set; }
        public string intra_branch_trn { get; set; }
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }

        public string agreement_no { get; set; }
        

    }
}