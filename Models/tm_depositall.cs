using System;

namespace SBWSDepositApi.Models
{
    public class tm_depositall: BaseModel
    {
        public string ardb_cd { get; set; }
        public string brn_cd { get; set; }
        public int acc_type_cd { get; set; }
        public string acc_num { get; set; }
        public int renew_id { get; set; }
        public decimal cust_cd { get; set; }
        public string intt_trf_type { get; set; }
        public int constitution_cd { get; set; }
        public int oprn_instr_cd { get; set; }
        public DateTime? opening_dt { get; set; }
        public decimal prn_amt { get; set; }
        public decimal intt_amt { get; set; }
        public string dep_period { get; set; }
        public decimal instl_amt { get; set; }
        public decimal instl_no { get; set; }
        public DateTime? mat_dt { get; set; }
        public decimal intt_rt { get; set; }
        public string tds_applicable { get; set; }
        public DateTime? last_intt_calc_dt { get; set; }
        public DateTime? acc_close_dt { get; set; }
        public decimal closing_prn_amt { get; set; }
        public decimal closing_intt_amt { get; set; }
        public decimal penal_amt { get; set; }
        public decimal ext_instl_tot { get; set; }
        public string mat_status { get; set; }
        public string acc_status { get; set; }
        public decimal curr_bal { get; set; }
        public decimal clr_bal { get; set; }
        public string standing_instr_flag { get; set; }
        public string cheque_facility_flag { get; set; }
        public string created_by { get; set; }
        public DateTime? created_dt { get; set; }
        public string modified_by { get; set; }
        public DateTime? modified_dt { get; set; }
        public string approval_status { get; set; }
        public string approved_by { get; set; }
        public DateTime? approved_dt { get; set; }
        public string user_acc_num { get; set; }
        public string lock_mode { get; set; }
        public string loan_id { get; set; }
        public string cert_no { get; set; }
        public decimal bonus_amt { get; set; }
        public decimal penal_intt_rt { get; set; }
        public decimal bonus_intt_rt { get; set; }
        public string transfer_flag { get; set; }
        public DateTime? transfer_dt { get; set; }
        public string agent_cd { get; set; }
        public string cust_type {get; set;}  
public string title {get; set;}   
public string first_name {get; set;}   
public string middle_name {get; set;}   
public string last_name {get; set;}    
public string cust_name {get; set;}   
public string guardian_name {get; set;}   
public DateTime? cust_dt {get; set;}   
public DateTime? dt_of_birth {get; set;}   
public decimal age {get; set;}   
public string sex {get; set;}   
public string marital_status {get; set;}   
public int catg_cd {get; set;}  
public decimal community {get; set;}   
public decimal caste {get; set;}    
public string permanent_address {get; set;}   
public decimal ward_no {get; set;}   
public string state {get; set;}   
public string dist {get; set;}   
public int pin {get; set;}   
public string vill_cd {get; set;}   
public string block_cd {get; set;}   
public string service_area_cd {get; set;}   
public string occupation {get; set;}   
public string phone {get; set;}   
public string present_address {get; set;}  
public string kyc_photo_type {get; set;}   
public string kyc_photo_no {get; set;}   
public string kyc_address_type {get; set;}   
public string kyc_address_no {get; set;}  
public string constitution_desc {get;set;}
        //public int temp_flag {get;set;}
         public int acc_cd {get;set;}
        public int intt_acc_cd {get;set;}  
        public int intt_prov_acc_cd {get;set;}

        public string del_flag { get; set; }
    }
}