using System;

namespace SBWSFinanceApi.Models
{
public class mm_customer
{
public string ardb_cd {get; set;}    
public string brn_cd {get; set;}   
public decimal cust_cd {get; set;}   
public string cust_type {get; set;}  
public string title {get; set;}   
public string first_name {get; set;}   
public string middle_name {get; set;}   
public string last_name {get; set;}    
public string cust_name {get; set;}   
public string guardian_name {get; set;}   
public DateTime? cust_dt {get; set;}   
public string old_cust_cd {get; set;}   
public DateTime? dt_of_birth {get; set;}   
public decimal age {get; set;}   
public string sex {get; set;}   
public string marital_status {get; set;}   
public int catg_cd {get; set;}
public string catg_desc { get; set; }
public decimal community {get; set;}   
public decimal caste {get; set;}    
public string permanent_address {get; set;}   
public decimal ward_no {get; set;}   
public string state {get; set;}   
public string dist {get; set;}   
public int pin {get; set;}   
public string vill_cd {get; set;}   
public string block_cd {get; set;}
        public int ps { get; set; }
        public int po { get; set; }
        public string service_area_cd {get; set;}   
public string occupation {get; set;}   
public string phone {get; set;}   
public string present_address {get; set;}  
public string farmer_type {get; set;}  
public string email {get; set;}     
public decimal monthly_income {get; set;}  
public DateTime? date_of_death {get; set;} 
public string sms_flag {get; set;}     
public string status {get; set;} 
public string pan {get; set;}   
public string nominee {get; set;}   
public string nom_relation {get; set;}
public string kyc_photo_type {get; set;}   
public string kyc_photo_no {get; set;}   
public string kyc_address_type {get; set;}   
public string kyc_address_no {get; set;}
        public string kyc_other_type { get; set; }
        public string kyc_other_no { get; set; }
        public string org_status {get; set;}   
public decimal org_reg_no {get; set;}   
public string created_by {get; set;}   
public DateTime? created_dt {get; set;}   
public string modified_by {get; set;}   
public DateTime? modified_dt {get; set;}
public string lbr_status {get; set;}  
public string is_weaker {get; set;}
        public string sb_folio_no { get; set; }
        public string td_folio_no { get; set; }
        public string fd_folio_no { get; set; }
        public string rd_folio_no { get; set; }
        public string mis_folio_no { get; set; }

        public string del_flag {get; set;}     }
}