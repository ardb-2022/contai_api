using System;
using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class tm_guaranter
    {
        public string loan_id {get;set;}      
public Decimal acc_cd {get;set;}       
public string gua_type {get;set;}     
public string gua_id {get;set;}       
public string gua_name {get;set;}     
public string gua_add {get;set;}      
public string office_name {get;set;}  
public string share_acc_num {get;set;}
public Int64 share_type {get;set;}   
public DateTime? opening_dt {get;set;}   
public decimal share_bal {get;set;}    
public string depart {get;set;}       
public string desig {get;set;}        
public decimal salary {get;set;}       
public string sec_58 {get;set;}       
public string mobile {get;set;}       
public Int64 srl_no {get;set;}
        public string ardb_cd { get; set; }
        public string del_flag { get; set; }

    }
}