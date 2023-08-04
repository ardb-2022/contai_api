using System;
using System.Collections.Generic;

namespace SBWSFinanceApi.Models
{
    public class tm_loan_all
    {
        public string brn_cd	 			{get;set;}       
        public decimal party_cd			{get;set;}       
        public Int32 acc_cd				{get;set;}        
        public string loan_id			{get;set;}	       
        public string loan_acc_no		{get;set;}	    
public decimal prn_limit			{get;set;}      
public decimal disb_amt			{get;set;}       
public DateTime? disb_dt			{get;set;}	       
public decimal curr_prn			{get;set;}       
public decimal ovd_prn			{get;set;}	       
public decimal curr_intt			{get;set;}      
public decimal ovd_intt			{get;set;}
        public decimal penal_intt { get; set; }
        public decimal pre_emi_intt		{get;set;}    
public decimal other_charges		{get;set;}   
public double curr_intt_rate		{get;set;}  
public double ovd_intt_rate		{get;set;}   
public string disb_status		{get;set;}	    
public string piriodicity		{get;set;}	    
public int    tenure_month		{get;set;}    
public DateTime?  instl_start_dt		{get;set;}  
public string created_by			{get;set;}     
public DateTime?  created_dt			{get;set;}     
public string modified_by		{get;set;}	    
public DateTime?  modified_dt		{get;set;}	    
public DateTime?  last_intt_calc_dt	{get;set;}
public DateTime?  ovd_trf_dt			{get;set;}     
public string approval_status	{get;set;}	 
public string cc_flag			{get;set;}	       
public string cheque_facility	{get;set;}	 
public string intt_calc_type		{get;set;}  
public int    emi_formula_no		{get;set;}  
public string rep_sch_flag		{get;set;}    
public DateTime?  loan_close_dt		{get;set;}   
public string loan_status		{get;set;}	    
public decimal instl_amt			{get;set;}      
public int     instl_no			{get;set;}       
public string activity_cd		{get;set;}	    
public string activity_dtls		{get;set;}   
public string sector_cd			{get;set;}      
public string fund_type			{get;set;}      
public decimal comp_unit_no	 	{get;set;}  
public string cust_name {get;set;}
public decimal tot_share_holding {get;set;}
public DateTime? trans_dt {get;set;}
public Int64 trans_cd {get;set;}
        public string acc_desc{ get; set; }

        public string block_name { get; set; }
        public string activity_name { get; set; }

        public string ardb_cd { get; set; }
        public string del_flag { get; set; }


    }
}