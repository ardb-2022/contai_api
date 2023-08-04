using System;

namespace SBWSFinanceApi.Models
{
public class td_outward_payment
{
public string ardb_cd { get; set; }
public string    brn_cd              {get; set;} 
public DateTime? trans_dt            {get; set;}
public Int64     trans_cd            {get; set;}
public string    payment_type        {get; set;}
public string    bene_name           {get; set;}
public string    bene_code           {get; set;}
public decimal   amount              {get; set;}
public double   charge_ded          {get; set;}
public DateTime? date_of_payment     {get; set;}
public string    bene_acc_no         {get; set;}
public string    bene_ifsc_code      {get; set;}
public Int64     dr_acc_no           {get; set;}
public string    bene_email_id       {get; set;}
public Int64     bene_mobile_no      {get; set;}
public Int32     bank_dr_acc_type    {get; set;}
public string    bank_dr_acc_no      {get; set;}
public string    bank_dr_acc_name    {get; set;}
public string    credit_narration    {get; set;}
public string    payment_ref_no      {get; set;}
public string    status              {get; set;}
public string    rejection_reason    {get; set;}
public string    processing_remarks  {get; set;}
public string    cust_ref_no         {get; set;}
public DateTime? value_date          {get; set;}
public string    created_by          {get; set;}
public DateTime? created_dt          {get; set;}
public string    modified_by         {get; set;}
public DateTime? modified_dt         {get; set;}
public string    approved_by         {get; set;}
public DateTime? approved_dt         {get; set;}
public string    approval_status     {get; set;}
public string del_flag { get; set; }

    }
}