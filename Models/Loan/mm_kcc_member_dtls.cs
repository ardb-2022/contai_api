
using System;
namespace SBWSFinanceApi.Models
{
    public sealed class mm_kcc_member_dtls
    {
public decimal member_id                        {get; set;}
public string bank_member_id                   {get; set;}
public string member_name                      {get; set;}
public string kcc_no                           {get; set;}
public string memo_no                          {get; set;}
public string kcc_acc_no                       {get; set;}
public decimal land_qty                         {get; set;}
public decimal land_valuation                   {get; set;}
public string created_by                       {get; set;}
public DateTime created_dt                       {get; set;}
public string modified_by                      {get; set;}
public DateTime modified_dt                      {get; set;}
public string karbanama_no                     {get; set;}
public DateTime mortgage_dt                      {get; set;}
public string m_land_qty                       {get; set;}
public decimal m_land_val                       {get; set;}
public DateTime karbannama_validity_dt           {get; set;}
public string bsbd_no                          {get; set;}
    }
}
