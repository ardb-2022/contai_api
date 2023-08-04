
using System;
namespace SBWSFinanceApi.Models
{
    public sealed class td_kcc_sanction_dtls
    {
public decimal member_id                        {get; set;}
public string activity_cd                      {get; set;}
public string crop_cd                          {get; set;}
public decimal sanction_amt                     {get; set;}
public DateTime effective_dt                     {get; set;}
public string created_by                       {get; set;}
public DateTime created_dt                       {get; set;}
public string modified_by                      {get; set;}
public DateTime modified_dt                      {get; set;}
public DateTime sanction_date                    {get; set;}
public DateTime validity_dt                      {get; set;}
public string credit_limit_no                  {get; set;}
    }
}
