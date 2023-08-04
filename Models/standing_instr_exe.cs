using System;

namespace SBWSFinanceApi.Models
{
    public class standing_instr_exe
    {         
       public DateTime    trans_dt    {get; set;}
       public Int32       dr_acc_type {get; set;}
       public string      dr_acc_num  {get; set;}
       public Int32       cust_cd     {get; set;}
       public Int32       cr_acc_type {get; set;}
       public string      cr_acc_num  {get; set;}
       public decimal     amount      {get; set;}   
     }
}