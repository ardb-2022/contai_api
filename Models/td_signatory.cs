using System;

namespace SBWSDepositApi.Models
{
  public class td_signatory : BaseModel
  {
        public string ardb_cd { get; set; }
        public string brn_cd  {get; set;}
    public int    acc_type_cd  {get; set;}
    public string    acc_num  {get; set;} 
    public string signatory_name {get; set;}
        public string del_flag { get; set; }
        // public int temp_flag {get;set;}
    }
}