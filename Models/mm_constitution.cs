using System;

namespace SBWSFinanceApi.Models
{
    public class mm_constitution
    {
         public int acc_type_cd {get; set;} 
         public int constitution_cd {get;set;}
         public string constitution_desc {get;set;}
         public int acc_cd {get;set;}
        public int intt_acc_cd {get;set;}  
        public int intt_prov_acc_cd {get;set;} 
        public string allow_trans {get;set;}
       
    }
}