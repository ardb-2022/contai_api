
using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class TransferLL
    {
       TransferDL _dac = new TransferDL(); 
        internal TransferDM GetTransferData(td_def_trans_trf trf)
        {
           return _dac.GetTransferData(trf);
        }
        internal string InsertTransferData(TransferDM acc)
        {
           return _dac.InsertTransferData(acc);
        }
        internal int UpdateTransferData(TransferDM acc)
        {
            return _dac.UpdateTransferData(acc);
        }
        internal int DeleteTransferData(td_def_trans_trf acc)
        {
           return _dac.DeleteTransferData(acc);
        }
        internal string ApproveTransfer(p_gen_param pgp)
        {
            return _dac.ApproveTransfer(pgp);
        }
         internal List<tm_transfer> GetUnapproveTransfer(tm_transfer tdt)
        {
             return _dac.GetUnapproveTransfer(tdt);
        }
        internal List<tm_transfer> GetTransfer(tm_transfer pmc)
        {         
           
            return _dac.GetTransfer(pmc);
        }  
        internal int InsertTransfer(List<tm_transfer> tdt)
        {         
           
            return _dac.InsertTransfer(tdt);
        }
        internal int UpdateTransfer(List<tm_transfer> tdt)
        {         
           
            return _dac.UpdateTransfer(tdt);
        } 

        internal int DeleteTransfer(tm_transfer tdt)
        {         
           
            return _dac.DeleteTransfer(tdt);
        } 

        internal List<td_def_trans_trf> GetDepTransTrfwithChild(td_def_trans_trf tdt)
        {
            return _dac.GetDepTransTrfwithChild(tdt);
        }

        internal mm_dashboard GetDashBoardInfo(p_gen_param td)
        {
           return _dac.GetDashBoardInfo(td);
        }
            
    }
}