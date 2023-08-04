using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSDepositApi.Models;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Controllers
{
    [Route("api/Common")]
    [ApiController]  
    [EnableCors("AllowOrigin")] 
    public class DenominationController : ControllerBase
    {
         DenominationDL _ll = new DenominationDL(); 
        [Route("GetDenominationDtls")]
        [HttpPost]
        public List<tm_denomination_trans> GetDenominationDtls([FromBody] tm_denomination_trans tdt)
        {
           return _ll.GetDenominationDtls(tdt);
        }
        [Route("GetDenomination")]
        [HttpPost]
        public List<tt_denomination> GetDenomination()
        {
           return _ll.GetDenomination();
        }
       
        [Route("InsertDenominationDtls")]
        [HttpPost]
        public int InsertDenominationDtls([FromBody] List<tm_denomination_trans> tdt)
        {
           return _ll.InsertDenominationDtls(tdt);
        }
        
        [Route("UpdateDenominationDtls")]
        [HttpPost]
        public int UpdateDenominationDtls([FromBody] List<tm_denomination_trans> tdt)
        {
           return _ll.UpdateDenominationDtls(tdt);
        }

        [Route("DeleteDenominationDtls")]
        [HttpPost]
        public int DeleteDenominationDtls([FromBody] tm_denomination_trans tdt)
        {
           return _ll.DeleteDenominationDtls(tdt);
        }

       
        DepTransactionTrfLL _ll1 = new DepTransactionTrfLL(); 
        [Route("GetDepTransTrf")]
        [HttpPost]
        public List<td_def_trans_trf> GetDepTransTrf([FromBody] td_def_trans_trf tdt)
        {
           return _ll1.GetDepTransTrf(tdt);
        }
        [Route("InsertDepTransTrf")]
        [HttpPost]
        public int InsertDepTransTrf([FromBody] List<td_def_trans_trf> tdt)
        {
           return _ll1.InsertDepTransTrf(tdt);
        }
        
        [Route("UpdateDepTransTrf")]
        [HttpPost]
        public int UpdateDepTransTrf([FromBody] List<td_def_trans_trf> tdt)
        {
           return _ll1.UpdateDepTransTrf(tdt);
        }
        [Route("DeleteDepTransTrf")]
        [HttpPost]
        public int DeleteDepTransTrf([FromBody] td_def_trans_trf tdt)
        {
           return _ll1.DeleteDepTransTrf(tdt);
        }
        

         DepTransactionLL _ll2 = new DepTransactionLL(); 
        [Route("GetDepTrans")]
        [HttpPost]
        public List<td_def_trans_trf> GetDepTrans([FromBody] td_def_trans_trf tdt)
        {
           return _ll2.GetDepTrans(tdt);
        }
        [Route("InsertDepTrans")]
        [HttpPost]
        public int InsertDepTrans([FromBody] List<td_def_trans_trf> tdt)
        {
           return _ll2.InsertDepTrans(tdt);
        }
        
        [Route("UpdateDepTrans")]
        [HttpPost]
        public int UpdateDepTrans([FromBody] List<td_def_trans_trf> tdt)
        {
           return _ll2.UpdateDepTrans(tdt);
        }

        [Route("GetUnapprovedDepTrans")]
        [HttpPost]
        public List<td_def_trans_trf> GetUnapprovedDepTrans([FromBody] td_def_trans_trf tdt)
        {
           return _ll2.GetUnapprovedDepTrans(tdt);
        }
        

        [Route("GetapprovedDepTrans")]
        [HttpPost]
        public List<td_def_trans_trf> GetapprovedDepTrans([FromBody] td_def_trans_trf tdt)
        {
            return _ll2.GetapprovedDepTrans(tdt);
        }

        [Route("UpdateTransactionDetails")]
        [HttpPost]
        public int UpdateTransactionDetails([FromBody] LoanOpenDM tdt)
        {
           return _ll2.UpdateTransactionDetails(tdt);
        }


       

        

        TransferLL _ll3 = new TransferLL(); 
        [Route("GetTransferData")]
        [HttpPost]
        public TransferDM GetTransferData([FromBody] td_def_trans_trf tdt)
        {
           return _ll3.GetTransferData(tdt);
        }
        
        [Route("InsertTransferData")]
        [HttpPost]
        public string InsertTransferData([FromBody] TransferDM acc)
        {
           return _ll3.InsertTransferData(acc);
        }
       
        [Route("UpdateTransferData")]
        [HttpPost]
        public int UpdateTransferData([FromBody] TransferDM acc)
        {
           return _ll3.UpdateTransferData(acc);
        }
        
        [Route("DeleteTransferData")]
        [HttpPost]
        public int DeleteTransferData([FromBody] td_def_trans_trf acc)
        {
           return _ll3.DeleteTransferData(acc);
        }
         [Route("ApproveTransfer")]
        [HttpPost]
        public string ApproveTransfer([FromBody] p_gen_param acc)
        {
           return _ll3.ApproveTransfer(acc);
        }
        [Route("GetUnapproveTransfer")]
        [HttpPost]
        public  List<tm_transfer> GetUnapproveTransfer([FromBody] tm_transfer acc)
        {
           return _ll3.GetUnapproveTransfer(acc);
        }

        [Route("GetTransfer")]
        [HttpPost]
        public List<tm_transfer> GetTransfer([FromBody] tm_transfer tdt)
        {
           return _ll3.GetTransfer(tdt);
        }
        [Route("InsertTransfer")]
        [HttpPost]
        public int InsertTransfer([FromBody] List<tm_transfer> tdt)
        {
           return _ll3.InsertTransfer(tdt);
        }
        
        [Route("UpdateTransfer")]
        [HttpPost]
        public int UpdateTransfer([FromBody] List<tm_transfer> tdt)
        {
           return _ll3.UpdateTransfer(tdt);
        }

        [Route("DeleteTransfer")]
        [HttpPost]
        public int DeleteTransfer([FromBody] tm_transfer tdt)
        {
           return _ll3.DeleteTransfer(tdt);
        }
        
        [Route("GetDepTransTrfwithChild")]
        [HttpPost]
        public List<td_def_trans_trf> GetDepTransTrfwithChild([FromBody] td_def_trans_trf tdt)
        {
           return _ll3.GetDepTransTrfwithChild(tdt);
        }

        [Route("GetDashBoardInfo")]
        [HttpPost]
        public mm_dashboard GetDashBoardInfo([FromBody] p_gen_param td)
        {
           return _ll3.GetDashBoardInfo(td);
        }
        

    }
}
