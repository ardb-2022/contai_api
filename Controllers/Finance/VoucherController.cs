using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.DL;
using Microsoft.AspNetCore.Cors;
using SBWSFinanceApi.LL;

namespace SBWSFinanceApi.Controllers
{
    [Route("api/Voucher")]
    [ApiController]  
    [EnableCors("AllowOrigin")] 
    public class VoucherController : ControllerBase
    {   
        VoucherDL _ll = new VoucherDL(); 
        [Route("GetTVoucherDtls")]
        [HttpPost]
        public List<t_voucher_dtls> GetTVoucherDtls([FromBody] t_voucher_dtls tvd)
        {
           return _ll.GetTVoucherDtls(tvd);
        }
        [Route("GetTVoucherNarration")]
        [HttpPost]
        public List<t_voucher_dtls> GetTVoucherNarration([FromBody] t_voucher_dtls tvd)
        {
           return _ll.GetTVoucherNarration(tvd);
        }
        [Route("InsertTVoucherDtls")]
        [HttpPost]
        public int InsertTVoucherDtls([FromBody] List<t_voucher_dtls> tvd)
        {
           return _ll.InsertTVoucherDtls(tvd);
        }
        
        [Route("UpdateTVoucherDtls")]
        [HttpPost]
        public int UpdateTVoucherDtls([FromBody] List<t_voucher_dtls> tvd)
        {
           return _ll.UpdateTVoucherDtls(tvd);
        }
       VoucherLL _ll1 = new VoucherLL(); 
       [Route("GetTVoucherDtlsForPrint")]
        [HttpPost]
        public List<t_voucher_narration> GetTVoucherDtlsForPrint([FromBody] t_voucher_dtls tvd)
        {
           return _ll1.GetTVoucherDtlsForPrint(tvd);
        }

        [Route("DeleteInsertVoucherDtls")]
        [HttpPost]
        public int DeleteInsertVoucherDtls([FromBody]  List<t_voucher_dtls> tvd)
        {
           return _ll1.DeleteInsertVoucherDtls(tvd);
        }
        [Route("DeleteVoucherDtls")]
        [HttpPost]
        public int DeleteVoucherDtls([FromBody]  List<t_voucher_dtls> tvd)
        {
           return _ll1.DeleteVoucherDtls(tvd);
        }

         
    }
}
