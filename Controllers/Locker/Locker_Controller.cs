using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Controllers
{
    [Route("api/Locker")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class Locker_Controller
    {
        LockerOpenLL _ll = new LockerOpenLL();

        [Route("GetLockerMaster")]
        [HttpPost]
        public List<mm_locker> GetLockerMaster([FromBody] p_report_param tvd)
        {
            return _ll.GetLockerMaster(tvd);
        }

        [Route("InsertLockerMaster")]
        [HttpPost]
        public int InsertLockerMaster([FromBody] List<mm_locker> tvd)
        {
            return _ll.InsertLockerMaster(tvd);
        }


        [Route("GetLockerOpeningData")]
        [HttpPost]
        public LockerOpenDM GetLockerOpeningData([FromBody] tm_locker tvd)
        {
            return _ll.GetLockerOpeningData(tvd);
        }


        [Route("GetLockerOpeningDataView")]
        [HttpPost]
        public LockerOpenDM GetLockerOpeningDataView([FromBody] tm_locker tvd)
        {
            return _ll.GetLockerOpeningDataView(tvd);
        }


        [Route("InsertLockerOpeningData")]
        [HttpPost]
        public string InsertLockerOpeningData([FromBody] LockerOpenDM tvd)
        {
            return _ll.InsertLockerOpeningData(tvd);
        }

        

        [Route("GetUnapprovedLockerTrans")]
        [HttpPost]
        public List<td_def_trans_trf> GetUnapprovedLockerTrans([FromBody] tm_locker tvd)
        {
            return _ll.GetUnapprovedLockerTrans(tvd);
        }


        [Route("UpdateLockerOpeningData")]
        [HttpPost]
        public int UpdateLockerOpeningData([FromBody] LockerOpenDM tvd)
        {
            return _ll.UpdateLockerOpeningData(tvd);
        }


        [Route("DeleteLockerOpeningData")]
        [HttpPost]
        public int DeleteLockerOpeningData([FromBody] td_def_trans_trf tvd)
        {
            return _ll.DeleteLockerOpeningData(tvd);
        }

        [Route("ApproveLockerTranaction")]
        [HttpPost]
        public string ApproveLockerTranaction([FromBody] p_gen_param tvd)
        {
            return _ll.ApproveLockerTranaction(tvd);
        }



        [Route("PopulateLockerDtlsRep")]
        [HttpPost]
        public List<locker_dtls_report> PopulateLockerDtlsRep([FromBody] p_report_param tvd)
        {
            return _ll.PopulateLockerDtlsRep(tvd);
        }
        
        [Route("PopulateLockerHistoryRep")]
        [HttpPost]
        public List<tm_locker> PopulateLockerHistoryRep([FromBody] p_report_param tvd)
        {
            return _ll.PopulateLockerHistoryRep(tvd);
        }

        [Route("PopulateLockerShouldBeRenewedRep")]
        [HttpPost]
        public List<tm_locker> PopulateLockerShouldBeRenewedRep([FromBody] p_report_param tvd)
        {
            return _ll.PopulateLockerShouldBeRenewedRep(tvd);
        }

        [Route("GetLockerRent")]
        [HttpPost]
        public List<locker_rent> GetLockerRent()
        {
            return _ll.GetLockerRent();
        }
        
        [Route("GetLockerRentlist")]
        [HttpPost]
        public List<locker_rent> GetLockerRentlist()
        {
            return _ll.GetLockerRentlist();
        }

        [Route("GetLockerDtls")]
        [HttpPost]
        public tm_locker GetLockerDtls([FromBody] tm_locker tvd)
        {
            return _ll.GetLockerDtls(tvd);
        }
        
        [Route("GetLockerDtlsView")]
        [HttpPost]
        public tm_locker GetLockerDtlsView([FromBody] tm_locker tvd)
        {
            return _ll.GetLockerDtlsView(tvd);
        }

        [Route("InsertLockerRentMaster")]
        [HttpPost]
        public int InsertLockerRentMaster([FromBody] List<locker_rent> tvd)
        {
            return _ll.InsertLockerRentMaster(tvd);
        }

    }
}
