using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSFinanceApi.DL;
using Microsoft.AspNetCore.Cors;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;
using SBWSDepositApi.Models;
using System.Data.Common;

namespace SBWSFinanceApi.Controllers
{
    [EnableCors("AllowOrigin")]
    [Route("api/UCIC")]
    [ApiController]


    public class UCICController : ControllerBase
    {


        [Route("ReadKycSig")]
        [HttpPost]
        public kyc_sig ReadKycSig([FromBody] kyc_sig ks)
        {
            KycSigLL _llKyc = new KycSigLL();
            return _llKyc.ReadKycSig(ks);
        }

        [Route("WriteKycSig")]
        [HttpPost]
        public kyc_sig WriteKycSig([FromBody] kyc_sig ks)
        {
            KycSigLL _llKyc = new KycSigLL();
            return _llKyc.WriteKycSig(ks);
        }
        CustomerLL _ll = new CustomerLL();
        [Route("GetCustomerDtls")]
        [HttpPost]
        public List<mm_customer> GetCustomerDtls([FromBody] mm_customer pmc)
        {
            return _ll.GetCustomerDtls(pmc);
        }
        
        [Route("GetMemberDtls")]
        [HttpPost]
        public List<mm_customer> GetMemberDtls([FromBody] mm_customer pmc)
        {
            return _ll.GetMemberDtls(pmc);
        }

        [Route("InsertCustomerDtls")]
        [HttpPost]
        public decimal InsertCustomerDtls([FromBody] mm_customer pmc)
        {
            return _ll.InsertCustomerDtls(pmc);
        }

        [Route("UpdateCustomerDtls")]
        [HttpPost]
        public int UpdateCustomerDtls([FromBody] mm_customer pmc)
        {
            return _ll.UpdateCustomerDtls(pmc);
        }

        [Route("DeleteCustomerDtls")]
        [HttpPost]
        public int DeleteCustomerDtls([FromBody] mm_customer pmc)
        {
            return _ll.DeleteCustomerDtls(pmc);
        }
        [Route("GetDepositDtls")]
        [HttpPost]
        public List<tm_deposit> GetDepositDtls([FromBody] mm_customer pmc)
        {
            return _ll.GetDepositDtls(pmc);
        }
        
        [Route("GetLoanDtls")]
        [HttpPost]
        public List<tm_loan_all> GetLoanDtls([FromBody] mm_customer pmc)
        {
            return _ll.GetLoanDtls(pmc);
        }

        ShgLL _ll1 = new ShgLL();
        [Route("GetShgData")]
        [HttpPost]
        public ShgDM GetShgData([FromBody] ShgDM pmc)
        {

            return _ll1.GetShgData(pmc);
        }


        [Route("InsertShgData")]
        [HttpPost]
        public string InsertShgData([FromBody] ShgDM pmc)
        {
            return _ll1.InsertShgData(pmc);
        }

        [Route("UpdateShgData")]
        [HttpPost]
        public int UpdateShgData([FromBody] ShgDM pmc)
        {
            return _ll1.UpdateShgData(pmc);
        }

        [Route("DeleteShgData")]
        [HttpPost]
        public int DeleteShgData([FromBody] ShgDM pmc)
        {
            return _ll1.DeleteShgData(pmc);
        }

        [Route("Checkpancard")]
        [HttpPost]
        public int Checkpancard([FromBody] mm_customer pmc)
        {
            return _ll.Checkpancard(pmc);
        }


        [Route("Checkaadharcard")]
        [HttpPost]
        public int Checkaadharcard([FromBody] mm_customer pmc)
        {
            return _ll.Checkaadharcard(pmc);
        }



    }
}



