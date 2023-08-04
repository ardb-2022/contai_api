using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Controllers
{
    [Route("api/BankConfigMst")] 
    [ApiController]
    [EnableCors("AllowOrigin")] 
    public class BankConfigMstController : ControllerBase
    {
        // GET: api/MyDetail
        [HttpGet]
        public BankConfigMst Get()
        {
            // return new string[] { "value1", "value2" }; some more words here
            return new BankConfigMstLL().ReadAllConfiguration();
            // dont know if this is really required
            // is this really required
        }

        // GET: api/MyDetail/5
        [HttpGet("{bankName}", Name = "GetDetail")]
        public string Get(string bankName)
        {
            return bankName;
        }

        // POST: api/MyDetail
        [HttpPost]
        public void Post([FromBody] BankConfigMst bankConfigMst)
        {
            new BankConfigMstLL().InsertUpdateBankConfig(bankConfigMst);
        }

        // PUT: api/MyDetail/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
