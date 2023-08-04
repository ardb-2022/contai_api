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
    [Route("api/BankConfigUx")]
    [ApiController]
    [EnableCors("AllowOrigin")] 
    public class BankConfigUxController : ControllerBase
    {
       // GET: api/BankConfigUx
        [HttpGet]
        public List<BankConfiguration> Get()
        {
            var obj=  new BankConfigUxLL();
            return obj.ReadBankConfigUx();
        }

        // GET: api/BankConfigUx/5
        // [HttpGet("{id}", Name = "Get")]
        // public string Get(int id)
        // {
        //     return "value";
        // }

        // POST: api/BankConfigUx
        [HttpPost]
       public void Post([FromBody] List<BankConfiguration> bankConfig)
        {
            var obj=  new BankConfigUxLL();
            obj.WriteBankConfigUx(bankConfig);
        }


        // PUT: api/BankConfigUx/5
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
