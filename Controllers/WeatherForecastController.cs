using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Logger;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace SBWSFinanceApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        public WeatherForecastController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet("{id}", Name = "Get")]
        public string Get(string id)
        {
            JsonFileLogger logger;
            string toRtrn = "Did not Reach to Server";
            CheckHealth dac = new CheckHealth(Request);
            switch (id)
            {
                case "MySql":
                    toRtrn = dac.GetMySqlHealth();
                    break;
                case "Oracle":
                    toRtrn = dac.GetOracleHealth();
                    break;
                case "Config":
                    toRtrn = dac.GetAdminConfig();
                    break;
                case "Folder":
                    string dir = System.IO.Directory.GetCurrentDirectory();
                    if (null != dir)
                    {
                        toRtrn = "Current Folder is " + dir.Split('\\').LastOrDefault();
                    }
                    break;
                case "Err":
                    throw new ArgumentException("This is test error.");
                case "CreateLog":
                    // throw new ArgumentException("This is test error.");
                    logger = new JsonFileLogger();
                    logger.Log(new Log
                    {
                        ApiName = "Test Api",
                        Message = "This is a test Message to be instrted in the log"
                    });
                    toRtrn = "Logged success";
                    break;
                case "GetAllLog":
                    // throw new ArgumentException("This is test error.");
                    logger = new JsonFileLogger();

                    // logger.Log(new Log{
                    //         ApiName = "Test Api",
                    //         Message = "This is a test Message to be instrted in the log" 
                    //     });
                    toRtrn = "All Logs successfully recieved - " + JsonSerializer.Serialize(logger.GetLogs());
                    break;
                case "CN":
                    toRtrn = JsonSerializer.Serialize(dac.GetConfigNew());
                    break;
                default:
                    toRtrn = new DateTime().ToLongTimeString();
                    break;
            }
            return toRtrn;
        }
    }
}
