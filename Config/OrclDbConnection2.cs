
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using SBWSAdminApi.Models;

namespace SBWSFinanceApi.Config
{
    internal static class OrclDbConnection2
    {
        static HttpRequest __req;
        public static void Init(HttpRequest req)
        {
            __req = req;
        }
        // static IConfiguration _config = new ConfigurationBuilder()
        //         .AddJsonFile(getRootPath("appSettings.json"))
        //         .Build();
        static IConfiguration _config = new ConfigurationBuilder()
                .SetBasePath(System.AppContext.BaseDirectory + @"\RPT\Constant\")
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();
        // static string getRootPath(string rootFilename)
        // {
        //     string _root;
        //     var rootDir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase);
        //     Regex matchThepath = new Regex(@"(?<!fil)[A-Za-z]:\\+[\S\s]*?(?=\\+bin)");
        //     var appRoot = matchThepath.Match(rootDir).Value;
        //     _root = Path.Combine(appRoot, rootFilename);

        //     return _root;
        // }

        public static BankConfig getConfiguration()
        {
            // string toRtrn;
            // if (__req.Headers.TryGetValue("bname", out var some))
            // {
            //     toRtrn = __req.Headers["bname"];
            // }
            // else
            // {
            //     toRtrn = some.ToString();
            // }
            return _config.GetSection("DbConnections").Get<BankConfig>();

            //return conns;
        }
    }
    // public static class HttpRequestExtension
    // {
    //     public static string GetHeader(this HttpRequest request, string key)
    //     {
    //         return request.Headers.FirstOrDefault(x => x.Key == key).Value.FirstOrDefault();
    //     }
    // }

}