using System;
using System.Text.Json;
using System.Collections.Generic;
using System.IO;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.DL
{
    internal sealed class BankConfigUxDL
    {
        private string pathMstr
        {
            get
            {                
                return Directory.GetCurrentDirectory() + @"\RPT\Constant\BankConfig.json";
            }
        }

        private string pathUx
        {
            get
            {                
                return @"C:\wwwroot\Ux\SSS\assets\constants\BankConfig.json";
            }
        }
       
        internal List<BankConfiguration> ReadBankConfigUx()
        {
            List<BankConfiguration> bankConfig = new List<BankConfiguration>();

            if (File.Exists(pathMstr))
            {
                // var fileContent = File.ReadAllText(Path.Combine(env.ContentRootPath, "ClientApp", "package.json"));
                var fileContent = File.ReadAllText(pathMstr);
                bankConfig = JsonSerializer.Deserialize<List<BankConfiguration>>(fileContent);
            }

            return bankConfig;
        }

        internal void WriteBankConfigUx(List<BankConfiguration> bankConfig)
        {
            var serializedContent = JsonSerializer.Serialize(bankConfig);
            try
            {
                System.IO.File.WriteAllText(pathMstr, serializedContent);
            }
            catch (Exception e)
            {
                throw e;
            }

          try
            {
                System.IO.File.WriteAllText(pathUx, serializedContent);
            }
            catch (Exception e)
            {
                throw e;
            }

        }

    }
}