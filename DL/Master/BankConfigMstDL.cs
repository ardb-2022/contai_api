using System;
using System.Text.Json;
using System.Collections.Generic;
using System.IO;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.DL
{
    internal class BankConfigMstDL
    {
        private string path
        {
            get
            {
                return Directory.GetCurrentDirectory() + @"\RPT\Constant\BranchConfig.json";
                
            }
        }
        // private string path = @"D:\POC\DreamBig\SSS\Banking\SBWS\SBWSFinanceApi\Constant\BranchConfig.json";
        internal BankConfigMst ReadAllConfiguration()
        {
            BankConfigMst bankConfig = new BankConfigMst();

            if (File.Exists(path))
            {
                // var fileContent = File.ReadAllText(Path.Combine(env.ContentRootPath, "ClientApp", "package.json"));
                var fileContent = File.ReadAllText(path);
                bankConfig = JsonSerializer.Deserialize<BankConfigMst>(fileContent);
            }

            return bankConfig;
        }

        internal void InsertUpdateBankConfig(BankConfigMst bankConfigMst)
        {
            // var config = ReadAllConfiguration();
            var serializedContent = JsonSerializer.Serialize(bankConfigMst);
            try
            {
                System.IO.File.WriteAllText(path, serializedContent);
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}