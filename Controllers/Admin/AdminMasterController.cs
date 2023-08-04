using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Cors;
using SBWSAdminApi.LL;
using SBWSAdminApi.Models;

namespace SBWSFinanceApi.Controllers
{
    [EnableCors("AllowOrigin")]
    [Route("api/Admin")]
    [ApiController]

    public class AdminMasterController : ControllerBase
    {

        AdminLL _ll = new AdminLL();

        [Route("GetBankConfigDtls")]
        [HttpPost]
        public List<BankConfig> GetBankConfigDtls()
        {
            return _ll.GetBankConfigDtls();
        }

        

        [Route("GetBankConfigDtlsNoPass")]
        [HttpPost]
        public List<BankConfig> GetBankConfigDtlsNoPass()
        {
            return _ll.GetBankConfigDtlsNoPass();
        }

        [Route("InsertUpdateBankConfigDtls")]
        [HttpPost]
        public int InsertUpdateBankConfigDtls(BankConfig bc)
        {
            return _ll.InsertUpdateBankConfigDtls(bc);
        }

        [Route("InsertMenuConfig")]
        [HttpPost]
        public int InsertMenuConfig(List<MenuConfig> mc)
        {
            return _ll.InsertMenuConfig(mc);
        }

        [Route("UpdateMenuConfig")]
        [HttpPost]
        public int UpdateMenuConfig(List<MenuConfig> mc)
        {
            return _ll.UpdateMenuConfig(mc);
        }


        [Route("GetMenuId")]
        [HttpPost]
        public decimal GetMenuId()
        {
            return _ll.GetMenuId();
        }

        [Route("GetMenuConfig")]
        [HttpPost]
        public List<MenuConfig> GetMenuConfig(MenuConfig mc)
        {
            return _ll.GetMenuConfig(mc);
        }

        [Route("GetMenu")]
        [HttpPost]
        public List<MenuConfig> GetMenu(MenuConfig mc)
        {
            return _ll.GetMenu(mc);
        }

        [Route("InsertUserLogin")]
        [HttpPost]
        public int InsertUserLogin(UserLogin ul)
        {
            return _ll.InsertUserLogin(ul);
        }

        [Route("UpdateUserLogin")]
        [HttpPost]
        public int UpdateUserLogin(UserLogin ul)
        {
            return _ll.UpdateUserLogin(ul);
        }


        [Route("GetUserLoginStat")]
        [HttpPost]
        public UserLoginStat GetUserLoginStat(UserLoginStat loginStat)
        {
            return _ll.GetUserLoginStat(loginStat);
        }


        [Route("GetUserLoginValidate")]
        [HttpPost]
        public UserLoginStat GetUserLoginValidate(UserLoginStat loginStat)
        {
            return _ll.GetUserLoginValidate(loginStat);
        }

        [Route("ValidateUserWithPhn")]
        [HttpPost]
        public UserLoginStat ValidateUserWithPhn(UserLoginStat loginStat)
        {
            return _ll.ValidateUserWithPhn(loginStat);
        }

        [Route("generateOTPAndSMS")]
        [HttpPost]
        public bool generateOTPAndSMS(SMSDtls smsDtls)
        {
            return _ll.generateOTPAndSMS(smsDtls);
        }
        
        

    }
}