using System;
using System.Collections.Generic;
using SBWSAdminApi.Models;
using SBWSAdminApi.Admin;
using System.Linq;

namespace SBWSAdminApi.LL
{
    public class AdminLL
    {
        AdminDL _dac = new AdminDL();
        internal List<BankConfig> GetBankConfigDtls()
        {
            return _dac.GetBankConfigDtls();
        }

        

        internal List<BankConfig> GetBankConfigDtlsNoPass()
        {
            return _dac.GetBankConfigDtlsNoPass();
        }

        internal int InsertUpdateBankConfigDtls(BankConfig bc)
        {
            return _dac.InsertUpdateBankConfigDtls(bc);
        }

        internal int InsertMenuConfig(List<MenuConfig> mc)
        {
            return _dac.InsertMenuConfig(mc);
        }

        internal int UpdateMenuConfig(List<MenuConfig> mc)
        {
            return _dac.UpdateMenuConfig(mc);
        }



        internal decimal GetMenuId()
        {
            return _dac.GetMenuId();
        }

        internal List<MenuConfig> GetMenuConfig(MenuConfig mc)
        {
            return _dac.GetMenuConfig(mc);
        }

        internal List<MenuConfig> GetMenu(MenuConfig mc)
        {
            List<MenuConfig> menuConfigs = _dac.GetMenuConfig(mc);
            // iterate over menuconfig and create tree of menuconfig
            List<MenuConfig> filterMenuforFirstLvl = menuConfigs.Where(e => e.level_no == 1).ToList();
            foreach (var firstLvlMenu in filterMenuforFirstLvl)
            {
                firstLvlMenu.ChildMenuConfigs.AddRange(
                menuConfigs.Where(e => e.parent_menu_id == firstLvlMenu.menu_id).ToList());

                if (null != firstLvlMenu.ChildMenuConfigs &&
                firstLvlMenu.ChildMenuConfigs.Any() &&
                firstLvlMenu.ChildMenuConfigs.Count > 0)
                {
                    foreach (var secondLvlMenu in firstLvlMenu.ChildMenuConfigs)
                    {
                        secondLvlMenu.ChildMenuConfigs.AddRange(
                        menuConfigs.Where(e => e.parent_menu_id == secondLvlMenu.menu_id).ToList());

                        if (null != secondLvlMenu.ChildMenuConfigs &&
                                        secondLvlMenu.ChildMenuConfigs.Any() &&
                                        secondLvlMenu.ChildMenuConfigs.Count > 0)
                        {
                            foreach (var thirdLvlMenu in secondLvlMenu.ChildMenuConfigs)
                            {
                                thirdLvlMenu.ChildMenuConfigs.AddRange(
                                menuConfigs.Where(e => e.parent_menu_id == thirdLvlMenu.menu_id).
                                ToList());

                                // if (null != thirdLvlMenu.ChildMenuConfigs &&
                                //         thirdLvlMenu.ChildMenuConfigs.Any() &&
                                //         thirdLvlMenu.ChildMenuConfigs.Count > 0)
                                // {
                                //     foreach (var forthLvlMenu in thirdLvlMenu.ChildMenuConfigs)
                                //     {
                                //         forthLvlMenu.ChildMenuConfigs.AddRange(
                                //         menuConfigs.Where(e => e.parent_menu_id == thirdLvlMenu.menu_id).
                                //         ToList());
                                //     }
                                // }
                            }
                        }
                    }
                }
            }

            return filterMenuforFirstLvl;
        }

        // private List<MenuConfig> CreateTreeMenu(List<MenuConfig> frstLvls,
        // List<MenuConfig> menuConfigs)
        // {
        //     foreach (var firstLvlMenu in frstLvls)
        //     {
        //         firstLvlMenu.ChildMenuConfigs =
        //         menuConfigs.Where(e => e.parent_menu_id == firstLvlMenu.menu_id).ToList();

        //         if (null != firstLvlMenu.ChildMenuConfigs &&
        //         firstLvlMenu.ChildMenuConfigs.Any() &&
        //         firstLvlMenu.ChildMenuConfigs.Count > 0)
        //         {
        //             firstLvlMenu.ChildMenuConfigs = 
        //             CreateTreeMenu(firstLvlMenu.ChildMenuConfigs, menuConfigs);
        //         }
        //     }

        //     return frstLvls;
        // }

        internal int InsertUserLogin(UserLogin ul)
        {
            return _dac.InsertUserLogin(ul);
        }

        internal int UpdateUserLogin(UserLogin ul)
        {
            return _dac.UpdateUserLogin(ul);
        }

        internal UserLoginStat GetUserLoginStat(UserLoginStat loginStat)
        {
            return _dac.GetUserLoginStat(loginStat);
        }
        internal UserLoginStat GetUserLoginValidate(UserLoginStat loginStat)
        {
            return _dac.GetUserLoginValidate(loginStat);
        }

        internal UserLoginStat ValidateUserWithPhn(UserLoginStat loginStat)
        {
            return _dac.ValidateUserWithPhn(loginStat);
        }

        internal bool generateOTPAndSMS(SMSDtls smsDtls)
        {
            bool toReturn = false;
            var banConfigs = _dac.GetBankConfigDtls();
            if (null != banConfigs && banConfigs.Any())
            {
                var banConfig = banConfigs.Where(e => e.bank_config_id == smsDtls.bankConfigId).FirstOrDefault();

                if (null != banConfig && banConfig.bank_config_id > 0)
                {
                    // var key = "Thu9srzzpf3"; // DEV - WvfhAd3p0is, PROD - Thu9srzzpf3
                    using (System.Net.Http.HttpClient cl = new System.Net.Http.HttpClient())
                    {
                        string url = banConfig.sms_provider;
                        // string url = "http://bulksms.sssplsales.in/api/api_http.php?username="
                        // + "SNSMBK&password=SN524SMBK&senderid=SNSMBK&to={0}&text=Your Savings "
                        // + "Deposit Account Number:*******{1}.OTP is {2} is CREDITED by "
                        // + "Rs.6000.Balance is Rs.41477 -{3}- "
                        // + ".SAKTINAGARSKUSLTD&route=Informative&type=text";

                        string apiUrl = string.Format(url, smsDtls.Mobile, smsDtls.OTP, smsDtls.Key);
                        System.Net.Http.HttpResponseMessage response = cl.GetAsync(apiUrl).Result;
                        if (response.IsSuccessStatusCode)
                        {
                            var result = response.Content.ReadAsStringAsync();
                            toReturn = true;
                        }
                    }
                }

            }
            return toReturn;
        }

    }
}