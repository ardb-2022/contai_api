using System;
using System.Collections.Generic;
using SBWSAdminApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Models;


namespace SBWSAdminApi.Admin
{
    public class AdminDL
    {
        string _statement;
        internal List<BankConfig> GetBankConfigDtls()
        {
            List<BankConfig> bankConfig = new List<BankConfig>();

            string _query = " SELECT BANK_CONFIG_ID, BANK_NAME, BANK_DESC,  "
                         + " SERVER_IP,DB_SERVER_IP, USER1, PASS1, USER2 , PASS2 , SMS_PROVIDER  "
                         + " FROM BANK_CONFIG "
                         + " WHERE DEL_FLAG = 'N' "
                         + " AND ACTIVE_FLAG = 'Y' ORDER  BY BANK_CONFIG_ID";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query);

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var rec = new BankConfig();

                                    rec.bank_config_id = UtilityM.CheckNull<decimal>(reader["BANK_CONFIG_ID"]);
                                    rec.bank_name = UtilityM.CheckNull<string>(reader["BANK_NAME"]);
                                    rec.bank_desc = UtilityM.CheckNull<string>(reader["BANK_DESC"]);
                                    rec.server_ip = UtilityM.CheckNull<string>(reader["SERVER_IP"]);
                                    rec.db_server_ip = UtilityM.CheckNull<string>(reader["DB_SERVER_IP"]);

                                    rec.user1 = UtilityM.CheckNull<string>(reader["USER1"]);
                                    rec.pass1 = UtilityM.CheckNull<string>(reader["PASS1"]);
                                    rec.user2 = UtilityM.CheckNull<string>(reader["USER2"]);
                                    rec.pass2 = UtilityM.CheckNull<string>(reader["PASS2"]);

                                    rec.sms_provider = UtilityM.CheckNull<string>(reader["SMS_PROVIDER"]);
                                    bankConfig.Add(rec);
                                }
                            }
                        }
                    }
                }
            }

            return bankConfig;
        }


        internal List<BankConfig> GetBankConfigDtlsNoPass()
        {
            List<BankConfig> bankConfig = new List<BankConfig>();

            string _query = " SELECT BANK_CONFIG_ID, BANK_NAME, BANK_DESC,  "
                         + " SERVER_IP,DB_SERVER_IP, SMS_PROVIDER  "
                         + " FROM BANK_CONFIG "
                         + " WHERE DEL_FLAG = 'N' "
                         + " AND ACTIVE_FLAG = 'Y' ORDER BY BANK_CONFIG_ID";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query);

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var rec = new BankConfig();

                                    rec.bank_config_id = UtilityM.CheckNull<decimal>(reader["BANK_CONFIG_ID"]);
                                    rec.bank_name = UtilityM.CheckNull<string>(reader["BANK_NAME"]);
                                    rec.bank_desc = UtilityM.CheckNull<string>(reader["BANK_DESC"]);
                                    rec.server_ip = UtilityM.CheckNull<string>(reader["SERVER_IP"]);
                                    rec.db_server_ip = UtilityM.CheckNull<string>(reader["DB_SERVER_IP"]);

                                    // rec.user1 = UtilityM.CheckNull<string>(reader["USER1"]);
                                    // rec.pass1 = UtilityM.CheckNull<string>(reader["PASS1"]);
                                    // rec.user2 = UtilityM.CheckNull<string>(reader["USER2"]);
                                    // rec.pass2 = UtilityM.CheckNull<string>(reader["PASS2"]);

                                    rec.sms_provider = UtilityM.CheckNull<string>(reader["SMS_PROVIDER"]);
                                    bankConfig.Add(rec);
                                }
                            }
                        }
                    }
                }
            }

            return bankConfig;
        }


        internal int InsertUpdateBankConfigDtls(BankConfig bc)
        {
            int ret = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, "INSERT_UPDATE_BANK_CONFIG"))
                        {

                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm = new OracleParameter("P_BANK_CONFIG_ID", OracleDbType.Int32, ParameterDirection.Input);
                            parm.Value = bc.bank_config_id;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_BANK_NAME", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.bank_name;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_BANK_DESC", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.bank_desc;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_SERVER_IP", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.server_ip;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_DB_SERVER_IP", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.db_server_ip;
                            command.Parameters.Add(parm);


                            parm = new OracleParameter("P_USER1", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.user1;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_USER1", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.pass1;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_USER1", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.user2;
                            command.Parameters.Add(parm);

                            parm = new OracleParameter("P_USER1", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.pass2;
                            command.Parameters.Add(parm);


                            parm = new OracleParameter("P_SMS_PROVIDER", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm.Value = bc.sms_provider;
                            command.Parameters.Add(parm);

                            command.ExecuteNonQuery();

                            transaction.Commit();
                            ret = 1;
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        ret = 0;
                    }
                }
            }

            return ret;
        }

        internal int InsertMenuConfig(List<MenuConfig> mc)
        {
            int ret = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        for (var i = 0; i < mc.Count; i++)
                        {
                            using (var command = OrclDbConnection.Command(connection, "INSERT_MENU_CONFIG"))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                var parm = new OracleParameter("P_BANK_CONFIG_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].bank_config_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_PARENT_MENU_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].parent_menu_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_MENU_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].menu_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_LEVEL_NO", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].level_no;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_MENU_NAME", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].menu_name;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_REF_PAGE", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].ref_page;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_IS_SCREEN", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].is_screen;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_ACTIVE_FLAG", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].active_flag;
                                command.Parameters.Add(parm);

                                command.ExecuteNonQuery();

                            }
                        }
                        transaction.Commit();
                        ret = 1;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        ret = 0;
                    }
                }
            }

            return ret;
        }

        internal int UpdateMenuConfig(List<MenuConfig> mc)
        {
            int ret = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        for (var i = 0; i < mc.Count; i++)
                        {
                            using (var command = OrclDbConnection.Command(connection, "UPDATE_MENU_CONFIG"))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                var parm = new OracleParameter("P_BANK_CONFIG_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].bank_config_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_PARENT_MENU_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].parent_menu_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_MENU_ID", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].menu_id;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_LEVEL_NO", OracleDbType.Int32, ParameterDirection.Input);
                                parm.Value = mc[i].level_no;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_MENU_NAME", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].menu_name;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_REF_PAGE", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].ref_page;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_IS_SCREEN", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].is_screen;
                                command.Parameters.Add(parm);

                                parm = new OracleParameter("P_ACTIVE_FLAG", OracleDbType.Varchar2, ParameterDirection.Input);
                                parm.Value = mc[i].active_flag;
                                command.Parameters.Add(parm);

                                command.ExecuteNonQuery();

                            }
                        }

                        transaction.Commit();
                        ret = 1;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        ret = 0;
                    }
                }
            }

            return ret;
        }

        internal decimal GetMenuId()
        {
            List<MenuConfig> menuConfigList = new List<MenuConfig>();
            decimal seq = 0;
            string _query = " SELECT TO_NUMBER(TO_CHAR(SYSDATE , 'YYYYMMDDHH24MISS')) SEQ FROM DUAL ";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query);

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    seq = UtilityM.CheckNull<decimal>(reader["SEQ"]);
                                }
                            }
                        }
                    }
                }
            }

            return seq;
        }

        internal List<MenuConfig> GetMenuConfig(MenuConfig mc)
        {
            List<MenuConfig> menuConfigList = new List<MenuConfig>();

            string _query = " SELECT BANK_CONFIG_ID, PARENT_MENU_ID, MENU_ID, LEVEL_NO , "
                         + " MENU_NAME, REF_PAGE, IS_SCREEN, ACTIVE_FLAG  "
                         + " FROM MENU_CONFIG "
                         + " WHERE DEL_FLAG = 'N' "
                         + " AND BANK_CONFIG_ID = {0} ORDER BY MENU_ID";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query, mc.bank_config_id);

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var rec = new MenuConfig();

                                    rec.bank_config_id = UtilityM.CheckNull<decimal>(reader["BANK_CONFIG_ID"]);
                                    rec.parent_menu_id = UtilityM.CheckNull<decimal>(reader["PARENT_MENU_ID"]);
                                    rec.menu_id = UtilityM.CheckNull<decimal>(reader["MENU_ID"]);
                                    rec.level_no = UtilityM.CheckNull<decimal>(reader["LEVEL_NO"]);
                                    rec.menu_name = UtilityM.CheckNull<string>(reader["MENU_NAME"]);
                                    rec.ref_page = UtilityM.CheckNull<string>(reader["REF_PAGE"]);
                                    rec.is_screen = UtilityM.CheckNull<string>(reader["IS_SCREEN"]);
                                    rec.active_flag = UtilityM.CheckNull<string>(reader["ACTIVE_FLAG"]);

                                    menuConfigList.Add(rec);
                                }
                            }
                        }
                    }
                }
            }

            return menuConfigList;
        }



        internal int InsertUserLogin(UserLogin ul)
        {
            string _query = "INSERT INTO USER_LOGIN ( LOGIN_ID, PASSWORD, CUST_CD, BANK_CONFIG_ID, ACCESS_TYPE, ACTIVE_FLAG, DEL_FLAG,CREATED_BY,CREATED_DT,UPDATED_BY,UPDATED_DT, MPIN) "
                        + " VALUES( {0},{1},{2},{3}, {4}, {5}, {6}, USER, SYSDATE, USER, SYSDATE, {7} ) ";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", ul.login_id, "'"),
                                                   string.Concat("'", ul.password, "'"),
                                                   ul.cust_cd,
                                                   ul.bank_config_id,
                                                   string.Concat("'", ul.access_type, "'"),
                                                    string.Concat("'", ul.active_flag, "'"),
                                                    string.Concat("'", ul.del_flag, "'"),
                                                   string.Concat("'", ul.MPIN, "'")
                                                    );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                        }

                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }
                }
            }

            return 0;
        }


        internal int UpdateUserLogin(UserLogin ul)
        {
            string _query = "UPDATE USER_LOGIN "
                        + " SET PASSWORD = {0}, CUST_CD = {1}, BANK_CONFIG_ID = {2}, "
                        + " ACCESS_TYPE = {3}, ACTIVE_FLAG = {4}, DEL_FLAG = {5} ,   "
                        + " MPIN = {7} ,   "
                        + " UPDATED_BY = USER , UPDATED_DT=SYSDATE                  "
                        + " WHERE LOGIN_ID = {6} ";

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   !string.IsNullOrWhiteSpace(ul.password) ? string.Concat("'", ul.password, "'") : "PASSWORD",
                                                   ul.cust_cd > 0 ? ul.cust_cd.ToString() : "CUST_CD",
                                                   ul.bank_config_id > 0 ? ul.bank_config_id.ToString() : "BANK_CONFIG_ID",
                                                   !string.IsNullOrWhiteSpace(ul.access_type) ? string.Concat("'", ul.access_type, "'") : "ACCESS_TYPE",
                                                   !string.IsNullOrWhiteSpace(ul.active_flag) ? string.Concat("'", ul.active_flag, "'") : "ACTIVE_FLAG",
                                                   !string.IsNullOrWhiteSpace(ul.del_flag) ? string.Concat("'", ul.del_flag, "'") : "DEL_FLAG",
                                                   string.Concat("'", ul.login_id, "'"),
                                                   string.Concat("'", ul.MPIN, "'")
                                                    );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            return 0;
                        }

                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }
                }
            }

            return 0;
        }

        internal UserLoginStat GetUserLoginStat(UserLoginStat loginStat)
        {
            string _query = " SELECT ACTIVE_FLAG , DEL_FLAG "
                        + " FROM USER_LOGIN "
                        + " WHERE LOGIN_ID = '{0}' ";

            UserLoginStat userLoginStat = new UserLoginStat();
            userLoginStat.login_id = loginStat.login_id;
            userLoginStat.is_found = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query, loginStat.login_id);

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    {
                                        while (reader.Read())
                                        {
                                            userLoginStat.active_flag = UtilityM.CheckNull<string>(reader["ACTIVE_FLAG"]);
                                            userLoginStat.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                            userLoginStat.is_found = 1;
                                        }
                                    }
                                }
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        userLoginStat.is_found = -1;
                        return userLoginStat;
                    }
                }
            }
            return userLoginStat;
        }

        internal UserLoginStat GetUserLoginValidate(UserLoginStat loginStat)
        {
            string _query = " SELECT A.CUST_CD CUST_CD,B.BANK_CONFIG_ID BANK_CONFIG_ID, "
                        + " B.BANK_NAME BANK_NAME,B.SMS_PROVIDER SMS_PROVIDER,B.BANK_DESC BANK_DESC "
                        + " FROM USER_LOGIN A,BANK_CONFIG B "
                        + " WHERE A.LOGIN_ID = '{0}' "
                        + "  AND   A.PASSWORD = '{1}'"
                        + "  AND   A.BANK_CONFIG_ID=B.BANK_CONFIG_ID "
                        + "  AND A.DEL_FLAG='N'"
                        + "  AND  A.ACTIVE_FLAG='Y' ";

            UserLoginStat userLoginStat = new UserLoginStat();
            userLoginStat.login_id = loginStat.login_id;
            // userLoginStat.password = loginStat.password;
            userLoginStat.is_found = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query,
                                          loginStat.login_id,
                                          loginStat.password);

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    {
                                        while (reader.Read())
                                        {
                                            userLoginStat.bank_config_id = UtilityM.CheckNull<decimal>(reader["BANK_CONFIG_ID"]);
                                            userLoginStat.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                            userLoginStat.bank_name = UtilityM.CheckNull<string>(reader["BANK_NAME"]);
                                            userLoginStat.sms_provider = UtilityM.CheckNull<string>(reader["SMS_PROVIDER"]);
                                            userLoginStat.bank_desc = UtilityM.CheckNull<string>(reader["BANK_DESC"]);

                                            userLoginStat.is_found = 1;
                                        }
                                    }
                                }
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        userLoginStat.is_found = -1;
                        return userLoginStat;
                    }
                }
            }
            return userLoginStat;
        }


        internal UserLoginStat ValidateUserWithPhn(UserLoginStat loginStat)
        {
            string _query = " SELECT A.CUST_CD CUST_CD, A.PASSWORD PASSWORD, A.MPIN MPIN,"
                        + "B.BANK_CONFIG_ID BANK_CONFIG_ID, B.SERVER_IP SERVER_IP, "
                        + " B.BANK_NAME BANK_NAME,B.BANK_DESC BANK_DESC,B.SMS_PROVIDER SMS_PROVIDER "
                        + " FROM USER_LOGIN A,BANK_CONFIG B "
                        + " WHERE A.LOGIN_ID = '{0}' "
                        + "  AND   A.BANK_CONFIG_ID=B.BANK_CONFIG_ID "
                        + "  AND A.DEL_FLAG='N'"
                        + "  AND  A.ACTIVE_FLAG='Y' ";

            UserLoginStat userLoginStat = new UserLoginStat();
            userLoginStat.login_id = loginStat.login_id;
            // userLoginStat.password = loginStat.password;
            userLoginStat.is_found = 0;

            using (var connection = OrclDbConnection.NewConnectionAdmin)
            {
                _statement = string.Format(_query,
                                          loginStat.login_id);

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    {
                                        while (reader.Read())
                                        {
                                            userLoginStat.bank_config_id = UtilityM.CheckNull<decimal>(reader["BANK_CONFIG_ID"]);
                                            userLoginStat.server_ip = UtilityM.CheckNull<string>(reader["SERVER_IP"]);
                                            userLoginStat.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                            userLoginStat.bank_name = UtilityM.CheckNull<string>(reader["BANK_NAME"]);
                                            userLoginStat.bank_desc = UtilityM.CheckNull<string>(reader["BANK_DESC"]);
                                            userLoginStat.sms_provider = UtilityM.CheckNull<string>(reader["SMS_PROVIDER"]);
                                            userLoginStat.password = UtilityM.CheckNull<string>(reader["PASSWORD"]);
                                            userLoginStat.MPIN = UtilityM.CheckNull<string>(reader["MPIN"]);
                                            userLoginStat.is_found = 1;
                                        }
                                    }
                                }
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        userLoginStat.is_found = -1;
                        return userLoginStat;
                    }
                }
            }
            return userLoginStat;
        }
    }
}
