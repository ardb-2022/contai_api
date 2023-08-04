using System;
using System.Data.Common;
using System.Linq;
using Oracle.ManagedDataAccess.Client;
using SBWSAdminApi.Models;
using SBWSFinanceApi.Utility;
using Microsoft.Extensions.Configuration;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Config
{
    internal static class OrclDbConnection
    {

        static IConfiguration _config = new ConfigurationBuilder()
                        .SetBasePath(System.AppContext.BaseDirectory + @"\RPT\Constant\")
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .Build();
        public static DbCommand Command(DbConnection connection, string cmdText)
        {
            return new OracleCommand(cmdText, (OracleConnection)connection);
        }

        public static DbConnection NewConnection
        {
            get
            {
                //BankConfigMst BC = new BankConfigMstLL().ReadAllConfiguration();
                //BankConfig bc = getBankConfigFromDB();

                OracleConnectionStringBuilder sb = new OracleConnectionStringBuilder();
                // Use below 3 for DEV
                sb.DataSource = "202.65.156.246:1521/orcl";// "10.65.65.246:1521/orcl";   //local
                sb.UserID = "ctardbcfs";
                sb.Password = "ctardbcfs31101";

                //sb.UserID = "cfstest1";
                //sb.Password = "signature";

                // Use below 3 for PRD deploymen/t
                //sb.DataSource = bc.db_server_ip;
                //sb.UserID = bc.user1;
                //sb.Password = bc.pass1;

              /*  string bankName = System.IO.Directory.GetCurrentDirectory();
                if (bc.bank_name == "CTARDBUX")
                {
                    sb.UserID = "ctardbcfs";
                    sb.Password = "ctardbcfs31101";
                }*/

                // Use below 3 for PRD deploymen/t
                //sb.DataSource = BC.connstring.Server;
                //sb.UserID = BC.connstring.UserId;
                //sb.Password = BC.connstring.Password;

                //sb.DataSource="202.65.156.246:1521/orcl";
                DbConnection connection = null;
                try
                {
                    connection = new OracleConnection(sb.ToString());
                    connection.Open();
                }
                catch (Exception ex)
                {
                    connection.Close();
                    throw ex;
                }

                return connection;
            }
        }

        private static BankConfig getBankConfigFromDB()
        {
            BankConfig bankConfigToRtrn = null;
            string bankName = System.IO.Directory.GetCurrentDirectory();
            if (null != bankName)
            {
                // PRD
                var folderName = bankName.Split('\\').LastOrDefault();                
                DbConnection connection = NewConnectionAdmin;
                try
                {

                    string _query = " SELECT BANK_NAME "
                                    + ",DB_SERVER_IP ,USER1 ,PASS1 ,USER2 "
                                    + ",PASS2 ,UPDATED_DT "
                                    + " FROM BANK_CONFIG WHERE ACTIVE_FLAG = 'Y' "
                                    + " AND BANK_NAME = '{0}' ";
                    _query = string.Format(_query, folderName);
                    using (var command = OrclDbConnection.Command(connection, _query))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                {
                                    while (reader.Read())
                                    {
                                        bankConfigToRtrn = new BankConfig();
                                        bankConfigToRtrn.bank_name = UtilityM.CheckNull<string>(reader["BANK_NAME"]);
                                        bankConfigToRtrn.db_server_ip = UtilityM.CheckNull<string>(reader["DB_SERVER_IP"]);
                                        bankConfigToRtrn.user1 = UtilityM.CheckNull<string>(reader["USER1"]);
                                        bankConfigToRtrn.pass1 = UtilityM.CheckNull<string>(reader["PASS1"]);
                                        bankConfigToRtrn.user2 = UtilityM.CheckNull<string>(reader["USER2"]);
                                        bankConfigToRtrn.pass2 = UtilityM.CheckNull<string>(reader["PASS2"]);
                                        bankConfigToRtrn.updated_dt = UtilityM.CheckNull<DateTime>(reader["UPDATED_DT"]);
                                    }
                                }
                            }
                        }
                    }

                }
                catch (Exception ex)
                {
                    if (null != connection && connection.State != System.Data.ConnectionState.Closed)
                    {
                        connection.Close();
                    }
                    throw ex;
                }
                finally
                {
                    if (null != connection && connection.State != System.Data.ConnectionState.Closed)
                    {
                        connection.Close();
                    }
                }

            }

            return bankConfigToRtrn;
        }

        // public static DbConnection NewConnectionV2
        // {
        //     get
        //     {
        //         // We need to change below to a db Call and get Config
        //         BankConfig bc = getBankConfigFromDB();
        //         // BankConfigMst BC = new BankConfigMstLL().ReadAllConfiguration();
        //         OracleConnectionStringBuilder sb = new OracleConnectionStringBuilder();
        //         // Use below 3 for DEV
        //         // sb.DataSource = "10.65.65.246:1521/orcl"; // EZ Connect -- no TNS Names!
        //         // sb.UserID = "cbs_demo";
        //         // sb.Password = "signature";

        //         // Use below 3 for PRD deploymen/t
        //         sb.DataSource = bc.db_server_ip; 
        //         sb.UserID = bc.user1; 
        //         sb.Password = bc.pass1; 

        //         //sb.DataSource="202.65.156.246:1521/orcl";
        //         DbConnection connection = null;
        //         try
        //         {
        //             connection = new OracleConnection(sb.ToString());
        //             connection.Open();
        //         }
        //         catch (Exception ex)
        //         {
        //             connection.Close();
        //             throw ex;
        //         }

        //         return connection;
        //     }
        // }


        public static OracleConnection NewConnection2
        {
            get
            {
                // BankConfigMst BC = new BankConfigMstLL().ReadAllConfiguration();
                BankConfig bc = getBankConfigFromDB();
                OracleConnectionStringBuilder sb = new OracleConnectionStringBuilder();
                // Use below 3 for DEV
                //sb.DataSource = "202.65.156.246:1521/orcl"; // ez connect -- no tns names!
                //sb.UserID = "cfs2022";
                //sb.Password = "signature";

                // Use below 3 for PRD deployment
                sb.DataSource = bc.db_server_ip;
                sb.UserID = bc.user2;
                sb.Password = bc.pass2;
                // sb.DataSource="202.65.156.246:1521/orcl";

                // string constr = "User ID=sig_demo;Password=signature;Data Source=10.65.65.246:1521/orcl";  
                OracleConnection connection = null;
                try
                {
                    connection = new OracleConnection(sb.ToString());
                    // connection = new OracleConnection(constr);
                    connection.Open();
                }
                catch (Exception ex)
                {
                    connection.Close();
                    throw ex;
                }

                return connection;
            }
        }


        public static DbConnection NewConnectionAdmin
        {
            get
            {
                // BankConfigMst BC = new BankConfigMstLL().ReadAllConfiguration();

                

                // // Use below 3 for PRD deploymen/t
                // sb.DataSource = BC.connstring.Server; 
                // sb.UserID = BC.connstring.UserId; 
                // sb.Password = BC.connstring.Password; 

                //sb.DataSource="202.65.156.246:1521/orcl";
                #region New Logic
                var conns = _config.GetSection("DbConnections").Get<BankConfig>();
                OracleConnectionStringBuilder sb = new OracleConnectionStringBuilder();
                // Use below 3 for DEV
                // sb.DataSource = "10.65.65.246:1521/orcl"; // EZ Connect -- no TNS Names!
                // sb.DataSource = "10.65.65.246:1521/orcl";                
                // // sb.DataSource = "202.65.156.246:1521/orcl";
                // sb.UserID = "admin_ardb_master";
                // sb.Password = "signature";
                
                sb.DataSource = conns.db_server_ip;
                sb.UserID = conns.user1;
                sb.Password = conns.pass1;
                #endregion

                DbConnection connection = null;
                try
                {

                    connection = new OracleConnection(sb.ToString());
                    connection.Open();
                }
                catch (Exception ex)
                {
                    connection.Close();
                    throw ex;
                }

                return connection;
            }
        }


    }
}