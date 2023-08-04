using System;
using System.Data.Common;
using System.IO;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;

namespace SBWSFinanceApi.Config
{
    
 internal static class MySqlDbConnection 
    {
        //Startup startup=new Startup(IConfiguration configuration);

        static string connectionString = "Server=213.175.201.201; Database=docrepo; Uid=usr; Pwd=2al3~B1j";
        //static string dbConn = Startup.Configuration.GetSection("MySettings").GetSection("DbConnection").Value;
        
        //static string connectionString = Configuration["ConnectionStrings:DefaultConnection"];
        //string dbConn2 = configuration.GetValue<string>("MySettings:DbConnection"); 
        public static DbCommand Command(DbConnection connection, string cmdText)
        {
            return new MySqlCommand(cmdText, (MySqlConnection)connection);
        }

        public static DbConnection NewConnection
        {
            get
            {
                DbConnection connection = null;
                try
                {
                    connection = new MySqlConnection(connectionString);
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


