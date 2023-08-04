using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class UserDL
    {
        string _statement;

internal List<m_user_master> GetUserIDDtls(m_user_master mum)
        {
            List<m_user_master> mumRets=new List<m_user_master>();
            string _query=" SELECT ARDB_CD,BRN_CD, USER_ID, PASSWORD, LOGIN_STATUS, USER_TYPE, USER_FIRST_NAME, USER_MIDDLE_NAME, USER_LAST_NAME "
                        +" FROM M_USER_MASTER WHERE  USER_ID={0} AND ARDB_CD={1} ";
            using (var connection = OrclDbConnection.NewConnection)
            {              
               _statement = string.Format(_query,                
                                            string.Concat("'",  mum.user_id, "'"),
                                            string.Concat("'",  mum.ardb_cd, "'")
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {    
                                var mumt = new m_user_master();
                                mumt.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                mumt.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                mumt.user_id = UtilityM.CheckNull<string>(reader["USER_ID"]);
                                mumt.password=SetUserPass(UtilityM.CheckNull<string>(reader["PASSWORD"]));
                                mumt.login_status = UtilityM.CheckNull<string>(reader["LOGIN_STATUS"]);
                                mumt.user_type = UtilityM.CheckNull<string>(reader["USER_TYPE"]);
                                mumt.user_first_name = UtilityM.CheckNull<string>(reader["USER_FIRST_NAME"]);
                                mumt.user_middle_name = UtilityM.CheckNull<string>(reader["USER_MIDDLE_NAME"]);
                                mumt.user_last_name = UtilityM.CheckNull<string>(reader["USER_LAST_NAME"]);
                                mumRets.Add(mumt);

                            }
                        }
                    }
                }
            }
            return mumRets;
        }


        internal List<m_user_master> GetUserIDStatus(m_user_master mum)
        {
            List<m_user_master> mumRets = new List<m_user_master>();
            string _query = " SELECT USER_ID, LOGIN_STATUS , USER_TYPE "
                        + " FROM M_USER_MASTER WHERE  ARDB_CD={0} AND BRN_CD = {1}";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                             string.Concat("'", mum.ardb_cd, "'"),
                                             string.Concat("'", mum.brn_cd, "'")
                                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var mumt = new m_user_master();                               
                                mumt.user_id = UtilityM.CheckNull<string>(reader["USER_ID"]);
                                mumt.login_status = UtilityM.CheckNull<string>(reader["LOGIN_STATUS"]);
                                mumt.user_type = UtilityM.CheckNull<string>(reader["USER_TYPE"]);                                
                                mumRets.Add(mumt);

                            }
                        }
                    }
                }
            }
            return mumRets;
        }


        internal List<m_user_master> GetUserIDStatusAll(m_user_master mum)
        {
            List<m_user_master> mumRets = new List<m_user_master>();
            string _query = " SELECT USER_ID, LOGIN_STATUS , USER_TYPE "
                        + " FROM M_USER_MASTER WHERE  ARDB_CD={0} ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                             string.Concat("'", mum.ardb_cd, "'")
                                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var mumt = new m_user_master();
                                mumt.user_id = UtilityM.CheckNull<string>(reader["USER_ID"]);
                                mumt.login_status = UtilityM.CheckNull<string>(reader["LOGIN_STATUS"]);
                                mumt.user_type = UtilityM.CheckNull<string>(reader["USER_TYPE"]);
                                mumRets.Add(mumt);

                            }
                        }
                    }
                }
            }
            return mumRets;
        }


        internal int UpdateUserIdStatus(List<m_user_master> tvd)
        {
            int _ret = 0;
            string _query = "Update m_user_master "
                            + " Set login_status = {0}  "
                            + " Where  ardb_cd = {1} AND brn_cd = {2}  AND user_id = {3} ";
                            
            
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < tvd.Count; i++)
                        {
                            _statement = string.Format(_query,
                                           string.IsNullOrWhiteSpace(tvd[i].login_status) ? "login_status" : string.Concat("'", tvd[i].login_status, "'"),
                                           string.IsNullOrWhiteSpace(tvd[i].ardb_cd) ? "ardb_cd" : string.Concat("'", tvd[i].ardb_cd, "'"),
                                           string.IsNullOrWhiteSpace(tvd[i].brn_cd) ? "brn_cd" : string.Concat("'", tvd[i].brn_cd, "'"),
                                           string.IsNullOrWhiteSpace(tvd[i].user_id) ? "user_id" : string.Concat("'", tvd[i].user_id, "'")
                                         );
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                command.ExecuteNonQuery();
                                _ret = 0;
                            }
                        }
                        

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            _ret = 0;
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _ret = -1;
                    }
                }
            }

            return _ret;
        }



        internal int InsertUserMaster(m_user_master mum)
        {
            List<m_user_master> sigList = new List<m_user_master>();
            var passkey=GetUserPass(mum.password); 
            string _query="INSERT INTO M_USER_MASTER (ARDB_CD,BRN_CD, USER_ID, PASSWORD, LOGIN_STATUS, USER_TYPE, USER_FIRST_NAME, USER_MIDDLE_NAME, USER_LAST_NAME )"
                          +" VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8} ) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", mum.ardb_cd, "'"),
                                                   string.Concat("'", mum.brn_cd, "'"),
                                                   string.Concat("'", mum.user_id, "'") ,
                                                   string.Concat("'", passkey, "'"),
                                                   string.Concat("'", mum.login_status, "'") ,
                                                   string.Concat("'", mum.user_type, "'") ,
                                                   string.Concat("'", mum.user_first_name, "'") ,
                                                   string.Concat("'", mum.user_middle_name, "'") ,
                                                   string.Concat("'", mum.user_last_name, "'") 
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
        internal int UpdateUserMaster(m_user_master mum)
        {
            int _ret=0;
            var passkey="";
            if (!String.IsNullOrWhiteSpace(mum.password))
                passkey = GetUserPass(mum.password);
           
            string _query=" UPDATE M_USER_MASTER " 
         + " SET USER_ID = {0} , "
         +" PASSWORD     = NVL({1},PASSWORD) , "
         +" LOGIN_STATUS      = {2} , "
         +" USER_TYPE    = {3} , "
         +" USER_FIRST_NAME   = {4} , "
         +" USER_MIDDLE_NAME   = {5} , "
         +" USER_LAST_NAME    = {6}  "
         +"  WHERE  USER_ID={7} AND ARDB_CD={8} ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                                   string.Concat("'", mum.user_id, "'") ,
                                                   string.Concat("'", passkey==""?null:passkey, "'"),
                                                   string.Concat("'", mum.login_status, "'") ,
                                                   string.Concat("'", mum.user_type, "'") ,
                                                   string.Concat("'", mum.user_first_name, "'") ,
                                                   string.Concat("'", mum.user_middle_name, "'") ,
                                                   string.Concat("'", mum.user_last_name, "'") ,
                                                   string.Concat("'", mum.user_id, "'") ,
                                                   string.Concat("'", mum.ardb_cd, "'")
                                                    );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            _ret=0;
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _ret=-1;
                    }
                }           
            }
            return _ret;
        }
       internal int DeleteUserMaster(m_user_master mum)
        {
            int _ret=0;

            string _query=" UPDATE M_USER_MASTER SET USER_TYPE = 'D' "
                         +" WHERE USER_ID={0} AND ARDB_CD = {1} ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(mum.user_id) ? string.Concat("'", mum.user_id, "'") : "user_id",
                                          !string.IsNullOrWhiteSpace(mum.ardb_cd) ? string.Concat("'", mum.ardb_cd, "'") : "ardb_cd"
                                          );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            _ret=0;
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _ret=-1;
                    }
                }           
            }
            return _ret;
        }

         internal string GetUserPass(string passinp)
        {
            string passout="";
            string _query=" Select f_encript_pass({0}) pass From Dual";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                _statement = string.Format(_query,                
                                            string.IsNullOrWhiteSpace(passinp) ? "" : string.Concat("'",  passinp , "'")
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                                passout = UtilityM.CheckNull<string>(reader["PASS"]);
                            }
                        }
                    }
                }
            }
            return passout;
        }  
         internal string SetUserPass(string passinp)
        {
            string passout="";
            string _query=" Select F_DECRIPT_PASS({0}) pass From Dual";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                _statement = string.Format(_query,                
                                            string.IsNullOrWhiteSpace(passinp) ? "" : string.Concat("'",  passinp , "'")
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                                passout = UtilityM.CheckNull<string>(reader["PASS"]);
                            }
                        }
                    }
                }
            }
            return passout;
        }


        internal int InsertUserTransfer(td_user_transfer mum)
        {
            string _query = "INSERT INTO TD_USER_TRANSFER (ARDB_CD,USER_ID,BRN_CD,NEW_BRN_CD,TRANSFER_DT,USER_TYPE,APPROVAL_STATUS,CREATED_BY,CREATED_DT,MODIFIED_BY,MODIFIED_DT)"
                          + " VALUES( {0},{1},{2},{3},to_date(substr({4},1,10),'dd/mm/yyyy'),{5},'U',{6},SYSDATE,{7},SYSDATE) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", mum.ardb_cd, "'"),
                                                   string.Concat("'", mum.user_id, "'"),
                                                   string.Concat("'", mum.brn_cd, "'"),
                                                   string.Concat("'", mum.new_brn_cd, "'"),
                                                   string.Concat("'", mum.transfer_dt, "'"),
                                                   string.Concat("'", mum.user_type, "'"),
                                                   string.Concat("'", mum.created_by, "'"),
                                                   string.Concat("'", mum.modified_by, "'")
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


        internal int ApproveUserTransfer(td_user_transfer mum)
        {
            int _ret = 0;
            

         string _query = " UPDATE TD_USER_TRANSFER "
         + " SET APPROVAL_STATUS = 'A' , "
         + " APPROVED_BY     = {0} , "
         + " APPROVED_DT      = to_date(substr({1},1,10),'dd/mm/yyyy')  "
         + "  WHERE ARDB_CD = {2} and  USER_ID={3} AND BRN_CD = {4} AND  NEW_BRN_CD = {5} AND TRANSFER_DT = to_date(substr({6},1,10),'dd/mm/yyyy') ";

        string _query1 = " UPDATE M_USER_MASTER "
        + " SET BRN_CD = {0}  "
        + "  WHERE ARDB_CD = {1} and  USER_ID={2} AND BRN_CD = {3} ";


            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                      string.Concat("'", mum.approved_by, "'"),
                                                      string.Concat("'", mum.approved_dt, "'"),
                                                      string.Concat("'", mum.ardb_cd, "'"),
                                                      string.Concat("'", mum.user_id, "'"),
                                                      string.Concat("'", mum.brn_cd, "'"),
                                                      string.Concat("'", mum.new_brn_cd, "'"),
                                                      string.Concat("'", mum.transfer_dt, "'")
                                                       );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();                            
                            _ret = 0;
                        }

                        _statement = string.Format(_query1,
                                                     string.Concat("'", mum.new_brn_cd, "'"),
                                                     string.Concat("'", mum.ardb_cd, "'"),
                                                     string.Concat("'", mum.user_id, "'"),
                                                     string.Concat("'", mum.brn_cd, "'")
                                                      );

                        using (var command1 = OrclDbConnection.Command(connection, _statement))
                        {
                            command1.ExecuteNonQuery();
                            _ret = 0;
                        }
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        _ret = -1;
                    }
                }
            }
            return _ret;
        }


    }
}
    