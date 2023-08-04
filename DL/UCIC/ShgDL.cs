using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class ShgDL
    {
        string _statement;

        internal string InsertShgData(ShgDM acc)
        {
            string _section=null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section="GetTransCDMaxId";
                        //int maxTransCD = GetShgMaxId(connection);
                        if (!String.IsNullOrWhiteSpace(acc.mmshg.shg_id.ToString()))
                        InsertShgMaster(connection, acc.mmshg);
                        if (acc.mmshgmember.Count>0)
                        InsertShgMember(connection, acc.mmshgmember,acc.mmshg.shg_id);
                        transaction.Commit();
                        return acc.mmshg.shg_id.ToString();
                    }
                    catch (Exception ex)
                    {
                        
                        transaction.Rollback();
                        return _section+ " : "+ex.Message;
                    }

                }
            }
        }
        internal int UpdateShgData(ShgDM acc) 
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.mmshg.shg_id.ToString()))
                            UpdateShgMaster(connection, acc.mmshg);
                        if (acc.mmshgmember.Count>0)
                            UpdateShgMember(connection, acc.mmshgmember,acc.mmshg.shg_id,acc.mmshg.brn_cd, acc.mmshg.ardb_cd);
                        transaction.Commit();
                        return 0;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }

                }
            }
        }
        internal int DeleteShgData(ShgDM acc) 
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.mmshg.shg_id.ToString()))
                        {
                            DeleteShgMaster(connection, acc.mmshg);

                            DeleteShgMember(connection, acc.mmshg.shg_id,acc.mmshg.brn_cd, acc.mmshg.ardb_cd);
                       
                            
                        transaction.Commit();
                        }
                        return 0;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return -1;
                    }

                }
            }
        }

        internal ShgDM GetShgData(ShgDM td)
        {
            ShgDM ShgDMRet = new ShgDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        ShgDMRet.mmshg = GetShgMaster(connection, td.mmshg);
                        ShgDMRet.mmshgmember = GetShgMember(connection, td.mmshg);
                        return ShgDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }

        internal mm_shg GetShgMaster(DbConnection connection, mm_shg dep)
        {
            mm_shg depRet = new mm_shg();
            string _query =  " SELECT SHG_ID, "   
                            + " CHAIRMAN_NAME, "   
                            + " SECRETARY_NAME, "   
                            + " VILLAGE, "   
                            + " GRUOP_SEX, "   
                            + " MONTHLY_SUBCRIPTION, "
                            + " MIN_MEMBER_LIMIT, "   
                            + " MALE_MEMBER, "   
                            + " FEMALE_MEMBER, "   
                            + " CASTE_SC, "   
                            + " CASTE_ST, "   
                            + " CASTE_GEN, "   
                            + " CASTE_MUSLIM, "   
                            + " FORM_DT, "   
                            + " SB_ACCNO, "   
                            + " BRN_CD "  
                            + " FROM MM_SHG "  
                            + " WHERE ARDB_CD = {0} AND SHG_ID = {1} AND DEL_FLAG = 'N' ";

            _statement = string.Format(_query,
                                          string.Concat("'", dep.ardb_cd, "'"),
                                          dep.shg_id != 0 ? Convert.ToString(dep.shg_id) : "SHG_ID"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new mm_shg();
                                d.shg_id               =UtilityM.CheckNull<Int64>(reader["SHG_ID"]);   
                                d.chairman_name         =UtilityM.CheckNull<string>(reader["CHAIRMAN_NAME"]);   
                                d.secretary_name        =UtilityM.CheckNull<string>(reader["SECRETARY_NAME"]);   
                                d.village               =UtilityM.CheckNull<string>(reader["VILLAGE"]);   
                                d.gruop_sex             =UtilityM.CheckNull<string>(reader["GRUOP_SEX"]);   
                                d.monthly_subcription       =Convert.ToDecimal(UtilityM.CheckNull<double>(reader["MONTHLY_SUBCRIPTION"]));
                                d.min_member_limit       =UtilityM.CheckNull<Int32>(reader["MIN_MEMBER_LIMIT"]);   
                                d.male_member          =UtilityM.CheckNull<Int32>(reader["MALE_MEMBER"]);   
                                d.female_member         =UtilityM.CheckNull<Int32>(reader["FEMALE_MEMBER"]);   
                                d.caste_sc             =UtilityM.CheckNull<Int32>(reader["CASTE_SC"]);   
                                d.caste_st              =UtilityM.CheckNull<Int32>(reader["CASTE_ST"]);   
                                d.caste_gen             =UtilityM.CheckNull<Int32>(reader["CASTE_GEN"]);   
                                d.caste_muslim          =UtilityM.CheckNull<Int32>(reader["CASTE_MUSLIM"]);   
                                d.form_dt              =UtilityM.CheckNull<DateTime>(reader["FORM_DT"]);   
                                d.sb_accno             =UtilityM.CheckNull<string>(reader["SB_ACCNO"]);   
                                d.brn_cd                =UtilityM.CheckNull<string>(reader["BRN_CD"]);  
      
                                depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }

        
        internal List<mm_shg_member> GetShgMember(DbConnection connection, mm_shg dep)
        {
            List<mm_shg_member> indList = new List<mm_shg_member>();
            string _query =  "SELECT SHG_ID, "   
         + "SHG_MEMBER_ID, "   
         + "SHG_MEMBER_NAME, "   
         + "GUARDIAN_NAME, "   
         + "SHG_MEMBER_SEX, " 
	     + "SHG_MEMBER_CASTE, "   
		 + "RELIGION, "	    
         + "DATE_OF_JOIN, "   
         + "EDUCATION, "  
		 + "BRN_CD, "
         + "STATUS, "    
         + "DATE_OF_BIRTH, "   
         + "AGE, "   
         + "WIDOW, "    
         + "TOILET_FLAG, "   
         + "MOBILE, "   
         + "ADHAR_NO, "   
         + "PAN, "   
	     + "DISABILITY_REMARKS, " 
         + "TRAINING_REMARKS "  
         + "FROM MM_SHG_MEMBER "  
         + "WHERE   ARDB_CD = {0} AND SHG_ID = {1} AND DEL_FLAG = 'N' ";  

            _statement = string.Format(_query,
                                          string.Concat("'", dep.ardb_cd, "'"),
                                          dep.shg_id != 0 ? Convert.ToString(dep.shg_id) : "SHG_ID"
                                           );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var d = new mm_shg_member();
                                d.shg_id       =    UtilityM.CheckNull<Int64>(reader["SHG_ID"]);   
                                 d.shg_member_id       =     UtilityM.CheckNull<Int64>(reader["SHG_MEMBER_ID"]);   
                                 d.shg_member_name     =      UtilityM.CheckNull<string>(reader["SHG_MEMBER_NAME"]);   
                                 d.guardian_name       =     UtilityM.CheckNull<string>(reader["GUARDIAN_NAME"]);   
                                 d.shg_member_sex      =     UtilityM.CheckNull<string>(reader["SHG_MEMBER_SEX"]); 
                                 d.shg_member_caste    =     UtilityM.CheckNull<string>(reader["SHG_MEMBER_CASTE"]);   
                                 d.religion	         =       UtilityM.CheckNull<string>(reader["RELIGION"]);	    
                                 d.date_of_join        =     UtilityM.CheckNull<DateTime>(reader["DATE_OF_JOIN"]);   
                                 d.education           =     UtilityM.CheckNull<string>(reader["EDUCATION"]);  
                                 d.brn_cd           	 =  UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                 d.status              =     UtilityM.CheckNull<string>(reader["STATUS"]);    
                                 d.date_of_birth       =     UtilityM.CheckNull<DateTime>(reader["DATE_OF_BIRTH"]);   
                                 d.age                 =     UtilityM.CheckNull<Int16>(reader["AGE"]);   
                                 d.widow               =     UtilityM.CheckNull<string>(reader["WIDOW"]);    
                                 d.toilet_flag         =     UtilityM.CheckNull<string>(reader["TOILET_FLAG"]);   
                                 d.mobile              =     UtilityM.CheckNull<Int64>(reader["MOBILE"]);   
                                 d.adhar_no            =     UtilityM.CheckNull<Int64>(reader["ADHAR_NO"]);   
                                 d.pan                 =     UtilityM.CheckNull<string>(reader["PAN"]);   
                                 d.disability_remarks  =       UtilityM.CheckNull<string>(reader["DISABILITY_REMARKS"]); 
                                 d.training_remarks    =       UtilityM.CheckNull<string>(reader["TRAINING_REMARKS"]);  
 
                                indList.Add(d);
                            }
                        }
                    }
                }
            }

            return indList;
        }
       
        internal bool InsertShgMaster(DbConnection connection, mm_shg dep)
        {
            string _query = "INSERT INTO  MM_SHG (SHG_ID,CHAIRMAN_NAME,SECRETARY_NAME,VILLAGE,GRUOP_SEX,MONTHLY_SUBCRIPTION,  " 
         + "MIN_MEMBER_LIMIT,MALE_MEMBER,FEMALE_MEMBER,CASTE_SC,CASTE_ST,CASTE_GEN,CASTE_MUSLIM,   "
         + "FORM_DT,SB_ACCNO,BRN_CD,ARDB_CD,DEL_FLAG) VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},'N')";
         
            _statement = string.Format(_query,string.Concat("'", dep.shg_id         , "'"),
                        string.Concat("'", dep.chairman_name       , "'"),
                        string.Concat("'", dep.secretary_name      , "'"),
                        string.Concat("'", dep.village             , "'"),
                        string.Concat("'", dep.gruop_sex           , "'"),
                        string.Concat("'", dep.monthly_subcription , "'"),
                        string.Concat("'", dep.min_member_limit    , "'"),
                        string.Concat("'", dep.male_member         , "'"),
                        string.Concat("'", dep.female_member       , "'"),
                        string.Concat("'", dep.caste_sc            , "'"),
                        string.Concat("'", dep.caste_st            , "'"),
                        string.Concat("'", dep.caste_gen           , "'"),
                        string.Concat("'", dep.caste_muslim        , "'"),
                        string.IsNullOrWhiteSpace(dep.form_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.form_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                        //string.Concat("'", dep.form_dt             , "'"),
                        string.Concat("'", dep.sb_accno            , "'"),
                        string.Concat("'", dep.brn_cd              , "'"),
                        string.Concat("'", dep.ardb_cd, "'"));
            
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool InsertShgMember(DbConnection connection, List<mm_shg_member> dep,decimal shgid)
        {
               string _query = " INSERT INTO MM_SHG_MEMBER (SHG_ID,SHG_MEMBER_ID,SHG_MEMBER_NAME,GUARDIAN_NAME,SHG_MEMBER_SEX,      "
+" SHG_MEMBER_CASTE,RELIGION,DATE_OF_JOIN,EDUCATION,BRN_CD,STATUS,DATE_OF_BIRTH,AGE,WIDOW,TOILET_FLAG,"
+" MOBILE,ADHAR_NO,PAN,DISABILITY_REMARKS,TRAINING_REMARKS,ARDB_CD,DEL_FLAG)                                           "
+" VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},'N') ";
   
   for (int i = 0; i < dep.Count; i++)
            {
                //int shgmemberid=GetShgMemberMaxId(connection);
            
            _statement = string.Format(_query,
            string.Concat("'", shgid           , "'"),   
            string.Concat("'", dep[i].shg_member_id     , "'"),
            string.Concat("'", dep[i].shg_member_name   , "'"),
            string.Concat("'", dep[i].guardian_name     , "'"),
            string.Concat("'", dep[i].shg_member_sex    , "'"),
            string.Concat("'", dep[i].shg_member_caste  , "'"),
            string.Concat("'", dep[i].religion	         , "'"),
            string.IsNullOrWhiteSpace(dep[i].date_of_join.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep[i].date_of_join.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            //string.Concat("'", dep.date_of_join      , "'"),
            string.Concat("'", dep[i].education         , "'"),
            string.Concat("'", dep[i].brn_cd            , "'"), 
            string.Concat("'", dep[i].status            , "'"),
            string.IsNullOrWhiteSpace(dep[i].date_of_birth.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep[i].date_of_birth.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            //string.Concat("'", dep.date_of_birth     , "'"),
            string.Concat("'", dep[i].age               , "'"),
            string.Concat("'", dep[i].widow             , "'"),
            string.Concat("'", dep[i].toilet_flag       , "'"),
            string.Concat("'", dep[i].mobile            , "'"),
            string.Concat("'", dep[i].adhar_no          , "'"),
            string.Concat("'", dep[i].pan               , "'"),
            string.Concat("'", dep[i].disability_remarks, "'"),
            string.Concat("'", dep[i].training_remarks  , "'"),
            string.Concat("'", dep[i].ardb_cd, "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            }
            return true;
        }

    internal bool UpdateShgMaster(DbConnection connection, mm_shg dep)
        {
            string _query = " UPDATE MM_SHG "
             +  " SET CHAIRMAN_NAME= {0} ,"   
            + " SECRETARY_NAME= {1} ,"   
            + " VILLAGE= {2} ,"   
            + " GRUOP_SEX= {3} ,"   
            + " MONTHLY_SUBCRIPTION= {4} ,"
            + " MIN_MEMBER_LIMIT= {5} ,"   
            + " MALE_MEMBER= {6} ,"   
            + " FEMALE_MEMBER= {7} ,"   
            + " CASTE_SC= {8} ,"   
            + " CASTE_ST= {9} ,"   
            + " CASTE_GEN= {10} ,"   
            + " CASTE_MUSLIM= {11} ,"   
            + " FORM_DT= {12} ,"   
            + " SB_ACCNO= {13} "
            + " WHERE ARDB_CD = {14} AND SHG_ID = {15} AND DEL_FLAG = 'N' ";
                _statement = string.Format(_query,
                            string.Concat("'", dep.chairman_name       , "'"),
                            string.Concat("'", dep.secretary_name      , "'"),
                            string.Concat("'", dep.village             , "'"),
                            string.Concat("'", dep.gruop_sex           , "'"),
                            string.Concat("'", dep.monthly_subcription , "'"),
                            string.Concat("'", dep.min_member_limit    , "'"),
                            string.Concat("'", dep.male_member         , "'"),
                            string.Concat("'", dep.female_member       , "'"),
                            string.Concat("'", dep.caste_sc            , "'"),
                            string.Concat("'", dep.caste_st            , "'"),
                            string.Concat("'", dep.caste_gen           , "'"),
                            string.Concat("'", dep.caste_muslim        , "'"),
                            string.IsNullOrWhiteSpace(dep.form_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.form_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            //string.Concat("'", dep.form_dt             , "'"),
                            string.Concat("'", dep.sb_accno            , "'"),
                            string.Concat("'", dep.ardb_cd              , "'"),
                            string.Concat("'", dep.shg_id              , "'"));
                
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            
            return true;
        }



        internal bool UpdateShgMember(DbConnection connection, List<mm_shg_member> dep,decimal shgid,string brncd,string ardbcd)
        {
            string _queryd=" DELETE FROM MM_SHG_MEMBER "
                         +" WHERE ardb_cd = {0} and shg_id = {1}"
                         + " AND BRN_CD={2} ";

                    try
                    {
                     _statement = string.Format(_queryd,
                                          string.Concat("'", ardbcd, "'"),
                                          dep[0].shg_id>0 ? string.Concat("'", shgid, "'") : "0",
                                          string.IsNullOrWhiteSpace(brncd.ToString()) ? "0" : string.Concat("'", brncd, "'")
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                        }
                    
              string _query = " INSERT INTO MM_SHG_MEMBER (SHG_ID,SHG_MEMBER_ID,SHG_MEMBER_NAME,GUARDIAN_NAME,SHG_MEMBER_SEX,      "
                            +" SHG_MEMBER_CASTE,RELIGION,DATE_OF_JOIN,EDUCATION,BRN_CD,STATUS,DATE_OF_BIRTH,AGE,WIDOW,TOILET_FLAG,"
                            +" MOBILE,ADHAR_NO,PAN,DISABILITY_REMARKS,TRAINING_REMARKS,ARDB_CD,DEL_FLAG)                                           "
                            +" VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},'N') ";
   
   for (int i = 0; i < dep.Count; i++)
            {
            _statement = string.Format(_query,
            string.Concat("'", shgid            , "'"),   
            string.Concat("'", dep[i].shg_member_id     , "'"),
            string.Concat("'", dep[i].shg_member_name   , "'"),
            string.Concat("'", dep[i].guardian_name     , "'"),
            string.Concat("'", dep[i].shg_member_sex    , "'"),
            string.Concat("'", dep[i].shg_member_caste  , "'"),
            string.Concat("'", dep[i].religion	         , "'"),
            string.IsNullOrWhiteSpace(dep[i].date_of_join.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep[i].date_of_join.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            //string.Concat("'", dep.date_of_join      , "'"),
            string.Concat("'", dep[i].education         , "'"),
            string.Concat("'", dep[i].brn_cd            , "'"), 
            string.Concat("'", dep[i].status            , "'"),
            string.IsNullOrWhiteSpace(dep[i].date_of_birth.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep[i].date_of_birth.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            //string.Concat("'", dep.date_of_birth     , "'"),
            string.Concat("'", dep[i].age               , "'"),
            string.Concat("'", dep[i].widow             , "'"),
            string.Concat("'", dep[i].toilet_flag       , "'"),
            string.Concat("'", dep[i].mobile            , "'"),
            string.Concat("'", dep[i].adhar_no          , "'"),
            string.Concat("'", dep[i].pan               , "'"),
            string.Concat("'", dep[i].disability_remarks, "'"),
            string.Concat("'", dep[i].training_remarks  , "'"),
            string.Concat("'", ardbcd, "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            }
            }
                    catch (Exception ex)
                    {
                        int x=0;
                        return false;
                    }
            return true;
        }
        
        internal bool DeleteShgMaster(DbConnection connection, mm_shg dep)
        {
            string _queryd = " UPDATE MM_SHG  SET DEL_FLAG = 'Y' "
                  + " WHERE ARDB_CD = {0} AND SHG_ID={1} ";

             _statement = string.Format(_queryd,
                                          string.Concat("'", dep.ardb_cd, "'"),
                                          dep.shg_id>0 ? string.Concat("'", dep.shg_id, "'") : "0"
                                           );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }

        internal bool DeleteShgMember(DbConnection connection, decimal shgid,string brncd, string ardbcd)
        {
             string _queryd=" Update MM_SHG_MEMBER set del_flag='Y'  "
                         +" WHERE ARDB_CD = {0} AND shg_id = {1}"
                         + " AND BRN_CD={2} ";

                     _statement = string.Format(_queryd,
                                          string.Concat("'", ardbcd, "'"),
                                          shgid >0 ? string.Concat("'", shgid, "'") : "0",
                                          string.IsNullOrWhiteSpace(brncd.ToString()) ? "0" :  string.Concat("'", brncd, "'")
                                           );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                        }
            return true;

        }

        internal int GetShgMemberMaxId(DbConnection connection,string ardbcd )
        {
            int maxTransCD = 0;
            string _query =   " Select		 nvl(max(SHG_MEMBER_ID),0) + 1 MAX_TRANS_CD "
                             +" From		MM_SHG_MEMBER where ARDB_CD = {0} ";
            _statement = string.Format(_query, string.Concat("'", ardbcd, "'"));
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            maxTransCD = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["MAX_TRANS_CD"]));
                        }
                    }
                }
            }
            return maxTransCD;
        }
         internal int GetShgMaxId(DbConnection connection, string ardbcd )
        {
            int maxTransCD = 0;
            string _query =   " Select		 nvl(max(SHG_ID),0) + 1 MAX_TRANS_CD "
                             +" From		MM_SHG where ardb_cd = {0} ";
            _statement = string.Format(_query, string.Concat("'", ardbcd, "'"));
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            maxTransCD = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["MAX_TRANS_CD"]));
                        }
                    }
                }
            }
            return maxTransCD;
        }

              
    }
}   
    
