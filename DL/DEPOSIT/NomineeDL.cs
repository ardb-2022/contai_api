using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class NomineeDL
    {
        string _statement;
        internal List<td_nominee> GetNomineeTemp(td_nominee nom)
        {
            List<td_nominee> nomList = new List<td_nominee>();

        string _query="SELECT ARDB_CD, BRN_CD, "
         +" ACC_TYPE_CD, "
         +" ACC_NUM,     "
         +" NOM_ID,      "
         +" NOM_NAME,    "
         +" NOM_ADDR1,   "
         +" NOM_ADDR2,   "
         +" PHONE_NO,    "
         +" PERCENTAGE,  "
         +" RELATION,DEL_FLAG     "
         +" FROM TD_NOMINEE_TEMP "
         +" WHERE ARDB_CD ={0} AND BRN_CD = {1} AND ACC_TYPE_CD = {2} AND ACC_NUM = {3}  AND DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'")   : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num"
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
                                    var n = new td_nominee();
                                    n.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                    n.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    n.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                    n.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    n.nom_id = UtilityM.CheckNull<Int16>(reader["NOM_ID"]);

                                    n.nom_name = UtilityM.CheckNull<string>(reader["NOM_NAME"]);
                                    n.nom_addr1 = UtilityM.CheckNull<string>(reader["NOM_ADDR1"]);
                                    n.nom_addr2 = UtilityM.CheckNull<string>(reader["NOM_ADDR2"]);
                                    n.phone_no = UtilityM.CheckNull<string>(reader["PHONE_NO"]);
                                    n.percentage = UtilityM.CheckNull<Single>(reader["PERCENTAGE"]);
                                    n.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                    n.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                    nomList.Add(n);
                                }
                            }
                        }
                    }
                }
            }

            return nomList;
        }
       internal decimal InsertNomineeTemp(td_nominee nom)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query="INSERT INTO TD_NOMINEE_TEMP (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                          +" VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", nom.ardb_cd, "'"),
                                                   string.Concat("'", nom.brn_cd, "'"),
                                                   nom.acc_type_cd ,
                                                   string.Concat("'", nom.acc_num, "'"),
                                                   nom.nom_id,
                                                   nom.nom_name,
                                                   nom.nom_addr1,
                                                   nom.nom_addr2,
                                                   nom.phone_no,
                                                   nom.percentage,
                                                   nom.relation
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
        internal int UpdateNomineeTemp(td_nominee nom)
        {
            int _ret=0;   

            string _query=" UPDATE TD_NOMINEE_TEMP " 
         +" SET brn_cd  = {0} , "
         +" acc_type_cd = {1} , "
         +" acc_num     = {2} , "
         +" nom_id      = {3} , "
         +" nom_name    = {4} , "
         +" nom_addr1   = {5} , "
         +" nom_addr2   = {6} , "
         +" phone_no    = {7} , "
         +" percentage  = {8} , "
         +" relation    = {9}  "
         +" WHERE ardb_cd={10} and  brn_cd = {11} AND acc_type_cd ={12} and acc_num = {13} AND nom_id = {14} AND DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? "brn_cd" : string.Concat("'", nom.brn_cd, "'") ,
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id",

                                          !string.IsNullOrWhiteSpace(nom.nom_name) ? string.Concat("'", nom.nom_name, "'") : "nom_name",
                                          !string.IsNullOrWhiteSpace(nom.nom_addr1) ? string.Concat("'", nom.nom_addr1, "'") : "nom_addr1",
                                          !string.IsNullOrWhiteSpace(nom.nom_addr2) ? string.Concat("'", nom.nom_addr2, "'") : "nom_addr2",
                                          !string.IsNullOrWhiteSpace(nom.phone_no) ? string.Concat("'", nom.phone_no, "'") : "phone_no",
                                          !string.IsNullOrWhiteSpace(nom.percentage.ToString()) ? string.Concat("'", nom.percentage, "'") : "percentage",
                                          !string.IsNullOrWhiteSpace(nom.relation) ? string.Concat("'", nom.relation, "'") : "relation",
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id"
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
       internal int DeleteNomineeTemp(td_nominee nom)
        {
            int _ret=0;

            string _query=" DELETE FROM TD_NOMINEE_TEMP "
                         +" WHERE ardb_cd = {0} and  brn_cd = {1} AND acc_type_cd ={2} and acc_num = {3}  AND nom_id = {4} AND DEL_FLAG='N'  ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id"
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

 internal List<td_nominee> GetNominee(td_nominee nom)
        {
            List<td_nominee> nomList = new List<td_nominee>();

        string _query="SELECT BRN_CD, "
         +" ACC_TYPE_CD, "
         +" ACC_NUM,     "
         +" NOM_ID,      "
         +" NOM_NAME,    "
         +" NOM_ADDR1,   "
         +" NOM_ADDR2,   "
         +" PHONE_NO,    "
         +" PERCENTAGE,  "
         +" RELATION ,ARDB_CD,DEL_FLAG    "
         +" FROM TD_NOMINEE "
         +" WHERE ARDB_CD={0} AND BRN_CD = {1} AND ACC_TYPE_CD = {2} AND ACC_NUM = {3} AND DEL_FLAG='N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'")   : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num"                                          
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
                                    var n = new td_nominee();
                                    n.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    n.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                    n.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    n.nom_id = UtilityM.CheckNull<Int16>(reader["NOM_ID"]);

                                    n.nom_name = UtilityM.CheckNull<string>(reader["NOM_NAME"]);
                                    n.nom_addr1 = UtilityM.CheckNull<string>(reader["NOM_ADDR1"]);
                                    n.nom_addr2 = UtilityM.CheckNull<string>(reader["NOM_ADDR2"]);
                                    n.phone_no = UtilityM.CheckNull<string>(reader["PHONE_NO"]);
                                    n.percentage = UtilityM.CheckNull<Single>(reader["PERCENTAGE"]);
                                    n.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                    n.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                    n.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                    nomList.Add(n);
                                }
                            }
                        }
                    }
                }
            }

            return nomList;
        }
       internal decimal InsertNominee(td_nominee nom)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query="INSERT INTO TD_NOMINEE (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                          +" VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", nom.ardb_cd, "'"),
                                                   string.Concat("'", nom.brn_cd, "'"),
                                                   nom.acc_type_cd ,
                                                   string.Concat("'", nom.acc_num, "'"),
                                                   nom.nom_id,
                                                   nom.nom_name,
                                                   nom.nom_addr1,
                                                   nom.nom_addr2,
                                                   nom.phone_no,
                                                   nom.percentage,
                                                   nom.relation
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
        internal int UpdateNominee(td_nominee nom)
        {
            int _ret=0;   

            string _query=" UPDATE TD_NOMINEE " 
         +" SET brn_cd  = {0} , "
         +" acc_type_cd = {1} , "
         +" acc_num     = {2} , "
         +" nom_id      = {3} , "
         +" nom_name    = {4} , "
         +" nom_addr1   = {5} , "
         +" nom_addr2   = {6} , "
         +" phone_no    = {7} , "
         +" percentage  = {8} , "
         +" relation    = {9}  "
         +" WHERE ardb_cd = {10} AND brn_cd = {11} AND acc_num = {12} AND nom_id = {13} and del_flag='N'";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? "brn_cd" : string.Concat("'", nom.brn_cd, "'") ,
                                          !string.IsNullOrWhiteSpace(nom.acc_type_cd.ToString()) ? string.Concat("'", nom.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id",

                                          !string.IsNullOrWhiteSpace(nom.nom_name) ? string.Concat("'", nom.nom_name, "'") : "nom_name",
                                          !string.IsNullOrWhiteSpace(nom.nom_addr1) ? string.Concat("'", nom.nom_addr1, "'") : "nom_addr1",
                                          !string.IsNullOrWhiteSpace(nom.nom_addr2) ? string.Concat("'", nom.nom_addr2, "'") : "nom_addr2",
                                          !string.IsNullOrWhiteSpace(nom.phone_no) ? string.Concat("'", nom.phone_no, "'") : "phone_no",
                                          !string.IsNullOrWhiteSpace(nom.percentage.ToString()) ? string.Concat("'", nom.percentage, "'") : "percentage",
                                          !string.IsNullOrWhiteSpace(nom.relation) ? string.Concat("'", nom.relation, "'") : "relation",
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id"
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
       internal int DeleteNominee(td_nominee nom)
        {
            int _ret=0;

            string _query=" DELETE FROM TD_NOMINEE "
                         +" WHERE ardb_cd={0} and brn_cd = {1} AND acc_num = {2} AND nom_id={3} ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(nom.acc_num) ? string.Concat("'", nom.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(nom.nom_id.ToString()) ? string.Concat("'", nom.nom_id, "'") : "nom_id"
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

    }
}
