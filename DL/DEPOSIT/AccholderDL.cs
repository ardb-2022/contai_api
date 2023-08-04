using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class AccholderDL
    {
        string _statement;
        internal List<td_accholder> GetAccholderTemp(td_accholder acc)
        {
            List<td_accholder> accList = new List<td_accholder>();

        string _query=" SELECT BRN_CD, "
             +" ACC_TYPE_CD,   "
             +" ACC_NUM,       "
             +" ACC_HOLDER,    "
             +" RELATION,      "
             +" CUST_CD        "
             +" FROM TD_ACCHOLDER_TEMP "
             +" WHERE BRN_CD = {0} AND ACC_NUM = {1} ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'")   : "brn_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num"                                          
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
                                    var a = new td_accholder();
                                    a.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    a.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                    a.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    a.acc_holder = UtilityM.CheckNull<string>(reader["ACC_HOLDER"]);
                                    a.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                    a.cust_cd = UtilityM.CheckNull<int>(reader["CUST_CD"]);                                    

                                    accList.Add(a);
                                }
                            }
                        }
                    }
                }
            }

            return accList;
        }
       internal decimal InsertAccholderTemp(td_accholder acc)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query= "INSERT INTO TD_ACCHOLDER_TEMP ( brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd ) "
                         +" VALUES( {0},{1},{2},{3}, {4}, {5} ) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", acc.brn_cd, "'"),
                                                   acc.acc_type_cd ,
                                                   string.Concat("'", acc.acc_num, "'"),
                                                   string.Concat("'", acc.acc_holder, "'"),
                                                   string.Concat("'", acc.relation, "'"),
                                                   acc.cust_cd             
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
        internal int UpdateAccholderTemp(td_accholder acc)
        {
            int _ret=0;   

            string _query=" UPDATE td_accholder_temp        " 
             +" SET brn_cd     = {0} "
             +" acc_type_cd    = {1} "
             +" acc_num        = {2} "
             +" acc_holder     = {3} "
             +" relation       = {4} "
             +" cust_cd        = {5} "
            +" WHERE brn_cd = {6} AND acc_num = {7}  ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_type_cd.ToString()) ? string.Concat("'", acc.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(acc.acc_holder) ? string.Concat("'", acc.acc_holder, "'") : "acc_holder",
                                          !string.IsNullOrWhiteSpace(acc.relation) ? string.Concat("'", acc.relation, "'") : "relation",
                                          !string.IsNullOrWhiteSpace(acc.cust_cd.ToString()) ? string.Concat("'", acc.cust_cd, "'") : "cust_cd"
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
       internal int DeleteAccholderTemp(td_accholder acc)
        {
            int _ret=0;

            string _query=" DELETE FROM td_accholder_temp "
             +" WHERE brn_cd = {0} AND acc_num = {1} AND  ACC_TYPE_CD = {2}";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num",
                                          (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD"
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
      internal List<td_accholder> GetAccholder(td_accholder acc)
        {
            List<td_accholder> accList = new List<td_accholder>();

        string _query=" SELECT BRN_CD, "
             +" ACC_TYPE_CD,   "
             +" ACC_NUM,       "
             +" ACC_HOLDER,    "
             +" RELATION,      "
             +" CUST_CD        "
             +" FROM TD_ACCHOLDER "
             +" WHERE ARDB_CD = {0} AND ACC_TYPE_CD = {1} AND ACC_NUM = {2} AND DEL_FLAG = 'N' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd",
                                          (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num"                                          
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
                                    var a = new td_accholder();
                                    a.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    a.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                    a.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    a.acc_holder = UtilityM.CheckNull<string>(reader["ACC_HOLDER"]);
                                    a.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                    a.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);                                    

                                    accList.Add(a);
                                }
                            }
                        }
                    }
                }
            }

            return accList;
        }
       internal decimal InsertAccholder(td_accholder acc)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query= "INSERT INTO TD_ACCHOLDER ( brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd ) "
                         +" VALUES( {0},{1},{2},{3}, {4}, {5} ) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _statement = string.Format(_query,
                                                   string.Concat("'", acc.brn_cd, "'"),
                                                   acc.acc_type_cd ,
                                                   string.Concat("'", acc.acc_num, "'"),
                                                   string.Concat("'", acc.acc_holder, "'"),
                                                   string.Concat("'", acc.relation, "'"),
                                                   acc.cust_cd             
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
        internal int UpdateAccholder(td_accholder acc)
        {
            int _ret=0;   

            string _query=" UPDATE td_accholder        " 
             +" SET brn_cd     = {0} "
             +" acc_type_cd    = {1} "
             +" acc_num        = {2} "
             +" acc_holder     = {3} "
             +" relation       = {4} "
             +" cust_cd        = {5} "
            +" WHERE brn_cd = {6} AND acc_num = {7}  ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_type_cd.ToString()) ? string.Concat("'", acc.acc_type_cd, "'") : "acc_type_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num",
                                          !string.IsNullOrWhiteSpace(acc.acc_holder) ? string.Concat("'", acc.acc_holder, "'") : "acc_holder",
                                          !string.IsNullOrWhiteSpace(acc.relation) ? string.Concat("'", acc.relation, "'") : "relation",
                                          !string.IsNullOrWhiteSpace(acc.cust_cd.ToString()) ? string.Concat("'", acc.cust_cd, "'") : "cust_cd"
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
       internal int DeleteAccholder(td_accholder acc)
        {
            int _ret=0;

            string _query=" DELETE FROM td_accholder "
             +" WHERE brn_cd = {0} AND acc_num = {1} AND  ACC_TYPE_CD = {2} ";

            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                     _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(acc.acc_num) ? string.Concat("'", acc.acc_num, "'") : "acc_num",
                                           (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD"
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
