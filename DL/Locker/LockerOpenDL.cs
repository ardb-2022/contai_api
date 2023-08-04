using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;


namespace SBWSFinanceApi.DL
{
    public class LockerOpenDL
    {
        string _statement;
        string _statement1;

        internal List<mm_locker> GetLockerMaster(p_report_param prp)
        {
            List<mm_locker> lockermaster = new List<mm_locker>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            
            string _query = " SELECT MM_LOCKER.ARDB_CD,         "
                  + " MM_LOCKER.BRN_CD,                        "
                  + " MM_LOCKER.LOCKER_TYPE,                     "
                  + " MM_LOCKER.LOCKER_ID,                         "
                  + " MM_LOCKER.LOCKER_STATUS,                      "
                  + " MM_LOCKER.CREATED_BY,                  "
                  + " MM_LOCKER.CREATED_DT,                          "
                  + " MM_LOCKER.MODIFIED_BY,  "
                  + " MM_LOCKER.MODIFIED_DT                    "
                  + " FROM MM_LOCKER                                              "
                  + " WHERE MM_LOCKER.ARDB_CD = '{0}' AND                   "
                  + "       MM_LOCKER.BRN_CD = '{1}' ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                                                   prp.ardb_cd,
                                                   prp.brn_cd);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var pb = new mm_locker();

                                        pb.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                        pb.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                        pb.locker_type = UtilityM.CheckNull<string>(reader["LOCKER_TYPE"]);
                                        pb.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                        pb.locker_status = UtilityM.CheckNull<string>(reader["LOCKER_STATUS"]);
                                        pb.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                        pb.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                        pb.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                        pb.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);

                                        lockermaster.Add(pb);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        lockermaster = null;
                    }
                }
            }

            return lockermaster;
        }



        internal int InsertLockerMaster(List<mm_locker> tvd)
        {
            int _ret = 0;
            string _query = "Select count(*) NO_COUNT from  mm_locker"
                        + " Where ardb_cd = {0} AND brn_cd = {1}  AND locker_id = {2} ";
            string _query1 = "Update mm_locker "
                            + " Set locker_type = {0} , locker_status = {1}, modified_by = {2}, modified_dt = to_date({3},'dd-mm-yyyy' ) "
                            + " Where ardb_cd = {4} AND brn_cd = {5}  AND locker_id = {6} ";                          
            string _queryIns = "INSERT INTO mm_locker (ARDB_CD,BRN_CD, locker_type, locker_id, locker_status, created_by, created_dt, modified_by, modified_dt)"
                            + " VALUES ({0},{1},{2},{3},{4},{5}, to_date('{6}','dd-mm-yyyy' ), {7}, to_date('{8}','dd-mm-yyyy' ))";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < tvd.Count; i++)
                        {
                            
                            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd[i].ardb_cd) ? "ardb_cd" : string.Concat("'", tvd[i].ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tvd[i].brn_cd) ? "brn_cd" : string.Concat("'", tvd[i].brn_cd, "'"),
                                            string.Concat("'", tvd[i].locker_id, "'")
                                            );
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    if (reader.Read()) //if (reader.HasRows)
                                    {
                                        decimal _ret1;

                                        _ret1 = (decimal)reader["NO_COUNT"]; // UtilityM.CheckNull<int>(reader["NO_COUNT"]);
                                            
                                        if(_ret1 == 0) 
                                        {
                                            _statement1 = string.Format(_queryIns,
                                                                        string.Concat("'", tvd[i].ardb_cd, "'"),
                                                                        string.Concat("'", tvd[i].brn_cd, "'"),
                                                                        string.Concat("'", tvd[i].locker_type, "'"),
                                                                        string.Concat("'", tvd[i].locker_id, "'"),
                                                                        string.Concat("'", tvd[i].locker_status, "'"),
                                                                        string.Concat("'", tvd[i].created_by, "'"),
                                                                        tvd[i].created_dt.ToString("dd/MM/yyyy"),
                                                                        string.Concat("'", tvd[i].modified_by, "'"),
                                                                        tvd[i].modified_dt.ToString("dd/MM/yyyy")
                                                                        );


                                            using (var command1 = OrclDbConnection.Command(connection, _statement1))
                                            {
                                                command1.ExecuteNonQuery();
                                                //transaction.Commit();
                                                _ret = 0;
                                            }
                                        }
                                        else                                               
                                        {
                                            _statement1 = string.Format(_query1,
                                                                        string.Concat("'", tvd[i].locker_type, "'"),
                                                                        string.Concat("'", tvd[i].locker_status, "'"),
                                                                        string.Concat("'", tvd[i].modified_by, "'"),
                                                                        string.Concat("'", tvd[i].modified_dt.ToString("dd/MM/yyyy"), "'"),
                                                                        string.Concat("'", tvd[i].ardb_cd, "'"),
                                                                        string.Concat("'", tvd[i].brn_cd, "'"),                                                                            
                                                                        string.Concat("'", tvd[i].locker_id, "'")
                                                                        );


                                            using (var command1 = OrclDbConnection.Command(connection, _statement1))
                                            {
                                                command1.ExecuteNonQuery();
                                                //transaction.Commit();
                                                _ret = 0;
                                            }
                                        }
                                    }
                                }
                            }
                                                       
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



        internal List<locker_rent> GetLockerRent()
        {
            List<locker_rent> lockermaster = new List<locker_rent>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";


            string _query = " SELECT LKRENT.LKTYPE,      "
                  + " LKRENT.RENTAMT,                    "
                  + " LKRENT.RENTDATE                    "                 
                  + " FROM LKRENT                        " ;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var pb = new locker_rent();

                                        pb.locker_type = UtilityM.CheckNull<string>(reader["LKTYPE"]);
                                        pb.rentamt = UtilityM.CheckNull<double>(reader["RENTAMT"]);
                                        pb.eff_date = UtilityM.CheckNull<DateTime>(reader["RENTDATE"]);                                       

                                        lockermaster.Add(pb);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        lockermaster = null;
                    }
                }
            }

            return lockermaster;
        }

      internal List<locker_rent> GetLockerRentlist()
        {
            List<locker_rent> lockermaster = new List<locker_rent>();

            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";


            string _query = " SELECT LKTYPE, RENTDATE, RENTAMT      "
                  + " FROM LKRENT                    "
                  + " WHERE RENTDATE = (SELECT max(RENTDATE) FROM LKRENT) ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query);

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var pb = new locker_rent();

                                        pb.locker_type = UtilityM.CheckNull<string>(reader["LKTYPE"]);
                                        pb.rentamt = UtilityM.CheckNull<double>(reader["RENTAMT"]);
                                        pb.eff_date = UtilityM.CheckNull<DateTime>(reader["RENTDATE"]);

                                        lockermaster.Add(pb);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        lockermaster = null;
                    }
                }
            }

            return lockermaster;
        }



        internal int InsertLockerRentMaster(List<locker_rent> tvd)
        {
            int _ret = 0;
            string _query = "Select count(*) NO_COUNT from  LKRENT"
                        + " Where LKTYPE = {0} AND RENTDATE = to_date({1},'dd-mm-yyyy' ) ";
            string _query1 = "Update LKRENT "
                            + " Set RENTAMT = {0}  "
                            + " Where LKTYPE = {1} AND RENTDATE = to_date({2},'dd-mm-yyyy' ) ";
            string _queryIns = "INSERT INTO LKRENT (LKTYPE, RENTDATE, RENTAMT )"
                            + " VALUES ({0}, to_date({1},'dd-mm-yyyy' ), {2})";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < tvd.Count; i++)
                        {

                            _statement = string.Format(_query,
                                            string.Concat("'", tvd[i].locker_type, "'"),
                                            string.Concat("'", tvd[i].eff_date.ToString("dd/MM/yyyy"), "'")
                                            );
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    if (reader.Read()) //if (reader.HasRows)
                                    {
                                        decimal _ret1;

                                        _ret1 = (decimal)reader["NO_COUNT"]; // UtilityM.CheckNull<int>(reader["NO_COUNT"]);

                                        if (_ret1 == 0)
                                        {
                                            _statement1 = string.Format(_queryIns,
                                                                        string.Concat("'", tvd[i].locker_type, "'"),
                                                                        string.Concat("'", tvd[i].eff_date.ToString("dd/MM/yyyy"), "'"),
                                                                        string.Concat("'", tvd[i].rentamt, "'")
                                                                        );


                                            using (var command1 = OrclDbConnection.Command(connection, _statement1))
                                            {
                                                command1.ExecuteNonQuery();
                                                //transaction.Commit();
                                                _ret = 0;
                                            }
                                        }
                                        else
                                        {
                                            _statement1 = string.Format(_query1,
                                                                        tvd[i].rentamt,
                                                                        string.Concat("'", tvd[i].locker_type, "'"),                                                                       
                                                                        string.Concat("'", tvd[i].eff_date.ToString("dd/MM/yyyy"), "'")
                                                                        );


                                            using (var command1 = OrclDbConnection.Command(connection, _statement1))
                                            {
                                                command1.ExecuteNonQuery();
                                                //transaction.Commit();
                                                _ret = 0;
                                            }
                                        }
                                    }
                                }
                            }

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


        internal tm_locker GetLockerDtls(tm_locker dep)
        {
            // tm_locker depRet = new tm_locker();

            var d = new tm_locker();

            string _query = " SELECT  TM_LOCKER.UCIC ,TM_LOCKER.KEY_NO , TM_LOCKER.AGREEMENT_NO , TM_LOCKER.AGREEMENT_DT , TM_LOCKER.LOCKER_ID, "
                            + " TM_LOCKER.ACC_TYPE_CD , TM_LOCKER.ACC_NUM , TM_LOCKER.IND_ACC_TYPE_CD , TM_LOCKER.IND_ACC_NUM, TM_LOCKER.NARRATION ,   "
                            + " TM_LOCKER.NOMINEE_NO , TM_LOCKER.RENTED_TILL , TM_LOCKER.NAME , TM_LOCKER.ACC_STATUS , TM_LOCKER.APPROVAL_STATUS ,    "
                            + " TM_LOCKER.BRN_CD , MM_CUSTOMER.CUST_NAME , MM_CUSTOMER.PRESENT_ADDRESS ,  "
                            + " MM_CUSTOMER.OCCUPATION , MM_CUSTOMER.PHONE , TM_LOCKER.AMT_RECV                         "
                            + " FROM TM_LOCKER , MM_CUSTOMER                                                                 "
                            + " WHERE ( TM_LOCKER.UCIC = MM_CUSTOMER.CUST_CD ) and ( ( TM_LOCKER.AGREEMENT_NO = {0} ) and  ( TM_LOCKER.BRN_CD = {1} ) and "
                            + " ( TM_LOCKER.ARDB_CD = {2} ) and ( TM_LOCKER.ACC_STATUS = 'O' ) ) ";

            _statement = string.Format(_query,                                         
                                          !string.IsNullOrWhiteSpace(dep.agreement_no) ? string.Concat("'", dep.agreement_no, "'") : "agreement_no",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                           !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd"
                                           );
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {

                    using (var reader = command.ExecuteReader())
                    {
                        {
                            while (reader.Read())
                            {
                               // var d = new tm_locker();
                                d.cust_cd = UtilityM.CheckNull<decimal>(reader["UCIC"]);
                                d.key_no = UtilityM.CheckNull<string>(reader["KEY_NO"]);
                                d.agreement_no = UtilityM.CheckNull<string>(reader["AGREEMENT_NO"]);
                                d.agreement_dt = UtilityM.CheckNull<DateTime>(reader["AGREEMENT_DT"]);
                                d.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                d.ind_acc_type_cd = UtilityM.CheckNull<int>(reader["IND_ACC_TYPE_CD"]);
                                d.ind_acc_num = UtilityM.CheckNull<string>(reader["IND_ACC_NUM"]);
                                d.narration = UtilityM.CheckNull<string>(reader["NARRATION"]);
                                d.nominee_no = UtilityM.CheckNull<string>(reader["NOMINEE_NO"]);
                                d.rented_till = UtilityM.CheckNull<DateTime>(reader["RENTED_TILL"]);
                                d.name = UtilityM.CheckNull<string>(reader["NAME"]);
                                d.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                d.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                d.occupation = UtilityM.CheckNull<string>(reader["OCCUPATION"]);
                                d.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                d.amt_recv = UtilityM.CheckNull<decimal>(reader["AMT_RECV"]); 
                                

                            }
                        }
                    }
                }
            }

            

            return d;
        }


        internal tm_locker GetLockerDtlsView(tm_locker dep)
        {
            // tm_locker depRet = new tm_locker();

            var d = new tm_locker();

            string _query = " SELECT  TM_LOCKER.UCIC ,TM_LOCKER.KEY_NO , TM_LOCKER.AGREEMENT_NO , TM_LOCKER.AGREEMENT_DT , TM_LOCKER.LOCKER_ID, "
                            + " TM_LOCKER.ACC_TYPE_CD , TM_LOCKER.ACC_NUM , TM_LOCKER.IND_ACC_TYPE_CD , TM_LOCKER.IND_ACC_NUM, TM_LOCKER.NARRATION ,   "
                            + " TM_LOCKER.NOMINEE_NO , TM_LOCKER.RENTED_TILL , TM_LOCKER.NAME , TM_LOCKER.ACC_STATUS , TM_LOCKER.APPROVAL_STATUS ,    "
                            + " TM_LOCKER.BRN_CD , MM_CUSTOMER.CUST_NAME , MM_CUSTOMER.PRESENT_ADDRESS ,  "
                            + " MM_CUSTOMER.OCCUPATION , MM_CUSTOMER.PHONE , TM_LOCKER.AMT_RECV                         "
                            + " FROM TM_LOCKER , MM_CUSTOMER                                                                 "
                            + " WHERE ( TM_LOCKER.UCIC = MM_CUSTOMER.CUST_CD ) and ( ( TM_LOCKER.AGREEMENT_NO = {0} ) and  ( TM_LOCKER.BRN_CD = {1} ) and "
                            + " ( TM_LOCKER.ARDB_CD = {2} ) and ( TM_LOCKER.ACC_STATUS = 'O' ) and ( TM_LOCKER.APPROVAL_STATUS = 'A' ) ) ";

            _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.agreement_no) ? string.Concat("'", dep.agreement_no, "'") : "agreement_no",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                           !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd"
                                           );
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {

                    using (var reader = command.ExecuteReader())
                    {
                        {
                            while (reader.Read())
                            {
                                // var d = new tm_locker();
                                d.cust_cd = UtilityM.CheckNull<decimal>(reader["UCIC"]);
                                d.key_no = UtilityM.CheckNull<string>(reader["KEY_NO"]);
                                d.agreement_no = UtilityM.CheckNull<string>(reader["AGREEMENT_NO"]);
                                d.agreement_dt = UtilityM.CheckNull<DateTime>(reader["AGREEMENT_DT"]);
                                d.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                d.ind_acc_type_cd = UtilityM.CheckNull<int>(reader["IND_ACC_TYPE_CD"]);
                                d.ind_acc_num = UtilityM.CheckNull<string>(reader["IND_ACC_NUM"]);
                                d.narration = UtilityM.CheckNull<string>(reader["NARRATION"]);
                                d.nominee_no = UtilityM.CheckNull<string>(reader["NOMINEE_NO"]);
                                d.rented_till = UtilityM.CheckNull<DateTime>(reader["RENTED_TILL"]);
                                d.name = UtilityM.CheckNull<string>(reader["NAME"]);
                                d.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                d.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                d.cust_name = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                d.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                d.occupation = UtilityM.CheckNull<string>(reader["OCCUPATION"]);
                                d.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                d.amt_recv = UtilityM.CheckNull<decimal>(reader["AMT_RECV"]);


                            }
                        }
                    }
                }
            }


            return d;
        }


        internal List<td_accholder_locker> GetAccholder(DbConnection connection, tm_locker acc)
        {
            List<td_accholder_locker> accList = new List<td_accholder_locker>();

            dynamic _query = " SELECT ARDB_CD, BRN_CD, "
                 + " ACC_TYPE_CD,   "
                 + " ACC_NUM,       "
                 + " ACC_HOLDER,    "
                 + " RELATION,      "
                 + " CUST_CD, DEL_FLAG        "
                 + " FROM TD_ACCHOLDER "
                 + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = 34102 AND DEL_FLAG='N'  ";
            var v1 = !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd";
            var v2 = !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd";
            var v3 = !string.IsNullOrWhiteSpace(acc.agreement_no) ? string.Concat("'", acc.agreement_no, "'") : "acc_num";
            //dynamic v4 = (acc.acc_type_cd > 0) ? acc.acc_type_cd.ToString() : "ACC_TYPE_CD";
            _statement = string.Format(_query, v1, v2, v3);


            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var a = new td_accholder_locker();
                                a.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                a.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                a.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                a.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                a.acc_holder = UtilityM.CheckNull<string>(reader["ACC_HOLDER"]);
                                a.relation = UtilityM.CheckNull<string>(reader["RELATION"]);
                                a.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                a.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);

                                accList.Add(a);
                            }
                        }
                    }
                }
            }
            return accList;
        }



        internal List<td_nominee_locker> GetNominee(DbConnection connection, tm_locker dep)
        {
            List<td_nominee_locker> nomList = new List<td_nominee_locker>();

            string _query = "SELECT ARDB_CD, BRN_CD, "
             + " ACC_TYPE_CD, "
             + " ACC_NUM,     "
             + " NOM_ID,      "
             + " NOM_NAME,    "
             + " NOM_ADDR1,   "
             + " NOM_ADDR2,   "
             + " PHONE_NO,    "
             + " PERCENTAGE,  "
             + " RELATION ,DEL_FLAG    "
             + " FROM TD_NOMINEE_TEMP "
             + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND ACC_TYPE_CD = 34102 AND ACC_NUM = {2} AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                !string.IsNullOrWhiteSpace(dep.agreement_no) ? string.Concat("'", dep.agreement_no, "'") : "acc_num");
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        {
                            while (reader.Read())
                            {
                                var n = new td_nominee_locker();
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
            return nomList;
        }



        internal td_def_trans_trf GetDepTrans(DbConnection connection, tm_locker tdt)
        {
            td_def_trans_trf tdtRets = new td_def_trans_trf();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " INSTRUMENT_DT,"
         + " INSTRUMENT_NUM,"
         + " PAID_TO,"
         + " TOKEN_NUM,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " MODIFIED_BY,"
         + " MODIFIED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " PARTICULARS,"
         + " TR_ACC_TYPE_CD,"
         + " TR_ACC_NUM,"
         + " VOUCHER_DT,"
         + " VOUCHER_ID,"
         + " TRF_TYPE,"
         + " TR_ACC_CD,"
         + " ACC_CD,"
         + " SHARE_AMT,"
         + " SUM_ASSURED,"
         + " PAID_AMT,"
         + " CURR_PRN_RECOV,"
         + " OVD_PRN_RECOV,"
         + " CURR_INTT_RECOV,"
         + " OVD_INTT_RECOV,"
         + " REMARKS,"
         + " CROP_CD,"
         + " ACTIVITY_CD,"
         + " CURR_INTT_RATE,"
         + " OVD_INTT_RATE,"
         + " INSTL_NO,"
         + " INSTL_START_DT,"
         + " PERIODICITY,"
         + " DISB_ID,"
         + " COMP_UNIT_NO,"
         + " ONGOING_UNIT_NO,"
         + " MIS_ADVANCE_RECOV,"
         + " AUDIT_FEES_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD,ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV "
         + " FROM TD_DEP_TRANS"
         + " WHERE  ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = 34102 AND  NVL(APPROVAL_STATUS, 'U') = 'U' AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                                      !string.IsNullOrWhiteSpace(tdt.ardb_cd) ? string.Concat("'", tdt.ardb_cd, "'") : "ardb_cd",
                                      !string.IsNullOrWhiteSpace(tdt.brn_cd) ? string.Concat("'", tdt.brn_cd, "'") : "brn_cd",
                                      !string.IsNullOrWhiteSpace(tdt.agreement_no) ? string.Concat("'", tdt.agreement_no, "'") : "acc_num"
                                      );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new td_def_trans_trf();
                            tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                            tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                            tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                            tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                            tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                            tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                            tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                            tdtr.instrument_dt = UtilityM.CheckNull<DateTime>(reader["INSTRUMENT_DT"]);
                            tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                            tdtr.paid_to = UtilityM.CheckNull<string>(reader["PAID_TO"]);
                            tdtr.token_num = UtilityM.CheckNull<string>(reader["TOKEN_NUM"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                            tdtr.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                            tdtr.tr_acc_type_cd = UtilityM.CheckNull<Int32>(reader["TR_ACC_TYPE_CD"]);
                            tdtr.tr_acc_num = UtilityM.CheckNull<string>(reader["TR_ACC_NUM"]);
                            tdtr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                            tdtr.voucher_id = UtilityM.CheckNull<decimal>(reader["VOUCHER_ID"]);
                            tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                            tdtr.tr_acc_cd = UtilityM.CheckNull<int>(reader["TR_ACC_CD"]);
                            tdtr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                            tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["SHARE_AMT"]);
                            tdtr.sum_assured = UtilityM.CheckNull<decimal>(reader["SUM_ASSURED"]);
                            tdtr.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                            tdtr.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                            tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                            tdtr.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                            tdtr.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                            tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                            tdtr.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                            tdtr.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                            tdtr.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                            tdtr.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                            tdtr.instl_no = UtilityM.CheckNull<Int32>(reader["INSTL_NO"]);
                            tdtr.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                            tdtr.periodicity = UtilityM.CheckNull<Int16>(reader["PERIODICITY"]);
                            tdtr.disb_id = UtilityM.CheckNull<decimal>(reader["DISB_ID"]);
                            tdtr.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                            tdtr.ongoing_unit_no = UtilityM.CheckNull<decimal>(reader["ONGOING_UNIT_NO"]);
                            tdtr.mis_advance_recov = UtilityM.CheckNull<decimal>(reader["MIS_ADVANCE_RECOV"]);
                            tdtr.audit_fees_recov = UtilityM.CheckNull<decimal>(reader["AUDIT_FEES_RECOV"]);
                            tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                            tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                            tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                            tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                            tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                            tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                            tdtRets = tdtr;
                        }
                    }
                }

            }
            return tdtRets;
        }


        internal List<tm_transfer> GetTransfer(DbConnection connection, tm_locker tdt)
        {
            List<tm_transfer> tdtRets = new List<tm_transfer>();
            string _query = "SELECT  TRF_DT,"
         + " TRF_CD,"
         + " TRANS_CD,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " BRN_CD,ARDB_CD,DEL_FLAG "
         + " FROM TM_TRANSFER"
         + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
         + " (TRF_DT = to_date('{2}','dd-mm-yyyy' )) AND  "
         + " (  TRANS_CD = {3} ) AND (DEL_FLAG = 'N') ";
            _statement = string.Format(_query,
                                         string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                         string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                         tdt.trans_dt != null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "TRF_DT",
                                         tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "TRANS_CD"
                                         );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new tm_transfer();
                            tdtr.trf_dt = UtilityM.CheckNull<DateTime>(reader["TRF_DT"]);
                            tdtr.trf_cd = UtilityM.CheckNull<Int32>(reader["TRF_CD"]);
                            tdtr.trans_cd = UtilityM.CheckNull<decimal>(reader["TRANS_CD"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }

            return tdtRets;
        }

        internal List<td_def_trans_trf> GetDepTransTrf(DbConnection connection, tm_locker tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " INSTRUMENT_DT,"
         + " INSTRUMENT_NUM,"
         + " PAID_TO,"
         + " TOKEN_NUM,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " MODIFIED_BY,"
         + " MODIFIED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " PARTICULARS,"
         + " TR_ACC_TYPE_CD,"
         + " TR_ACC_NUM,"
         + " VOUCHER_DT,"
         + " VOUCHER_ID,"
         + " TRF_TYPE,"
         + " TR_ACC_CD,"
         + " ACC_CD,"
         + " SHARE_AMT,"
         + " SUM_ASSURED,"
         + " PAID_AMT,"
         + " CURR_PRN_RECOV,"
         + " OVD_PRN_RECOV,"
         + " CURR_INTT_RECOV,"
         + " OVD_INTT_RECOV,"
         + " REMARKS,"
         + " CROP_CD,"
         + " ACTIVITY_CD,"
         + " CURR_INTT_RATE,"
         + " OVD_INTT_RATE,"
         + " INSTL_NO,"
         + " INSTL_START_DT,"
         + " PERIODICITY,"
         + " DISB_ID,"
         + " COMP_UNIT_NO,"
         + " ONGOING_UNIT_NO,"
         + " MIS_ADVANCE_RECOV,"
         + " AUDIT_FEES_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD, ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV "
         + " FROM TD_DEP_TRANS_TRF"
         + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
         + " (TRANS_DT = to_date('{2}','dd-mm-yyyy' )) AND  "
         + " (  TRANS_CD = {3} ) AND (DEL_FLAG='N')  ";
            _statement = string.Format(_query,
                                        string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'"),
                                        string.IsNullOrWhiteSpace(tdt.brn_cd) ? "brn_cd" : string.Concat("'", tdt.brn_cd, "'"),
                                        tdt.trans_dt != null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy") : "trans_dt",
                                        tdt.trans_cd != 0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
                                        );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tdtr = new td_def_trans_trf();
                            tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                            tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                            tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                            tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                            tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                            tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                            tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                            tdtr.instrument_dt = UtilityM.CheckNull<DateTime>(reader["INSTRUMENT_DT"]);
                            tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                            tdtr.paid_to = UtilityM.CheckNull<string>(reader["PAID_TO"]);
                            tdtr.token_num = UtilityM.CheckNull<string>(reader["TOKEN_NUM"]);
                            tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                            tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                            tdtr.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                            tdtr.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                            tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                            tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                            tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                            tdtr.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                            tdtr.tr_acc_type_cd = UtilityM.CheckNull<Int32>(reader["TR_ACC_TYPE_CD"]);
                            tdtr.tr_acc_num = UtilityM.CheckNull<string>(reader["TR_ACC_NUM"]);
                            tdtr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                            tdtr.voucher_id = UtilityM.CheckNull<decimal>(reader["VOUCHER_ID"]);
                            tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                            tdtr.tr_acc_cd = UtilityM.CheckNull<int>(reader["TR_ACC_CD"]);
                            tdtr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                            tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["SHARE_AMT"]);
                            tdtr.sum_assured = UtilityM.CheckNull<decimal>(reader["SUM_ASSURED"]);
                            tdtr.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                            tdtr.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                            tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                            tdtr.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                            tdtr.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                            tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                            tdtr.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                            tdtr.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                            tdtr.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                            tdtr.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                            tdtr.instl_no = UtilityM.CheckNull<Int32>(reader["INSTL_NO"]);
                            tdtr.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                            tdtr.periodicity = UtilityM.CheckNull<Int16>(reader["PERIODICITY"]);
                            tdtr.disb_id = UtilityM.CheckNull<decimal>(reader["DISB_ID"]);
                            tdtr.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                            tdtr.ongoing_unit_no = UtilityM.CheckNull<decimal>(reader["ONGOING_UNIT_NO"]);
                            tdtr.mis_advance_recov = UtilityM.CheckNull<decimal>(reader["MIS_ADVANCE_RECOV"]);
                            tdtr.audit_fees_recov = UtilityM.CheckNull<decimal>(reader["AUDIT_FEES_RECOV"]);
                            tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                            tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                            tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                            tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                            tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                            tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                            tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                            tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                            tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                            tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                            tdtRets.Add(tdtr);
                        }
                    }
                }
            }
            return tdtRets;
        }


        internal LockerOpenDM GetLockerOpeningData(tm_locker td)
        {
            LockerOpenDM lockerOpenDMRet = new LockerOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        lockerOpenDMRet.tmlocker = GetLockerDtls(td);
                        lockerOpenDMRet.tdaccholder = GetAccholder(connection, td);
                        lockerOpenDMRet.tdnominee = GetNominee(connection, td);
                        lockerOpenDMRet.tddeftrans = GetDepTrans(connection, td);
                        if (!String.IsNullOrWhiteSpace(lockerOpenDMRet.tddeftrans.trans_cd.ToString()) && lockerOpenDMRet.tddeftrans.trans_cd > 0)
                        {
                            td.trans_cd = lockerOpenDMRet.tddeftrans.trans_cd;
                            td.trans_dt = lockerOpenDMRet.tddeftrans.trans_dt;
                            lockerOpenDMRet.tmtransfer = GetTransfer(connection, td);
                            lockerOpenDMRet.tddeftranstrf = GetDepTransTrf(connection, td);
                        }
                        // transaction.Commit();
                        return lockerOpenDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }


        internal LockerOpenDM GetLockerOpeningDataView(tm_locker td)
        {
            LockerOpenDM lockerOpenDMRet = new LockerOpenDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        lockerOpenDMRet.tmlocker = GetLockerDtlsView(td);
                        lockerOpenDMRet.tdaccholder = GetAccholder(connection, td);
                        lockerOpenDMRet.tdnominee = GetNominee(connection, td);
                        
                        // transaction.Commit();
                        return lockerOpenDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }


        internal int GetTransCDMaxId(DbConnection connection, td_def_trans_trf tvd)
        {
            int maxTransCD = 0;
            string _query = "Select Nvl(max(trans_cd) + 1, 1) max_trans_cd"
                            + " From   td_dep_trans"
                            + " Where ardb_cd = {0} and  trans_dt =  {1} "
                            + " And    brn_cd = {2} ";
            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tvd.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tvd.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            string.IsNullOrWhiteSpace(tvd.brn_cd) ? "brn_cd" : string.Concat("'", tvd.brn_cd, "'")
                                            );
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




        internal int GetTrfCDMaxId(DbConnection connection, tm_transfer tvd)
        {
            int maxTransCD = 0;
            string _query = "Select Nvl(max(trf_cd) + 1, 1) max_trf_cd"
                            + " From   tm_transfer"
                            + " Where ARDB_CD = {0} AND  trf_dt =  {1} "
                            + "  And    brn_cd = {2} ";
            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(tvd.ardb_cd) ? "ardb_cd" : string.Concat("'", tvd.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace(tvd.trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tvd.trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            string.IsNullOrWhiteSpace(tvd.brn_cd) ? "brn_cd" : string.Concat("'", tvd.brn_cd, "'")
                                            );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            maxTransCD = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["max_trf_cd"]));
                        }
                    }
                }
            }
            return maxTransCD;
        }



        internal string InsertLockerOpeningData(LockerOpenDM acc)
        {
            string _section = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section = "GetTransCDMaxId";
                        int maxTransCD = GetTransCDMaxId(connection, acc.tddeftrans);
                        _section = "InsertTmlocker";
                        if (!String.IsNullOrWhiteSpace(acc.tmlocker.agreement_no))
                            InsertTmlocker(connection, acc.tmlocker);
                        _section = "InsertNominee";
                        if (acc.tdnominee.Count > 0)
                            InsertNominee(connection, acc.tdnominee);                        
                        _section = "InsertAccholder";
                        if (acc.tdaccholder.Count > 0)
                            InsertAccholder(connection, acc.tdaccholder);
                        _section = "InsertTransfer";
                        if (acc.tmtransfer.Count > 0)
                            InsertTransfer(connection, acc.tmtransfer, maxTransCD);
                        _section = "InsertDepTransTrf";
                        if (acc.tddeftranstrf.Count > 0)
                            InsertDepTransTrf(connection, acc.tddeftranstrf, maxTransCD);
                        _section = "InsertDepTrans";
                        if (!String.IsNullOrWhiteSpace(maxTransCD.ToString()) && maxTransCD != 0)
                            InsertDepTrans(connection, acc.tddeftrans, maxTransCD);
                        transaction.Commit();
                        return maxTransCD.ToString();
                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return _section + " : " + ex.Message;
                    }

                }
            }
        }


        internal bool InsertTmlocker(DbConnection connection, tm_locker dep)
        {
            string _query = " INSERT INTO TM_LOCKER( ARDB_CD, BRN_CD, AGREEMENT_NO,AGREEMENT_DT,LOCKER_ID,"
                           + " KEY_NO,	ACC_TYPE_CD,ACC_NUM,NAME,NOMINEE_NO,RENTED_TILL,NARRATION,     "
                           + " IND_ACC_TYPE_CD,IND_ACC_NUM,ACC_STATUS,	APPROVAL_STATUS,"
                           + " AMT_RECV,UCIC,CREATED_BY,CREATED_DT,MODIFIED_BY,      "
                           + " MODIFIED_DT,APPROVED_BY,APPROVED_DT,DEL_FLAG )  "
                           + " VALUES({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},"
                           + " 'O', 'U', {14},{15},{16},sysdate,null,null,null,null,'N')";

            _statement = string.Format(_query,
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.agreement_no, "'"),
            string.IsNullOrWhiteSpace(dep.agreement_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.agreement_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.locker_id, "'"),
            string.Concat("'", dep.key_no, "'"),
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.name, "'"),
            string.Concat("'", dep.nominee_no, "'"),
            string.IsNullOrWhiteSpace(dep.rented_till.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.rented_till.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.narration, "'"),
            string.Concat("'", dep.ind_acc_type_cd, "'"),
            string.Concat("'", dep.ind_acc_num, "'"),
            string.Concat("'", dep.amt_recv, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.created_by, "'")
                  );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }


        internal bool InsertTdlockertrans(DbConnection connection, tm_locker dep , int transcd,DateTime transdt)
        {
            string _query = " INSERT INTO TD_LOCKER_TRANS(ARDB_CD,BRN_CD,TRANS_CD,TRANS_DT,LOCK_ACC_NUM,TRANS_TYPE,AMOUNT,"
                           + " APPROVAL_STATUS,CREATED_BY,CREATED_DT,GST,     "
                           + "TOTAL_AMT,LOCKER_ID,TRANS_MODE,RENTED_TILL,DEL_FLAG) "
                           + " VALUES({0},{1},{2},{3},{4},'O',{5},'U',{6},{7},sysdate,{8},{9},{10},"
                           + " 'O', 'U', {11},{12},{13},sysdate,null,null,null,null,'N')";

            _statement = string.Format(_query,
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            transcd,
            string.Concat("'", transdt, "'"),
            string.Concat("'", dep.agreement_no, "'"),
            string.Concat("'", dep.amt_recv, "'"),
            string.Concat("'", dep.created_by, "'"),
            string.Concat("'", dep.amt_recv, "'"),
            string.Concat("'", dep.name, "'"),
            string.Concat("'", dep.nominee_no, "'"),
            string.IsNullOrWhiteSpace(dep.rented_till.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.rented_till.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
            string.Concat("'", dep.narration, "'"),
            string.Concat("'", dep.ind_acc_type_cd, "'"),
            string.Concat("'", dep.ind_acc_num, "'"),
            string.Concat("'", dep.acc_status, "'"),
            string.Concat("'", dep.approval_status, "'"),
            string.Concat("'", dep.amt_recv, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.created_by, "'")
                  );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }


        internal bool InsertNominee(DbConnection connection, List<td_nominee_locker> nom)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "INSERT INTO TD_NOMINEE (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                          + " VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                _statement = string.Format(_query,
                                                  string.Concat("'", nom[i].ardb_cd, "'"),
                                                  string.Concat("'", nom[i].brn_cd, "'"),
                                                  nom[i].acc_type_cd,
                                                  string.Concat("'", nom[i].acc_num, "'"),
                                                  nom[i].nom_id,
                                                  string.Concat("'", nom[i].nom_name, "'"),
                                                  string.Concat("'", nom[i].nom_addr1, "'"),
                                                  string.Concat("'", nom[i].nom_addr2, "'"),
                                                  string.Concat("'", nom[i].phone_no, "'"),
                                                  nom[i].percentage,
                                                  string.Concat("'", nom[i].relation, "'")
                                                   );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }



       internal bool InsertAccholder(DbConnection connection, List<td_accholder_locker> acc)
        {
            List<td_signatory> sigList = new List<td_signatory>();

            string _query = "INSERT INTO TD_ACCHOLDER ( ardb_cd, brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,del_flag ) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N') ";

            for (int i = 0; i < acc.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", acc[i].ardb_cd, "'"),
                                                       string.Concat("'", acc[i].brn_cd, "'"),
                                                       acc[i].acc_type_cd,
                                                       string.Concat("'", acc[i].acc_num, "'"),
                                                       string.Concat("'", acc[i].acc_holder, "'"),
                                                       string.Concat("'", acc[i].relation, "'"),
                                                       acc[i].cust_cd
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }



        internal bool InsertTransfer(DbConnection connection, List<tm_transfer> tdt, int transcd)
        {


            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TM_TRANSFER (TRF_DT,TRF_CD,TRANS_CD,CREATED_BY,"
                        + " CREATED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG)"
                        + " VALUES ({0},{1},{2},{3},"
                        + " {4},{5},{6},{7},"
                        + " {8},{9},'N')";

            for (int i = 0; i < tdt.Count; i++)
            {
                //int maxTrfCD = GetTrfCDMaxId(connection, tdt[i]);
                _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(transcd),
                             string.Concat(transcd),
                             string.Concat("'", tdt[i].created_by, "'"),
                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("sysdate"),
                             string.Concat("'", tdt[i].approval_status, "'"),
                             string.Concat("'", tdt[i].approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.Concat("'", tdt[i].ardb_cd, "'")
                             );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }


        internal bool InsertDepTransTrf(DbConnection connection, List<td_def_trans_trf> tdt, int transcd)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TD_DEP_TRANS_TRF (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                        + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                        + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG,HOME_BRN_CD,INTRA_BRANCH_TRN,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                        + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                        + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                        + " {38},{39},{40},{41},{42},{43},{44}, {45},{46},{47},{48},{49},"
                        + " {50},{51},'N',{52},{53},{54},{55},{56})";


            for (int i = 0; i < tdt.Count; i++)
            {



                _statement = string.Format(_query,
                                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat(transcd),
                                             string.Concat("'", tdt[i].acc_type_cd, "'"),
                                             string.Concat("'", tdt[i].acc_num, "'"),
                                             string.Concat("'", tdt[i].trans_type, "'"),
                                             string.Concat("'", tdt[i].trans_mode, "'"),
                                             string.Concat("'", tdt[i].amount, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].instrument_num, "'"),
                                             string.Concat("'", tdt[i].paid_to, "'"),
                                             string.Concat("'", tdt[i].token_num, "'"),
                                             string.Concat("'", tdt[i].created_by, "'"),
                                             string.Concat("sysdate"),
                                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].modified_by, "'"),
                                             string.Concat("sysdate"),
                                             //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].approval_status, "'"),
                                             string.Concat("'", tdt[i].approved_by, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].particulars, "'"),
                                             string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                             string.Concat("'", tdt[i].tr_acc_num, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].voucher_id, "'"),
                                             string.Concat("'", tdt[i].trf_type, "'"),
                                             string.Concat("'", tdt[i].tr_acc_cd, "'"),
                                             string.Concat("'", tdt[i].acc_cd, "'"),
                                             string.Concat("'", tdt[i].share_amt, "'"),
                                             string.Concat("'", tdt[i].sum_assured, "'"),
                                             string.Concat("'", tdt[i].paid_amt, "'"),
                                             string.Concat("'", tdt[i].curr_prn_recov, "'"),
                                             string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                                             string.Concat("'", tdt[i].curr_intt_recov, "'"),
                                             string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                                             string.Concat("'", tdt[i].remarks, "'"),
                                             string.Concat("'", tdt[i].crop_cd, "'"),
                                             string.Concat("'", tdt[i].activity_cd, "'"),
                                             string.Concat("'", tdt[i].curr_intt_rate, "'"),
                                             string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                                             string.Concat("'", tdt[i].instl_no, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].periodicity, "'"),
                                             string.Concat("'", tdt[i].disb_id, "'"),
                                             string.Concat("'", tdt[i].comp_unit_no, "'"),
                                             string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                             string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                             string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                             string.Concat("'", tdt[i].sector_cd, "'"),
                                             string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                             string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat("'", tdt[i].brn_cd, "'"),
                                             string.Concat("'", tdt[i].ardb_cd, "'"),
                                             string.Concat("'", tdt[i].home_brn_cd, "'"),
                                             string.Concat("'", tdt[i].intra_branch_trn, "'"),
                                             string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                             string.Concat("'", tdt[i].adv_prn_recov, "'"),
                                             string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }



        internal bool InsertDepTrans(DbConnection connection, td_def_trans_trf tdt, int transcd)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "INSERT INTO TD_DEP_TRANS (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                        + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                        + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                        + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                        + " BRN_CD,ARDB_CD,DEL_FLAG,HOME_BRN_CD,INTRA_BRANCH_TRN,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                        + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                        + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                        + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                        + " {38},{39},{40},{41},{42},{43},{44}, {45},{46},{47},{48},{49},"
                        + " {50},{51},'N',{52},{53},{54},{55},{56})";

            _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", transcd, "'"),
                             string.Concat("'", tdt.acc_type_cd, "'"),
                             string.Concat("'", tdt.acc_num, "'"),
                             string.Concat("'", tdt.trans_type, "'"),
                             string.Concat("'", tdt.trans_mode, "'"),
                             string.Concat("'", tdt.amount, "'"),
                             string.IsNullOrWhiteSpace(tdt.instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.instrument_num, "'"),
                             string.Concat("'", tdt.paid_to, "'"),
                             string.Concat("'", tdt.token_num, "'"),
                             string.Concat("'", tdt.created_by, "'"),
                             string.Concat("sysdate"),
                             // string.IsNullOrWhiteSpace(tdt.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.modified_by, "'"),
                             string.Concat("sysdate"),
                             //string.IsNullOrWhiteSpace(tdt.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.approval_status, "'"),
                             string.Concat("'", tdt.approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.particulars, "'"),
                             string.Concat("'", tdt.tr_acc_type_cd, "'"),
                             string.Concat("'", tdt.tr_acc_num, "'"),
                             string.IsNullOrWhiteSpace(tdt.voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.voucher_id, "'"),
                             string.Concat("'", tdt.trf_type, "'"),
                             string.Concat("'", tdt.tr_acc_cd, "'"),
                             string.Concat("'", tdt.acc_cd, "'"),
                             string.Concat("'", tdt.share_amt, "'"),
                             string.Concat("'", tdt.sum_assured, "'"),
                             string.Concat("'", tdt.paid_amt, "'"),
                             string.Concat("'", tdt.curr_prn_recov, "'"),
                             string.Concat("'", tdt.ovd_prn_recov, "'"),
                             string.Concat("'", tdt.curr_intt_recov, "'"),
                             string.Concat("'", tdt.ovd_intt_recov, "'"),
                             string.Concat("'", tdt.remarks, "'"),
                             string.Concat("'", tdt.crop_cd, "'"),
                             string.Concat("'", tdt.activity_cd, "'"),
                             string.Concat("'", tdt.curr_intt_rate, "'"),
                             string.Concat("'", tdt.ovd_intt_rate, "'"),
                             string.Concat("'", tdt.instl_no, "'"),
                             string.IsNullOrWhiteSpace(tdt.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.periodicity, "'"),
                             string.Concat("'", tdt.disb_id, "'"),
                             string.Concat("'", tdt.comp_unit_no, "'"),
                             string.Concat("'", tdt.ongoing_unit_no, "'"),
                             string.Concat("'", tdt.mis_advance_recov, "'"),
                             string.Concat("'", tdt.audit_fees_recov, "'"),
                             string.Concat("'", tdt.sector_cd, "'"),
                             string.Concat("'", tdt.spl_prog_cd, "'"),
                             string.Concat("'", tdt.borrower_cr_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.home_brn_cd, "'"),
                             string.Concat("'", tdt.intra_branch_trn, "'"),
                             string.Concat("'", tdt.penal_intt_recov, "'"),
                             string.Concat("'", tdt.adv_prn_recov, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                             );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }



        internal int UpdateLockerOpeningData(LockerOpenDM acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.tmlocker.agreement_no))
                            UpdateTmlocker(connection, acc.tmlocker);
                        if (acc.tdnominee.Count > 0)
                            UpdateNominee(connection, acc.tdnominee);
                        if (acc.tdaccholder.Count > 0)
                            UpdateAccholder(connection, acc.tdaccholder);
                        if (acc.tmtransfer.Count > 0)
                            UpdateTransfer(connection, acc.tmtransfer);
                        if (acc.tddeftranstrf.Count > 0)
                            UpdateDepTransTrf(connection, acc.tddeftranstrf);
                        if (!String.IsNullOrWhiteSpace(acc.tddeftrans.trans_cd.ToString()))
                            UpdateDepTrans(connection, acc.tddeftrans);
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


        internal bool UpdateTmlocker(DbConnection connection, tm_locker dep)
        {
            string _query = " UPDATE TM_LOCKER SET "
                  + "acc_type_cd          = NVL({0},  acc_type_cd         ),"
                  + "acc_num              = NVL({1},  acc_num             ),"
                  + "ind_acc_type_cd      = NVL({2},  ind_acc_type_cd     ),"
                  + "ind_acc_num          = NVL({3},  ind_acc_num         ),"
                  + "locker_id            = NVL({4},  locker_id           ),"
                  + "key_no               = NVL({5},  key_no           ),"
                  + "ucic                 = NVL({6},  ucic            ),"
                  + "narration            = NVL({7},  narration       ),"
                  + "rented_till          = NVL({8},  rented_till     ),"
                  + "amt_recv             = NVL({9},  amt_recv        ),"
                  + "name                 = NVL({10},  name           ),"
                  + "modified_dt          =  sysdate ,"
                  + "modified_by          = NVL({11},  modified_by ) "                  
                  + "WHERE ardb_cd={12} and brn_cd = NVL({13}, brn_cd) AND agreement_no = NVL({14},  agreement_no ) " ;

            _statement = string.Format(_query,
                    
            string.Concat("'", dep.acc_type_cd, "'"),
            string.Concat("'", dep.acc_num, "'"),
            string.Concat("'", dep.ind_acc_type_cd, "'"),
            string.Concat("'", dep.ind_acc_num, "'"),
            string.Concat("'", dep.locker_id, "'"),
            string.Concat("'", dep.key_no, "'"),
            string.Concat("'", dep.cust_cd, "'"),
            string.Concat("'", dep.narration, "'"),
            string.Concat("'", dep.rented_till, "'"),
            string.Concat("'", dep.amt_recv, "'"),
            string.Concat("'", dep.name, "'"),
            string.Concat("'", dep.modified_by, "'"),
            string.Concat("'", dep.ardb_cd, "'"),
            string.Concat("'", dep.brn_cd, "'"),
            string.Concat("'", dep.agreement_no, "'")
            );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }


        internal bool UpdateNominee(DbConnection connection, List<td_nominee_locker> nom)
        {
            string _queryd = " DELETE FROM TD_NOMINEE "
                         + " WHERE ardb_cd={0} and  brn_cd = {1} AND acc_num = {2} ";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(nom[0].ardb_cd) ? string.Concat("'", nom[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(nom[0].brn_cd) ? string.Concat("'", nom[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(nom[0].acc_num) ? string.Concat("'", nom[0].acc_num, "'") : "acc_num"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = " UPDATE TD_NOMINEE "
             + " SET brn_cd  = {0} , "
             + " acc_type_cd = {1} , "
             + " acc_num     = {2} , "
             + " nom_id      = {3} , "
             + " nom_name    = {4} , "
             + " nom_addr1   = {5} , "
             + " nom_addr2   = {6} , "
             + " phone_no    = {7} , "
             + " percentage  = {8} , "
             + " relation    = {9}  "
             + " WHERE ardb_cd = {10} And brn_cd = {11} AND acc_num = {12} AND nom_id = {13} AND acc_type_cd=NVL({14},  acc_type_cd ) ";
            string _queryins = "INSERT INTO TD_NOMINEE (ardb_cd,brn_cd,acc_type_cd,acc_num,nom_id,nom_name,nom_addr1,nom_addr2,phone_no,percentage,relation,del_flag )"
                         + " VALUES( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},'N' ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                nom[i].upd_ins_flag = "I";
                if (nom[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(nom[i].brn_cd) ? "brn_cd" : string.Concat("'", nom[i].brn_cd, "'"),
                                         !string.IsNullOrWhiteSpace(nom[i].acc_type_cd.ToString()) ? string.Concat("'", nom[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].acc_num) ? string.Concat("'", nom[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_id.ToString()) ? string.Concat("'", nom[i].nom_id, "'") : "nom_id",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_name) ? string.Concat("'", nom[i].nom_name, "'") : "nom_name",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_addr1) ? string.Concat("'", nom[i].nom_addr1, "'") : "nom_addr1",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_addr2) ? string.Concat("'", nom[i].nom_addr2, "'") : "nom_addr2",
                                         !string.IsNullOrWhiteSpace(nom[i].phone_no) ? string.Concat("'", nom[i].phone_no, "'") : "phone_no",
                                         !string.IsNullOrWhiteSpace(nom[i].percentage.ToString()) ? string.Concat("'", nom[i].percentage, "'") : "percentage",
                                         !string.IsNullOrWhiteSpace(nom[i].relation) ? string.Concat("'", nom[i].relation, "'") : "relation",
                                         !string.IsNullOrWhiteSpace(nom[i].ardb_cd) ? string.Concat("'", nom[i].ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].brn_cd) ? string.Concat("'", nom[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(nom[i].acc_num) ? string.Concat("'", nom[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(nom[i].nom_id.ToString()) ? string.Concat("'", nom[i].nom_id, "'") : "nom_id",
                                          string.Concat("'", nom[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                 string.Concat("'", nom[i].ardb_cd, "'"),
                                                 string.Concat("'", nom[i].brn_cd, "'"),
                                                 nom[i].acc_type_cd,
                                                 string.Concat("'", nom[i].acc_num, "'"),
                                                 nom[i].nom_id,
                                                 string.Concat("'", nom[i].nom_name, "'"),
                                                 string.Concat("'", nom[i].nom_addr1, "'"),
                                                 string.Concat("'", nom[i].nom_addr2, "'"),
                                                 string.Concat("'", nom[i].phone_no, "'"),
                                                 nom[i].percentage,
                                                 string.Concat("'", nom[i].relation, "'")
                                                  );

                }

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }


        internal bool UpdateAccholder(DbConnection connection, List<td_accholder_locker> acc)
        {
            string _queryd = " DELETE FROM td_accholder "
            + " WHERE ardb_cd = {0} and brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = 34102";
            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(acc[0].ardb_cd) ? string.Concat("'", acc[0].ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(acc[0].brn_cd) ? string.Concat("'", acc[0].brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(acc[0].acc_num) ? string.Concat("'", acc[0].acc_num, "'") : "acc_num"
                                     );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            string _query = " UPDATE td_accholder   "
                 + " SET brn_cd     = {0}, "
                 + " acc_type_cd    = {1}, "
                 + " acc_num        = {2}, "
                 + " acc_holder     = {3}, "
                 + " relation       = {4}, "
                 + " cust_cd        = {5} "
                + " WHERE ardb_cd={6} and brn_cd = {7} AND acc_num = {8} AND acc_type_cd=34102  and del_flag='N' ";
            string _queryins = "INSERT INTO TD_ACCHOLDER ( ardb_cd,brn_cd, acc_type_cd, acc_num, acc_holder, relation, cust_cd,del_flag ) "
                        + " VALUES( {0},{1},{2},{3}, {4}, {5},{6},'N' ) ";

            for (int i = 0; i < acc.Count; i++)
            {
                acc[i].upd_ins_flag = "I";
                if (acc[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                         !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_type_cd.ToString()) ? string.Concat("'", acc[i].acc_type_cd, "'") : "acc_type_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_holder) ? string.Concat("'", acc[i].acc_holder, "'") : "acc_holder",
                                         !string.IsNullOrWhiteSpace(acc[i].relation) ? string.Concat("'", acc[i].relation, "'") : "relation",
                                         !string.IsNullOrWhiteSpace(acc[i].cust_cd.ToString()) ? string.Concat("'", acc[i].cust_cd, "'") : "cust_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].ardb_cd) ? string.Concat("'", acc[i].ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].brn_cd) ? string.Concat("'", acc[i].brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(acc[i].acc_num) ? string.Concat("'", acc[i].acc_num, "'") : "acc_num",
                                         string.Concat("'", acc[i].acc_type_cd, "'")
                                         );
                }
                else
                {
                    _statement = string.Format(_queryins,
                                                       string.Concat("'", acc[i].ardb_cd, "'"),
                                                       string.Concat("'", acc[i].brn_cd, "'"),
                                                       acc[i].acc_type_cd,
                                                       string.Concat("'", acc[i].acc_num, "'"),
                                                       string.Concat("'", acc[i].acc_holder, "'"),
                                                       string.Concat("'", acc[i].relation, "'"),
                                                       acc[i].cust_cd
                                                        );

                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            return true;
        }


        public bool UpdateTransfer(DbConnection connection, List<tm_transfer> tdt)
        {
            string _queryd = "DELETE FROM TM_TRANSFER  "
             + " WHERE  (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
             + " (TRF_DT = {2}) AND  "
             + " (  TRANS_CD = {3} ) ";
            try
            {
                _statement = string.Format(_queryd,
                             string.Concat("'", tdt[0].ardb_cd, "'"),
                             string.Concat("'", tdt[0].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[0].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[0].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt[0].trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }

            string _query = "UPDATE TM_TRANSFER SET "
                   + " TRF_DT               =NVL({0},TRF_DT       ),"
                   + " TRF_CD               =NVL({1},TRF_CD       ),"
                   + " TRANS_CD               =NVL({2},TRANS_CD       ),"
                   + " CREATED_BY             =NVL({3},CREATED_BY     ),"
                   + " CREATED_DT             =NVL({4},CREATED_DT     ),"
                   + " APPROVAL_STATUS        =NVL({5},APPROVAL_STATUS),"
                   + " APPROVED_BY            =NVL({6},APPROVED_BY    ),"
                   + " APPROVED_DT            =NVL({7},APPROVED_DT    ),"
                   + " BRN_CD                 =NVL({8},BRN_CD         )"
              + " WHERE (ARDB_CD = {9}) AND (BRN_CD = {10}) AND "
              + " (TRF_DT = {11} ) AND  "
              + " (  TRF_CD = {12} ) AND DEL_FLAG ='N' ";
            string _queryins = "INSERT INTO TM_TRANSFER (TRF_DT,TRF_CD,TRANS_CD,CREATED_BY,"
                      + " CREATED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,"
                      + " BRN_CD,ARDB_CD,DEL_FLAG)"
                      + " VALUES ({0},{1},{2},{3},"
                      + " {4},{5},{6},{7},"
                      + " {8},{9},'N')";
            for (int i = 0; i < tdt.Count; i++)
            {
                tdt[i].upd_ins_flag = "I";
                if (tdt[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].trans_cd, "'"),
                             string.Concat("'", tdt[i].trans_cd, "'"),
                             string.Concat("'", tdt[i].created_by, "'"),
                             //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("sysdate"),
                             string.Concat("'", tdt[i].approval_status, "'"),
                             string.Concat("'", tdt[i].approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.Concat("'", tdt[i].ardb_cd, "'"),
                             string.Concat("'", tdt[i].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt[i].trf_cd, "'")
                             );
                }
                else
                {
                    int maxTrfCD = GetTrfCDMaxId(connection, tdt[i]);
                    _statement = string.Format(_queryins,
                           string.IsNullOrWhiteSpace(tdt[i].trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                           tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                           string.Concat("'", tdt[i].created_by, "'"),
                           //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           string.Concat("sysdate"),
                           string.Concat("'", tdt[i].approval_status, "'"),
                           string.Concat("'", tdt[i].approved_by, "'"),
                           string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                           string.Concat("'", tdt[i].brn_cd, "'"),
                           string.Concat("'", tdt[i].ardb_cd, "'")
                           );
                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }

            }
            return true;
        }


        public bool UpdateDepTransTrf(DbConnection connection, List<td_def_trans_trf> tdt)
        {
            string _queryd = "DELETE FROM TD_DEP_TRANS_TRF  "
            + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
            + " (TRANS_DT = {2}) AND  "
            + " (  TRANS_CD = {3} ) ";

            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt[0].ardb_cd, "'"),
                             string.Concat("'", tdt[0].brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt[0].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[0].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt[0].trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            string _query = "UPDATE TD_DEP_TRANS_TRF SET "
         + " TRANS_DT               =NVL({0},TRANS_DT       ),"
         + " TRANS_CD               =NVL({1},TRANS_CD       ),"
         + " ACC_TYPE_CD            =NVL({2},ACC_TYPE_CD    ),"
         + " ACC_NUM                =NVL({3},ACC_NUM        ),"
         + " TRANS_TYPE             =NVL({4},TRANS_TYPE     ),"
         + " TRANS_MODE             =NVL({5},TRANS_MODE     ),"
         + " AMOUNT                 =NVL({6},AMOUNT         ),"
         + " INSTRUMENT_DT          =NVL({7},INSTRUMENT_DT  ),"
         + " INSTRUMENT_NUM         =NVL({8},INSTRUMENT_NUM ),"
         + " PAID_TO                =NVL({9},PAID_TO        ),"
         + " TOKEN_NUM              =NVL({10},TOKEN_NUM      ),"
         + " CREATED_BY             =NVL({11},CREATED_BY     ),"
         + " CREATED_DT             =NVL({12},CREATED_DT     ),"
         + " MODIFIED_BY            =NVL({13},MODIFIED_BY    ),"
         + " MODIFIED_DT            =NVL({14},MODIFIED_DT    ),"
         + " APPROVAL_STATUS        =NVL({15},APPROVAL_STATUS),"
         + " APPROVED_BY            =NVL({16},APPROVED_BY    ),"
         + " APPROVED_DT            =NVL({17},APPROVED_DT    ),"
         + " PARTICULARS            =NVL({18},PARTICULARS    ),"
         + " TR_ACC_TYPE_CD         =NVL({19},TR_ACC_TYPE_CD ),"
         + " TR_ACC_NUM             =NVL({20},TR_ACC_NUM     ),"
         + " VOUCHER_DT             =NVL({21},VOUCHER_DT     ),"
         + " VOUCHER_ID             =NVL({22},VOUCHER_ID     ),"
         + " TRF_TYPE               =NVL({23},TRF_TYPE       ),"
         + " TR_ACC_CD              =NVL({24},TR_ACC_CD      ),"
         + " ACC_CD                 =NVL({25},ACC_CD         ),"
         + " SHARE_AMT              =NVL({26},SHARE_AMT      ),"
         + " SUM_ASSURED            =NVL({27},SUM_ASSURED    ),"
         + " PAID_AMT               =NVL({28},PAID_AMT       ),"
         + " CURR_PRN_RECOV         =NVL({29},CURR_PRN_RECOV ),"
         + " OVD_PRN_RECOV          =NVL({30},OVD_PRN_RECOV  ),"
         + " CURR_INTT_RECOV        =NVL({31},CURR_INTT_RECOV),"
         + " OVD_INTT_RECOV         =NVL({32},OVD_INTT_RECOV ),"
         + " REMARKS                =NVL({33},REMARKS        ),"
         + " CROP_CD                =NVL({34},CROP_CD        ),"
         + " ACTIVITY_CD            =NVL({35},ACTIVITY_CD    ),"
         + " CURR_INTT_RATE         =NVL({36},CURR_INTT_RATE ),"
         + " OVD_INTT_RATE          =NVL({37},OVD_INTT_RATE  ),"
         + " INSTL_NO               =NVL({38},INSTL_NO       ),"
         + " INSTL_START_DT         =NVL({39},INSTL_START_DT ),"
         + " PERIODICITY            =NVL({40},PERIODICITY    ),"
         + " DISB_ID                =NVL({41},DISB_ID        ),"
         + " COMP_UNIT_NO           =NVL({42},COMP_UNIT_NO   ),"
         + " ONGOING_UNIT_NO        =NVL({43},ONGOING_UNIT_NO),"
         + " MIS_ADVANCE_RECOV      =NVL({44},MIS_ADVANCE_RECOV),"
         + " AUDIT_FEES_RECOV       =NVL({45},AUDIT_FEES_RECOV),"
         + " SECTOR_CD              =NVL({46},SECTOR_CD      ),"
         + " SPL_PROG_CD            =NVL({47},SPL_PROG_CD    ),"
         + " BORROWER_CR_CD         =NVL({48},BORROWER_CR_CD ),"
         + " INTT_TILL_DT           =NVL({49},INTT_TILL_DT   ),"
         + " BRN_CD                 =NVL({50},BRN_CD         ),"
         + " PENAL_INTT_RECOV       =NVL({51},PENAL_INTT_RECOV),"
         + " ADV_PRN_RECOV          =NVL({52},ADV_PRN_RECOV) "
    + " WHERE (ARDB_CD = {53}) AND (BRN_CD = {54}) AND "
    + " (TRANS_DT = {55}) AND  "
    + " (  TRANS_CD = {56} ) AND  "
    + " ACC_TYPE_CD = {57} AND "
    + " ACC_NUM = {58} AND DEL_FLAG = 'N' ";
            string _queryins = "INSERT INTO TD_DEP_TRANS_TRF (TRANS_DT,TRANS_CD,ACC_TYPE_CD,ACC_NUM,TRANS_TYPE,TRANS_MODE,AMOUNT,INSTRUMENT_DT,INSTRUMENT_NUM,PAID_TO,TOKEN_NUM,CREATED_BY,"
                                + " CREATED_DT,MODIFIED_BY,MODIFIED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT,PARTICULARS,TR_ACC_TYPE_CD,TR_ACC_NUM,VOUCHER_DT,VOUCHER_ID,TRF_TYPE,TR_ACC_CD,"
                                + " ACC_CD,SHARE_AMT,SUM_ASSURED,PAID_AMT,CURR_PRN_RECOV,OVD_PRN_RECOV,CURR_INTT_RECOV,OVD_INTT_RECOV,REMARKS,CROP_CD,ACTIVITY_CD,CURR_INTT_RATE,OVD_INTT_RATE,"
                                + " INSTL_NO,INSTL_START_DT,PERIODICITY,DISB_ID,COMP_UNIT_NO,ONGOING_UNIT_NO,MIS_ADVANCE_RECOV,AUDIT_FEES_RECOV,SECTOR_CD,SPL_PROG_CD,BORROWER_CR_CD,INTT_TILL_DT,"
                                + " BRN_CD,ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV,VALUE_DT)"
                                + " VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}, {10},{11},"
                                + " {12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}, {24},"
                                + " {25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},"
                                + " {38},{39},{40},{41},{42},{43},{44},{45},{46},{47},{48},{49}, "
                                + " {50},{51},'N',{52},{53},{54})";
            for (int i = 0; i < tdt.Count; i++)
            {
                tdt[i].upd_ins_flag = "I";
                if (tdt[i].upd_ins_flag == "U")
                {
                    _statement = string.Format(_query,
                                 string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat(tdt[i].trans_cd),
                                 string.Concat("'", tdt[i].acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].acc_num, "'"),
                                 string.Concat("'", tdt[i].trans_type, "'"),
                                 string.Concat("'", tdt[i].trans_mode, "'"),
                                 string.Concat("'", tdt[i].amount, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].instrument_num, "'"),
                                 string.Concat("'", tdt[i].paid_to, "'"),
                                 string.Concat("'", tdt[i].token_num, "'"),
                                 string.Concat("'", tdt[i].created_by, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].modified_by, "'"),
                                 string.Concat("sysdate"),
                                 //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].approval_status, "'"),
                                 string.Concat("'", tdt[i].approved_by, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].particulars, "'"),
                                 string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].tr_acc_num, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].voucher_id, "'"),
                                 string.Concat("'", tdt[i].trf_type, "'"),
                                 string.Concat("'", tdt[i].tr_acc_cd, "'"),
                                 string.Concat("'", tdt[i].acc_cd, "'"),
                                 string.Concat("'", tdt[i].share_amt, "'"),
                                 string.Concat("'", tdt[i].sum_assured, "'"),
                                 string.Concat("'", tdt[i].paid_amt, "'"),
                                 string.Concat("'", tdt[i].curr_prn_recov, "'"),
                                 string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                                 string.Concat("'", tdt[i].curr_intt_recov, "'"),
                                 string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                                 string.Concat("'", tdt[i].remarks, "'"),
                                 string.Concat("'", tdt[i].crop_cd, "'"),
                                 string.Concat("'", tdt[i].activity_cd, "'"),
                                 string.Concat("'", tdt[i].curr_intt_rate, "'"),
                                 string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                                 string.Concat("'", tdt[i].instl_no, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].periodicity, "'"),
                                 string.Concat("'", tdt[i].disb_id, "'"),
                                 string.Concat("'", tdt[i].comp_unit_no, "'"),
                                 string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                                 string.Concat("'", tdt[i].mis_advance_recov, "'"),
                                 string.Concat("'", tdt[i].audit_fees_recov, "'"),
                                 string.Concat("'", tdt[i].sector_cd, "'"),
                                 string.Concat("'", tdt[i].spl_prog_cd, "'"),
                                 string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat("'", tdt[i].brn_cd, "'"),
                                 string.Concat("'", tdt[i].penal_intt_recov, "'"),
                                 string.Concat("'", tdt[i].adv_prn_recov, "'"),
                                 string.Concat("'", tdt[i].ardb_cd, "'"),
                                 string.Concat("'", tdt[i].brn_cd, "'"),
                                 string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                 string.Concat(tdt[i].trans_cd),
                                 string.Concat("'", tdt[i].acc_type_cd, "'"),
                                 string.Concat("'", tdt[i].acc_num, "'")
                                 );
                }
                else
                {
                    _statement = string.Format(_queryins,
                            string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            tdt[i].trans_cd != 0 ? Convert.ToString(tdt[i].trans_cd) : string.Concat("null"),
                            string.Concat("'", tdt[i].acc_type_cd, "'"),
                            string.Concat("'", tdt[i].acc_num, "'"),
                            string.Concat("'", tdt[i].trans_type, "'"),
                            string.Concat("'", tdt[i].trans_mode, "'"),
                            string.Concat("'", tdt[i].amount, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].instrument_num, "'"),
                            string.Concat("'", tdt[i].paid_to, "'"),
                            string.Concat("'", tdt[i].token_num, "'"),
                            string.Concat("'", tdt[i].created_by, "'"),
                            string.Concat("sysdate"),
                            //string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].modified_by, "'"),
                            string.Concat("sysdate"),
                            //string.IsNullOrWhiteSpace(tdt[i].modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].approval_status, "'"),
                            string.Concat("'", tdt[i].approved_by, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].particulars, "'"),
                            string.Concat("'", tdt[i].tr_acc_type_cd, "'"),
                            string.Concat("'", tdt[i].tr_acc_num, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].voucher_id, "'"),
                            string.Concat("'", tdt[i].trf_type, "'"),
                            string.Concat("'", tdt[i].tr_acc_cd, "'"),
                            string.Concat("'", tdt[i].acc_cd, "'"),
                            string.Concat("'", tdt[i].share_amt, "'"),
                            string.Concat("'", tdt[i].sum_assured, "'"),
                            string.Concat("'", tdt[i].paid_amt, "'"),
                            string.Concat("'", tdt[i].curr_prn_recov, "'"),
                            string.Concat("'", tdt[i].ovd_prn_recov, "'"),
                            string.Concat("'", tdt[i].curr_intt_recov, "'"),
                            string.Concat("'", tdt[i].ovd_intt_recov, "'"),
                            string.Concat("'", tdt[i].remarks, "'"),
                            string.Concat("'", tdt[i].crop_cd, "'"),
                            string.Concat("'", tdt[i].activity_cd, "'"),
                            string.Concat("'", tdt[i].curr_intt_rate, "'"),
                            string.Concat("'", tdt[i].ovd_intt_rate, "'"),
                            string.Concat("'", tdt[i].instl_no, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].periodicity, "'"),
                            string.Concat("'", tdt[i].disb_id, "'"),
                            string.Concat("'", tdt[i].comp_unit_no, "'"),
                            string.Concat("'", tdt[i].ongoing_unit_no, "'"),
                            string.Concat("'", tdt[i].mis_advance_recov, "'"),
                            string.Concat("'", tdt[i].audit_fees_recov, "'"),
                            string.Concat("'", tdt[i].sector_cd, "'"),
                            string.Concat("'", tdt[i].spl_prog_cd, "'"),
                            string.Concat("'", tdt[i].borrower_cr_cd, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                            string.Concat("'", tdt[i].brn_cd, "'"),
                            string.Concat("'", tdt[i].ardb_cd, "'"),
                            string.Concat("'", tdt[i].penal_intt_recov, "'"),
                            string.Concat("'", tdt[i].adv_prn_recov, "'"),
                            string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )")
                            );

                }
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }

            }
            return true;
        }



        public bool UpdateDepTrans(DbConnection connection, td_def_trans_trf tdt)
        {
            string _query = "UPDATE TD_DEP_TRANS SET "
         + " TRANS_DT               =NVL({0},TRANS_DT       ),"
         + " TRANS_CD               =NVL({1},TRANS_CD       ),"
         + " ACC_TYPE_CD            =NVL({2},ACC_TYPE_CD    ),"
         + " ACC_NUM                =NVL({3},ACC_NUM        ),"
         + " TRANS_TYPE             =NVL({4},TRANS_TYPE     ),"
         + " TRANS_MODE             =NVL({5},TRANS_MODE     ),"
         + " AMOUNT                 =NVL({6},AMOUNT         ),"
         + " INSTRUMENT_DT          =NVL({7},INSTRUMENT_DT  ),"
         + " INSTRUMENT_NUM         =NVL({8},INSTRUMENT_NUM ),"
         + " PAID_TO                =NVL({9},PAID_TO        ),"
         + " TOKEN_NUM              =NVL({10},TOKEN_NUM      ),"
         + " CREATED_BY             =NVL({11},CREATED_BY     ),"
         + " CREATED_DT             =NVL({12},CREATED_DT     ),"
         + " MODIFIED_BY            =NVL({13},MODIFIED_BY    ),"
         + " MODIFIED_DT            =NVL({14},MODIFIED_DT    ),"
         + " APPROVAL_STATUS        =NVL({15},APPROVAL_STATUS),"
         + " APPROVED_BY            =NVL({16},APPROVED_BY    ),"
         + " APPROVED_DT            =NVL({17},APPROVED_DT    ),"
         + " PARTICULARS            =NVL({18},PARTICULARS    ),"
         + " TR_ACC_TYPE_CD         =NVL({19},TR_ACC_TYPE_CD ),"
         + " TR_ACC_NUM             =NVL({20},TR_ACC_NUM     ),"
         + " VOUCHER_DT             =NVL({21},VOUCHER_DT     ),"
         + " VOUCHER_ID             =NVL({22},VOUCHER_ID     ),"
         + " TRF_TYPE               =NVL({23},TRF_TYPE       ),"
         + " TR_ACC_CD              =NVL({24},TR_ACC_CD      ),"
         + " ACC_CD                 =NVL({25},ACC_CD         ),"
         + " SHARE_AMT              =NVL({26},SHARE_AMT      ),"
         + " SUM_ASSURED            =NVL({27},SUM_ASSURED    ),"
         + " PAID_AMT               =NVL({28},PAID_AMT       ),"
         + " CURR_PRN_RECOV         =NVL({29},CURR_PRN_RECOV ),"
         + " OVD_PRN_RECOV          =NVL({30},OVD_PRN_RECOV  ),"
         + " CURR_INTT_RECOV        =NVL({31},CURR_INTT_RECOV),"
         + " OVD_INTT_RECOV         =NVL({32},OVD_INTT_RECOV ),"
         + " REMARKS                =NVL({33},REMARKS        ),"
         + " CROP_CD                =NVL({34},CROP_CD        ),"
         + " ACTIVITY_CD            =NVL({35},ACTIVITY_CD    ),"
         + " CURR_INTT_RATE         =NVL({36},CURR_INTT_RATE ),"
         + " OVD_INTT_RATE          =NVL({37},OVD_INTT_RATE  ),"
         + " INSTL_NO               =NVL({38},INSTL_NO       ),"
         + " INSTL_START_DT         =NVL({39},INSTL_START_DT ),"
         + " PERIODICITY            =NVL({40},PERIODICITY    ),"
         + " DISB_ID                =NVL({41},DISB_ID        ),"
         + " COMP_UNIT_NO           =NVL({42},COMP_UNIT_NO   ),"
         + " ONGOING_UNIT_NO        =NVL({43},ONGOING_UNIT_NO),"
         + " MIS_ADVANCE_RECOV      =NVL({44},MIS_ADVANCE_RECOV),"
         + " AUDIT_FEES_RECOV       =NVL({45},AUDIT_FEES_RECOV),"
         + " SECTOR_CD              =NVL({46},SECTOR_CD      ),"
         + " SPL_PROG_CD            =NVL({47},SPL_PROG_CD    ),"
         + " BORROWER_CR_CD         =NVL({48},BORROWER_CR_CD ),"
         + " INTT_TILL_DT           =NVL({49},INTT_TILL_DT   ),"
         + " BRN_CD                 =NVL({50},BRN_CD         ),"
         + " PENAL_INTT_RECOV       =NVL({51},PENAL_INTT_RECOV),"
         + " ADV_PRN_RECOV          =NVL({52},ADV_PRN_RECOV )"
                + " WHERE ARDB_CD = {53} AND BRN_CD = {54} AND "
                + " TRANS_DT = {55} AND  "
                + " TRANS_CD = {56} AND  "
                + "  DEL_FLAG='N' ";
            _statement = string.Format(_query,
                            string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd),
                             string.Concat("'", tdt.acc_type_cd, "'"),
                             string.Concat("'", tdt.acc_num, "'"),
                             string.Concat("'", tdt.trans_type, "'"),
                             string.Concat("'", tdt.trans_mode, "'"),
                             string.Concat("'", tdt.amount, "'"),
                             string.IsNullOrWhiteSpace(tdt.instrument_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instrument_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.instrument_num, "'"),
                             string.Concat("'", tdt.paid_to, "'"),
                             string.Concat("'", tdt.token_num, "'"),
                             string.Concat("'", tdt.created_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.modified_by, "'"),
                             string.Concat("sysdate"),
                             //string.IsNullOrWhiteSpace(tdt.modified_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.modified_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.approval_status, "'"),
                             string.Concat("'", tdt.approved_by, "'"),
                             string.IsNullOrWhiteSpace(tdt.approved_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.approved_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.particulars, "'"),
                             string.Concat("'", tdt.tr_acc_type_cd, "'"),
                             string.Concat("'", tdt.tr_acc_num, "'"),
                             string.IsNullOrWhiteSpace(tdt.voucher_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.voucher_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.voucher_id, "'"),
                             string.Concat("'", tdt.trf_type, "'"),
                             string.Concat("'", tdt.tr_acc_cd, "'"),
                             string.Concat("'", tdt.acc_cd, "'"),
                             string.Concat("'", tdt.share_amt, "'"),
                             string.Concat("'", tdt.sum_assured, "'"),
                             string.Concat("'", tdt.paid_amt, "'"),
                             string.Concat("'", tdt.curr_prn_recov, "'"),
                             string.Concat("'", tdt.ovd_prn_recov, "'"),
                             string.Concat("'", tdt.curr_intt_recov, "'"),
                             string.Concat("'", tdt.ovd_intt_recov, "'"),
                             string.Concat("'", tdt.remarks, "'"),
                             string.Concat("'", tdt.crop_cd, "'"),
                             string.Concat("'", tdt.activity_cd, "'"),
                             string.Concat("'", tdt.curr_intt_rate, "'"),
                             string.Concat("'", tdt.ovd_intt_rate, "'"),
                             string.Concat("'", tdt.instl_no, "'"),
                             string.IsNullOrWhiteSpace(tdt.instl_start_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.instl_start_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.periodicity, "'"),
                             string.Concat("'", tdt.disb_id, "'"),
                             string.Concat("'", tdt.comp_unit_no, "'"),
                             string.Concat("'", tdt.ongoing_unit_no, "'"),
                             string.Concat("'", tdt.mis_advance_recov, "'"),
                             string.Concat("'", tdt.audit_fees_recov, "'"),
                             string.Concat("'", tdt.sector_cd, "'"),
                             string.Concat("'", tdt.spl_prog_cd, "'"),
                             string.Concat("'", tdt.borrower_cr_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.intt_till_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.intt_till_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.Concat("'", tdt.penal_intt_recov, "'"),
                             string.Concat("'", tdt.adv_prn_recov, "'"),
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                            string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                             string.Concat(tdt.trans_cd)
                                             );
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }




        internal int DeleteLockerOpeningData(td_def_trans_trf acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.acc_num))
                        {
                            if (acc.trans_mode == "O")
                            {
                                DeleteTmlocker(connection, acc);
                                DeleteNominee(connection, acc);
                                DeleteAccholder(connection, acc);
                            }
                            
                            DeleteTransfer(connection, acc);

                            DeleteDepTransTrf(connection, acc);

                            DeleteDepTrans(connection, acc);
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


        internal bool DeleteTmlocker(DbConnection connection, td_def_trans_trf ind)
        {
            string _queryd = " UPDATE TM_LOCKER SET DEL_FLAG = 'Y'  "
                  + "WHERE ARDB_CD = {0} AND brn_cd = NVL({1}, brn_cd) AND agreement_no = NVL({2}, agreement_no ) AND DEL_FLAG='N'";

            _statement = string.Format(_queryd,
                                         !string.IsNullOrWhiteSpace(ind.ardb_cd) ? string.Concat("'", ind.ardb_cd, "'") : "ardb_cd",
                                         !string.IsNullOrWhiteSpace(ind.brn_cd) ? string.Concat("'", ind.brn_cd, "'") : "brn_cd",
                                         !string.IsNullOrWhiteSpace(ind.agreement_no) ? string.Concat("'", ind.agreement_no, "'") : "agreement_no"
                                      );

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;

        }


        internal bool DeleteNominee(DbConnection connection, td_def_trans_trf nom)
        {
            string _queryd = " UPDATE TD_NOMINEE  SET DEL_FLAG = 'Y' "
                         + " WHERE   ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2}  AND acc_type_cd = 34102 AND DEL_FLAG='N'";

            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(nom.ardb_cd) ? string.Concat("'", nom.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(nom.brn_cd) ? string.Concat("'", nom.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(nom.agreement_no) ? string.Concat("'", nom.agreement_no, "'") : "agreement_no"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }


        internal bool DeleteAccholder(DbConnection connection, td_def_trans_trf acc)
        {
            string _queryd = " UPDATE td_accholder   SET DEL_FLAG = 'Y' "
            + " WHERE   ARDB_CD = {0} AND brn_cd = {1} AND acc_num = {2} AND  ACC_TYPE_CD = 34102 AND DEL_FLAG = 'N' ";
            try
            {
                _statement = string.Format(_queryd,
                                     !string.IsNullOrWhiteSpace(acc.ardb_cd) ? string.Concat("'", acc.ardb_cd, "'") : "ardb_cd",
                                     !string.IsNullOrWhiteSpace(acc.brn_cd) ? string.Concat("'", acc.brn_cd, "'") : "brn_cd",
                                     !string.IsNullOrWhiteSpace(acc.agreement_no) ? string.Concat("'", acc.agreement_no, "'") : "agreement_no"
                                      );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }

            return true;
        }


        internal bool DeleteTransfer(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TM_TRANSFER SET DEL_FLAG = 'Y' "
              + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
               + " (TRF_DT = {2}) AND  "
              + " (  TRANS_CD = {3} ) AND (DEL_FLAG = 'N') ";
            try
            {
                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;

        }


        internal bool DeleteDepTransTrf(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TD_DEP_TRANS_TRF    SET DEL_FLAG = 'Y'  "
           + " WHERE (ARDB_CD = {0}) AND (BRN_CD = {1}) AND "
           + " (TRANS_DT = {2}) AND  "
           + " (  TRANS_CD = {3} )  AND DEL_FLAG='N' ";
            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }

        internal bool DeleteDepTrans(DbConnection connection, td_def_trans_trf tdt)
        {
            string _queryd = "UPDATE TD_DEP_TRANS     SET DEL_FLAG = 'Y'  "
                    + " WHERE ARDB_CD = {0} AND BRN_CD = {1} AND "
                + " TRANS_DT = {2} AND  "
                + " TRANS_CD = {3}  AND DEL_FLAG='N' ";
            try
            {

                _statement = string.Format(_queryd,
                             string.Concat("'", tdt.ardb_cd, "'"),
                             string.Concat("'", tdt.brn_cd, "'"),
                             string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                             string.Concat(tdt.trans_cd)
                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                int x = 0;
            }
            return true;
        }


        internal string ApproveLockerTranaction(p_gen_param pgp)
        {
            string _ret = null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        string _query1 = "UPDATE TD_DEP_TRANS SET "
                                      + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                      + " MODIFIED_DT            =SYSDATE,"
                                      + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                      + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                      + " APPROVED_DT            =SYSDATE"
                                      + " WHERE (ARDB_CD = {3}) AND (BRN_CD = {4}) AND "
                                      + " (TRANS_DT = to_date('{5}','dd-mm-yyyy' )) AND  "
                                      + " (  TRANS_CD = {6} ) ";
                        _statement = string.Format(_query1,
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", "A", "'"),
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", pgp.ardb_cd, "'"),
                                string.Concat("'", pgp.brn_cd, "'"),
                                string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                string.Concat(pgp.ad_trans_cd)
                                );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                        }
                        string _query2 = "UPDATE TD_DEP_TRANS_TRF SET "
                                  + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                  + " MODIFIED_DT            =SYSDATE,"
                                  + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                  + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                  + " APPROVED_DT            =SYSDATE"
                                  + " WHERE (ARDB_CD = {3}) AND (BRN_CD = {4}) AND "
                                  + " (TRANS_DT = to_date('{5}','dd-mm-yyyy' )) AND  "
                                  + " (  TRANS_CD = {6} ) ";
                        _statement = string.Format(_query2,
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", "A", "'"),
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", pgp.ardb_cd, "'"),
                                string.Concat("'", pgp.brn_cd, "'"),
                                string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                string.Concat(pgp.ad_trans_cd)
                                );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                        }
                        string _query = "UPDATE TM_TRANSFER SET "
                                  + " APPROVAL_STATUS        =NVL({0},APPROVAL_STATUS),"
                                  + " APPROVED_BY            =NVL({1},APPROVED_BY    ),"
                                  + " APPROVED_DT            =SYSDATE"
                                  + " WHERE (ARDB_CD = {2}) AND (BRN_CD = {3}) AND "
                                  + " (TRF_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                  + " (  TRANS_CD = {5} ) ";
                        _statement = string.Format(_query,
                                string.Concat("'", "A", "'"),
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", pgp.ardb_cd, "'"),
                                string.Concat("'", pgp.brn_cd, "'"),
                                string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                string.Concat(pgp.ad_trans_cd)
                                );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.ExecuteNonQuery();
                        }

                        DepTransactionDL _dl1 = new DepTransactionDL();
                        _ret = _dl1.P_LOCKER_UPDATE(connection, pgp);
                        if (_ret == "0")
                        {

                            transaction.Commit();
                            return "0";

                        }
                        else
                        {
                            transaction.Rollback();
                            return _ret;
                        }
                        //}
                        //else
                        //{
                        //    transaction.Rollback();
                        //    return _ret;
                        //}
                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return ex.Message.ToString();
                    }

                }
            }
        }


        internal List<locker_dtls_report> PopulateLockerDtlsRep(p_report_param prp)
        {
            List<locker_dtls_report> tcaRet = new List<locker_dtls_report>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
           
            string _query = " SELECT MM_LOCKER.LOCKER_TYPE, "
                                + "  MM_LOCKER.LOCKER_ID,       "
                                + "  MM_LOCKER.LOCKER_STATUS,    "
                                + "  TM_LOCKER.NAME, "
                                + "  TM_LOCKER.RENTED_TILL,        "
                                + "  TM_LOCKER.AGREEMENT_NO,       "
                                + "  TM_LOCKER.KEY_NO       "
                                + "  FROM MM_LOCKER,TM_LOCKER "
                                + "  WHERE (MM_LOCKER.ARDB_CD = TM_LOCKER.ARDB_CD) AND (MM_LOCKER.BRN_CD = TM_LOCKER.BRN_CD) AND (MM_LOCKER.LOCKER_ID = TM_LOCKER.LOCKER_ID) "
                                + "  AND (MM_LOCKER.ARDB_CD = {0})   "
                                + "  AND (MM_LOCKER.BRN_CD = {1})   "
                                + "  AND (TM_LOCKER.ACC_STATUS = 'O')   "
                                + "  AND (MM_LOCKER.LOCKER_STATUS = 'A')   "
                                + "  AND (TM_LOCKER.ACC_STATUS = 'O')   "
                                + "  UNION "
                                + "  SELECT MM_LOCKER.LOCKER_TYPE, "
                                + "  MM_LOCKER.LOCKER_ID,       "
                                + "  MM_LOCKER.LOCKER_STATUS,    "
                                + "  ' ' C, "
                                + " to_date('01/01/1900', 'dd/mm/yyyy'), "
                                + " '0' ,  "
                                + " '0'  "
                                + " FROM MM_LOCKER "
                                + " WHERE MM_LOCKER.LOCKER_STATUS = 'V' "
                                + "  AND (MM_LOCKER.ARDB_CD = {2})   "
                                + "  AND (MM_LOCKER.BRN_CD = {3})   ";
                                
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }
                        
                        _statement = string.Format(_query,
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.brn_cd, "'"),
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new locker_dtls_report();
                                        tca.locker_type = UtilityM.CheckNull<string>(reader["LOCKER_TYPE"]);
                                        tca.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                        tca.locker_status = UtilityM.CheckNull<string>(reader["LOCKER_STATUS"]);
                                        tca.name = UtilityM.CheckNull<string>(reader["NAME"]);
                                        tca.rented_till = UtilityM.CheckNull<DateTime>(reader["RENTED_TILL"]);
                                        tca.agreement_no = UtilityM.CheckNull<string>(reader["AGREEMENT_NO"]);
                                        tca.key_no = UtilityM.CheckNull<string>(reader["KEY_NO"]);                                        
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }   



        internal List<tm_locker> PopulateLockerHistoryRep(p_report_param prp)
        {
            List<tm_locker> tcaRet = new List<tm_locker>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            string _query = "  SELECT TM_LOCKER.AGREEMENT_NO, "
                                + "   TM_LOCKER.LOCKER_ID,      "
                                + "  TM_LOCKER.AGREEMENT_DT,    "
                                + "  TM_LOCKER.NAME, "
                                + "  TM_LOCKER.KEY_NO,        "
                                + "  TM_LOCKER.ACC_TYPE_CD,      "
                                + "  TM_LOCKER.ACC_NUM,   "
                                + "  TM_LOCKER.NAME, "
                                + "  TM_LOCKER.NARRATION, "
                                + "  TM_LOCKER.RENTED_TILL,   "
                                + "  TM_LOCKER.ACC_STATUS,   "
                                + "  MM_CUSTOMER.PRESENT_ADDRESS,   "
                                + "  MM_CUSTOMER.PHONE   "
                                + "  FROM TM_LOCKER,   "
                                + "  MM_CUSTOMER, "
                                + "  TM_DEPOSIT "
                                + "  WHERE (MM_CUSTOMER.ARDB_CD = TM_DEPOSIT.ARDB_CD) and (MM_CUSTOMER.BRN_CD = TM_DEPOSIT.BRN_CD)       "
                                + "  AND (TM_LOCKER.ARDB_CD = TM_DEPOSIT.ARDB_CD) AND (TM_LOCKER.ACC_TYPE_CD = TM_DEPOSIT.ACC_TYPE_CD)    "
                                + "  AND (TM_LOCKER.ACC_NUM = TM_DEPOSIT.ACC_NUM) "
                                + "  AND (TM_DEPOSIT.CUST_CD  = MM_CUSTOMER.CUST_CD) AND TM_DEPOSIT.ARDB_CD = {0} AND TM_DEPOSIT.BRN_CD = {1} ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tm_locker();
                                        tca.agreement_no = UtilityM.CheckNull<string>(reader["AGREEMENT_NO"]);
                                        tca.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                        tca.agreement_dt = UtilityM.CheckNull<DateTime>(reader["AGREEMENT_DT"]);
                                        tca.name = UtilityM.CheckNull<string>(reader["NAME"]);
                                        tca.key_no = UtilityM.CheckNull<string>(reader["KEY_NO"]);
                                        tca.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);                                       
                                        tca.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tca.narration = UtilityM.CheckNull<string>(reader["NARRATION"]);
                                        tca.rented_till = UtilityM.CheckNull<DateTime>(reader["RENTED_TILL"]);                                        
                                        tca.acc_status = UtilityM.CheckNull<string>(reader["ACC_STATUS"]);
                                        tca.present_address = UtilityM.CheckNull<string>(reader["PRESENT_ADDRESS"]);
                                        tca.phone = UtilityM.CheckNull<string>(reader["PHONE"]);
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }


        internal List<tm_locker> PopulateLockerShouldBeRenewedRep(p_report_param prp)
        {
            List<tm_locker> tcaRet = new List<tm_locker>();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";

            string _query = "  SELECT TM_LOCKER.AGREEMENT_NO, "
                                + "   TM_LOCKER.LOCKER_ID,      "
                                + "  TM_LOCKER.NAME,    "
                                + "  TM_LOCKER.RENTED_TILL, "
                                + "  MM_CUSTOMER.CUST_CD,        "
                                + "  MM_CUSTOMER.PHONE      "
                                + "  FROM   TM_LOCKER, MM_CUSTOMER   "
                                + "  WHERE  TM_LOCKER.UCIC = MM_CUSTOMER.CUST_CD "
                                + "  AND TM_LOCKER.BRN_CD = MM_CUSTOMER.BRN_CD "
                                + "  AND TM_LOCKER.ARDB_CD = MM_CUSTOMER.ARDB_CD   "
                                + "  AND TM_LOCKER.RENTED_TILL<TO_DATE(F_GET_OPERATION_DATE({0}))   "
                                + "  AND TM_LOCKER.ARDB_CD= {1}   "
                                + "  AND TM_LOCKER.BRN_CD = {2}   "
                                + "  AND TM_LOCKER.ACC_STATUS = 'O'   "
                                + "  ORDER BY TM_LOCKER.RENTED_TILL ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                        _statement = string.Format(_query,
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.ardb_cd, "'"),
                                                    string.Concat("'", prp.brn_cd, "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        var tca = new tm_locker();
                                        tca.agreement_no = UtilityM.CheckNull<string>(reader["AGREEMENT_NO"]);
                                        tca.locker_id = UtilityM.CheckNull<string>(reader["LOCKER_ID"]);
                                        tca.name = UtilityM.CheckNull<string>(reader["NAME"]);
                                        tca.rented_till = UtilityM.CheckNull<DateTime>(reader["RENTED_TILL"]);
                                        tca.cust_cd = UtilityM.CheckNull<decimal>(reader["CUST_CD"]);
                                        tca.phone = UtilityM.CheckNull<string>(reader["PHONE"]);                                     
                                        tcaRet.Add(tca);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        tcaRet = null;
                    }
                }
            }
            return tcaRet;
        }






        internal List<td_def_trans_trf> GetUnapprovedLockerTrans(tm_locker tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = "SELECT  TRANS_DT,"
         + " TRANS_CD,"
         + " ACC_TYPE_CD,"
         + " ACC_NUM,"
         + " TRANS_TYPE,"
         + " TRANS_MODE,"
         + " AMOUNT,"
         + " INSTRUMENT_DT,"
         + " INSTRUMENT_NUM,"
         + " PAID_TO,"
         + " TOKEN_NUM,"
         + " CREATED_BY,"
         + " CREATED_DT,"
         + " MODIFIED_BY,"
         + " MODIFIED_DT,"
         + " APPROVAL_STATUS,"
         + " APPROVED_BY,"
         + " APPROVED_DT,"
         + " PARTICULARS,"
         + " TR_ACC_TYPE_CD,"
         + " TR_ACC_NUM,"
         + " VOUCHER_DT,"
         + " VOUCHER_ID,"
         + " TRF_TYPE,"
         + " TR_ACC_CD,"
         + " ACC_CD,"
         + " SHARE_AMT,"
         + " SUM_ASSURED,"
         + " PAID_AMT,"
         + " CURR_PRN_RECOV,"
         + " OVD_PRN_RECOV,"
         + " CURR_INTT_RECOV,"
         + " OVD_INTT_RECOV,"
         + " REMARKS,"
         + " CROP_CD,"
         + " ACTIVITY_CD,"
         + " CURR_INTT_RATE,"
         + " OVD_INTT_RATE,"
         + " INSTL_NO,"
         + " INSTL_START_DT,"
         + " PERIODICITY,"
         + " DISB_ID,"
         + " COMP_UNIT_NO,"
         + " ONGOING_UNIT_NO,"
         + " MIS_ADVANCE_RECOV,"
         + " AUDIT_FEES_RECOV,"
         + " SECTOR_CD,"
         + " SPL_PROG_CD,"
         + " BORROWER_CR_CD,"
         + " INTT_TILL_DT,"
         + " '' ACC_NAME ,"
         + " BRN_CD,ARDB_CD,DEL_FLAG,PENAL_INTT_RECOV,ADV_PRN_RECOV "
         + " FROM TD_DEP_TRANS"
         + " WHERE  ARDB_CD = {0} AND BRN_CD = {1} AND ACC_NUM = {2} AND  ACC_TYPE_CD = 34102 AND  NVL(APPROVAL_STATUS, 'U') = 'U' AND DEL_FLAG='N' ";
            _statement = string.Format(_query,
                                      !string.IsNullOrWhiteSpace(tdt.ardb_cd) ? string.Concat("'", tdt.ardb_cd, "'") : "ardb_cd",
                                      !string.IsNullOrWhiteSpace(tdt.brn_cd) ? string.Concat("'", tdt.brn_cd, "'") : "brn_cd",
                                      !string.IsNullOrWhiteSpace(tdt.agreement_no) ? string.Concat("'", tdt.agreement_no, "'") : "acc_num"
                                      );

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var tdtr = new td_def_trans_trf();
                                tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                tdtr.acc_type_cd = UtilityM.CheckNull<Int32>(reader["ACC_TYPE_CD"]);
                                tdtr.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                tdtr.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                tdtr.trans_mode = UtilityM.CheckNull<string>(reader["TRANS_MODE"]);
                                tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                tdtr.instrument_dt = UtilityM.CheckNull<DateTime>(reader["INSTRUMENT_DT"]);
                                tdtr.instrument_num = UtilityM.CheckNull<Int64>(reader["INSTRUMENT_NUM"]);
                                tdtr.paid_to = UtilityM.CheckNull<string>(reader["PAID_TO"]);
                                tdtr.token_num = UtilityM.CheckNull<string>(reader["TOKEN_NUM"]);
                                tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                tdtr.modified_by = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);
                                tdtr.modified_dt = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);
                                tdtr.approval_status = UtilityM.CheckNull<string>(reader["APPROVAL_STATUS"]);
                                tdtr.approved_by = UtilityM.CheckNull<string>(reader["APPROVED_BY"]);
                                tdtr.approved_dt = UtilityM.CheckNull<DateTime>(reader["APPROVED_DT"]);
                                tdtr.particulars = UtilityM.CheckNull<string>(reader["PARTICULARS"]);
                                tdtr.tr_acc_type_cd = UtilityM.CheckNull<Int32>(reader["TR_ACC_TYPE_CD"]);
                                tdtr.tr_acc_num = UtilityM.CheckNull<string>(reader["TR_ACC_NUM"]);
                                tdtr.voucher_dt = UtilityM.CheckNull<DateTime>(reader["VOUCHER_DT"]);
                                tdtr.voucher_id = UtilityM.CheckNull<decimal>(reader["VOUCHER_ID"]);
                                tdtr.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                tdtr.tr_acc_cd = UtilityM.CheckNull<int>(reader["TR_ACC_CD"]);
                                tdtr.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                tdtr.share_amt = UtilityM.CheckNull<decimal>(reader["SHARE_AMT"]);
                                tdtr.sum_assured = UtilityM.CheckNull<decimal>(reader["SUM_ASSURED"]);
                                tdtr.paid_amt = UtilityM.CheckNull<decimal>(reader["PAID_AMT"]);
                                tdtr.curr_prn_recov = UtilityM.CheckNull<decimal>(reader["CURR_PRN_RECOV"]);
                                tdtr.ovd_prn_recov = UtilityM.CheckNull<decimal>(reader["OVD_PRN_RECOV"]);
                                tdtr.curr_intt_recov = UtilityM.CheckNull<decimal>(reader["CURR_INTT_RECOV"]);
                                tdtr.ovd_intt_recov = UtilityM.CheckNull<decimal>(reader["OVD_INTT_RECOV"]);
                                tdtr.remarks = UtilityM.CheckNull<string>(reader["REMARKS"]);
                                tdtr.crop_cd = UtilityM.CheckNull<string>(reader["CROP_CD"]);
                                tdtr.activity_cd = UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);
                                tdtr.curr_intt_rate = UtilityM.CheckNull<double>(reader["CURR_INTT_RATE"]);
                                tdtr.ovd_intt_rate = UtilityM.CheckNull<double>(reader["OVD_INTT_RATE"]);
                                tdtr.instl_no = UtilityM.CheckNull<Int32>(reader["INSTL_NO"]);
                                tdtr.instl_start_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_START_DT"]);
                                tdtr.periodicity = UtilityM.CheckNull<Int16>(reader["PERIODICITY"]);
                                tdtr.disb_id = UtilityM.CheckNull<decimal>(reader["DISB_ID"]);
                                tdtr.comp_unit_no = UtilityM.CheckNull<decimal>(reader["COMP_UNIT_NO"]);
                                tdtr.ongoing_unit_no = UtilityM.CheckNull<decimal>(reader["ONGOING_UNIT_NO"]);
                                tdtr.mis_advance_recov = UtilityM.CheckNull<decimal>(reader["MIS_ADVANCE_RECOV"]);
                                tdtr.audit_fees_recov = UtilityM.CheckNull<decimal>(reader["AUDIT_FEES_RECOV"]);
                                tdtr.sector_cd = UtilityM.CheckNull<string>(reader["SECTOR_CD"]);
                                tdtr.spl_prog_cd = UtilityM.CheckNull<string>(reader["SPL_PROG_CD"]);
                                tdtr.borrower_cr_cd = UtilityM.CheckNull<string>(reader["BORROWER_CR_CD"]);
                                tdtr.intt_till_dt = UtilityM.CheckNull<DateTime>(reader["INTT_TILL_DT"]);
                                tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtr.ardb_cd = UtilityM.CheckNull<string>(reader["ARDB_CD"]);
                                tdtr.del_flag = UtilityM.CheckNull<string>(reader["DEL_FLAG"]);
                                tdtr.penal_intt_recov = UtilityM.CheckNull<decimal>(reader["PENAL_INTT_RECOV"]);
                                tdtr.adv_prn_recov = UtilityM.CheckNull<decimal>(reader["ADV_PRN_RECOV"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }

                }
            }
            return tdtRets;
        }




    }
}
