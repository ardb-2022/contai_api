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
    public class KccMstDL
    {
        string _statement;
        internal string InsertKccData(KccMstDM acc)
        {
            string _section=null;

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.mmkccmemberdtls.member_id.ToString()))
                        InsertKccMemberDtls(connection, acc.mmkccmemberdtls);
                        if (acc.mmlandregister.Count>0)
                        InsertLandRegister(connection, acc.mmlandregister);
                        if (acc.tdkccsanctiondtls.Count>0)
                        InsertKccSanctionDtls(connection, acc.tdkccsanctiondtls);
                        transaction.Commit();
                        return acc.mmkccmemberdtls.member_id.ToString();
                    }
                    catch (Exception ex)
                    {
                        
                        transaction.Rollback();
                        return _section+ " : "+ex.Message;
                    }

                }
            }
        }
        internal int UpdateKccData(KccMstDM acc) 
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.mmkccmemberdtls.member_id.ToString()))
                            UpdateKccMemberDtls(connection, acc.mmkccmemberdtls);
                        if (acc.mmlandregister.Count>0)
                            UpdateLandRegister(connection, acc.mmlandregister);
                        if (acc.tdkccsanctiondtls.Count>0)
                            UpdateKccSanctionDtls(connection, acc.tdkccsanctiondtls);
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
        internal int DeleteKccData(mm_kcc_member_dtls acc) 
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.member_id.ToString()))
                        {
                            DeleteKccMemberDtls(connection, acc);

                            DeleteLandRegister(connection, acc);

                            DeleteKccSanctionDtls(connection, acc);
                       
                            
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
        internal KccMstDM GetKccData(mm_kcc_member_dtls td)
        {
            KccMstDM KccDMRet = new KccMstDM();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        KccDMRet.mmkccmemberdtls = GetKccMemberDtls(connection, td);
                        KccDMRet.mmlandregister = GetLandRegister(connection, td);
                        KccDMRet.tdkccsanctiondtls = GetKccSanctionDtls(connection, td);
                        return KccDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }


        internal mm_kcc_member_dtls GetKccMemberDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            mm_kcc_member_dtls depRet = new mm_kcc_member_dtls();
            string _query = " SELECT MEMBER_ID, "   
+" BANK_MEMBER_ID, "   
+" MEMBER_NAME, "   
+" KCC_NO, "   
+" MEMO_NO, "   
+" KCC_ACC_NO, "   
+" LAND_QTY, "
+" LAND_VALUATION, "   
+" CREATED_BY, "   
+" CREATED_DT, "   
+" MODIFIED_BY, "   
+" MODIFIED_DT, "   
+" KARBANAMA_NO, "   
+" MORTGAGE_DT, "   
+" M_LAND_QTY, "   
+" M_LAND_VAL, "   
+" KARBANNAMA_VALIDITY_DT, "
+" BSBD_NO "  
+" FROM MM_KCC_MEMBER_DTLS "
+" WHERE MEMBER_ID = {0} " ;

            _statement = string.Format(_query,
                                          dep.member_id != 0 ? Convert.ToString(dep.member_id) : "MEMBER_ID"
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
                                var d = new mm_kcc_member_dtls();
                                d.member_id               = UtilityM.CheckNull<decimal>(reader["MEMBER_ID"]);   
d.bank_member_id          = UtilityM.CheckNull<string>(reader["BANK_MEMBER_ID"]);   
d.member_name             = UtilityM.CheckNull<string>(reader["MEMBER_NAME"]);   
d.kcc_no                  = UtilityM.CheckNull<string>(reader["KCC_NO"]);   
d.memo_no                 = UtilityM.CheckNull<string>(reader["MEMO_NO"]);   
d.kcc_acc_no              = UtilityM.CheckNull<string>(reader["KCC_ACC_NO"]);   
d.land_qty                = UtilityM.CheckNull<decimal>(reader["LAND_QTY"]);   
d.land_valuation          = UtilityM.CheckNull<decimal>(reader["LAND_VALUATION"]);   
d.created_by              = UtilityM.CheckNull<string>(reader["CREATED_BY"]);   
d.created_dt              = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);   
d.modified_by             = UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);   
d.modified_dt             = UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);   
d.karbanama_no            = UtilityM.CheckNull<string>(reader["KARBANAMA_NO"]);   
d.mortgage_dt             = UtilityM.CheckNull<DateTime>(reader["MORTGAGE_DT"]);   
d.m_land_qty              = UtilityM.CheckNull<string>(reader["M_LAND_QTY"]);   
d.m_land_val              = Convert.ToDecimal(UtilityM.CheckNull<double>(reader["M_LAND_VAL"]));   
d.karbannama_validity_dt  = UtilityM.CheckNull<DateTime>(reader["KARBANNAMA_VALIDITY_DT"]);
d.bsbd_no                 = UtilityM.CheckNull<string>(reader["BSBD_NO"]);
depRet = d;
                            }
                        }
                    }
                }
            }
            return depRet;
        }
        internal List<mm_land_register> GetLandRegister(DbConnection connection, mm_kcc_member_dtls dep)
        {
            List<mm_land_register> indList = new List<mm_land_register>();
            string _query = " SELECT CUST_CD, "   
+" DAG_NO,        "
+" TYPE,          "
+" MOUZA_NAME,    "
+" KHATIAN_NO,    "
+" PLOT_NO,       "
+" LAND_AREA,     "
+" LF_NO         "
+" FROM MM_LAND_REGISTER " 
+" WHERE CUST_CD = {0}   ";

            _statement = string.Format(_query,
                                          dep.member_id != 0 ? Convert.ToString(dep.member_id) : "CUST_CD"
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
                                var i = new mm_land_register();
                                i.cust_cd=UtilityM.CheckNull<Int64>(reader["CUST_CD"]);   
         i.dag_no=UtilityM.CheckNull<string>(reader["DAG_NO"]);
         i.type=UtilityM.CheckNull<string>(reader["TYPE"]);
         i.mouza_name=UtilityM.CheckNull<string>(reader["MOUZA_NAME"]);
         i.khatian_no=UtilityM.CheckNull<string>(reader["KHATIAN_NO"]);
         i.plot_no=UtilityM.CheckNull<string>(reader["PLOT_NO"]);
         i.land_area=Convert.ToDecimal(UtilityM.CheckNull<double>(reader["LAND_AREA"]));
         i.lf_no=UtilityM.CheckNull<string>(reader["LF_NO"]);
                                indList.Add(i);
                            }
                        }
                    }
                }
            }

            return indList;
        }
        internal List<td_kcc_sanction_dtls> GetKccSanctionDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            List<td_kcc_sanction_dtls> nomList = new List<td_kcc_sanction_dtls>();

            string _query = " SELECT MEMBER_ID, "   
+" ACTIVITY_CD, "   
+" CROP_CD, "   
+" SANCTION_AMT, "   
+" EFFECTIVE_DT, "   
+" CREATED_BY, "   
+" CREATED_DT, "   
+" MODIFIED_BY, "   
+" MODIFIED_DT, "   
+" SANCTION_DATE, "   
+" VALIDITY_DT, "   
+" CREDIT_LIMIT_NO "  
+" FROM TD_KCC_SANCTION_DTLS " 
+" WHERE MEMBER_ID = {0} " ;  
           
            _statement = string.Format(_query,
                                          dep.member_id != 0 ? Convert.ToString(dep.member_id) : "MEMBER_ID"
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
                                var n = new td_kcc_sanction_dtls();
                               n.member_id= UtilityM.CheckNull<decimal>(reader["MEMBER_ID"]);   
n.activity_cd=UtilityM.CheckNull<string>(reader["ACTIVITY_CD"]);   
n.crop_cd=UtilityM.CheckNull<string>(reader["CROP_CD"]);   
n.sanction_amt=UtilityM.CheckNull<decimal>(reader["SANCTION_AMT"]);   
n.effective_dt=UtilityM.CheckNull<DateTime>(reader["EFFECTIVE_DT"]);   
n.created_by=UtilityM.CheckNull<string>(reader["CREATED_BY"]);   
n.created_dt=UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);   
n.modified_by=UtilityM.CheckNull<string>(reader["MODIFIED_BY"]);   
n.modified_dt=UtilityM.CheckNull<DateTime>(reader["MODIFIED_DT"]);   
n.sanction_date=UtilityM.CheckNull<DateTime>(reader["SANCTION_DATE"]);  
n.validity_dt=UtilityM.CheckNull<DateTime>(reader["VALIDITY_DT"]);   
n.credit_limit_no=UtilityM.CheckNull<string>(reader["CREDIT_LIMIT_NO"]);
                                nomList.Add(n);
                            }
                        }
                    }
                }
            }
            return nomList;
        }
        internal bool InsertKccMemberDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            string _query = " INSERT INTO MM_KCC_MEMBER_DTLS(MEMBER_ID,BANK_MEMBER_ID,MEMBER_NAME,KCC_NO,MEMO_NO,KCC_ACC_NO, "
+" LAND_QTY,LAND_VALUATION,CREATED_BY,CREATED_DT,MODIFIED_BY,MODIFIED_DT,KARBANAMA_NO,MORTGAGE_DT,"
+" M_LAND_QTY,M_LAND_VAL,KARBANNAMA_VALIDITY_DT,BSBD_NO)                                          "
+" VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},SYSDATE,{9},SYSDATE,{10},{11},{12},{13},{14},{15}) ";

            _statement = string.Format(_query,
            string.Concat("'", dep.member_id      , "'"),       
string.Concat("'", dep.bank_member_id , "'"),       
string.Concat("'", dep.member_name    , "'"),       
string.Concat("'", dep.kcc_no         , "'"),       
string.Concat("'", dep.memo_no        , "'"),       
string.Concat("'", dep.kcc_acc_no     , "'"),       
string.Concat("'", dep.land_qty       , "'"),       
string.Concat("'", dep.land_valuation , "'"),       
string.Concat("'", dep.created_by     , "'"),       
string.Concat("'", dep.modified_by    , "'"),       
string.Concat("'", dep.karbanama_no   , "'"),       
string.IsNullOrWhiteSpace(dep.mortgage_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mortgage_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
string.Concat("'", dep.m_land_qty     , "'"),       
string.Concat("'", dep.m_land_val     , "'"),       
string.IsNullOrWhiteSpace(dep.karbannama_validity_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.karbannama_validity_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
string.Concat("'", dep.bsbd_no  , "'"));
            
            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool InsertLandRegister(DbConnection connection, List<mm_land_register> ind)
        {
            string _query = "INSERT INTO MM_LAND_REGISTER ( CUST_CD, DAG_NO, TYPE, MOUZA_NAME, KHATIAN_NO, PLOT_NO, LAND_AREA,LF_NO) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5} , {6} ,{7} ) ";
            for (int i = 0; i < ind.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", ind[i].cust_cd, "'"),
                                                       string.Concat("'", ind[i].dag_no, "'"),
                                                       string.Concat("'", ind[i].type, "'"),
                                                       string.Concat("'", ind[i].mouza_name, "'"),
                                                       string.Concat("'", ind[i].khatian_no, "'"),
                                                       string.Concat("'", ind[i].plot_no, "'"),
                                                       string.Concat("'", ind[i].land_area, "'"),
                                                        string.Concat("'", ind[i].lf_no, "'")
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }
            return true;
        }
        internal bool InsertKccSanctionDtls(DbConnection connection, List<td_kcc_sanction_dtls> nom)
        {
           
            string _query = "INSERT INTO TD_KCC_SANCTION_DTLS (MEMBER_ID,ACTIVITY_CD,CROP_CD,SANCTION_AMT,EFFECTIVE_DT,CREATED_BY, "
                           +" CREATED_DT,MODIFIED_BY,MODIFIED_DT,SANCTION_DATE,VALIDITY_DT,CREDIT_LIMIT_NO )"
                          + " VALUES( {0},{1},{2},{3},SYSDATE,{4},SYSDATE,{5},SYSDATE,{6},{7},{8} ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                _statement = string.Format(_query,
                                                  string.Concat("'", nom[i].member_id, "'"),
                                                  string.Concat("'", nom[i].activity_cd, "'"),
                                                  string.Concat("'", nom[i].crop_cd, "'"),
                                                  string.Concat("'", nom[i].sanction_amt, "'"),
                                                  string.Concat("'", nom[i].created_by, "'"),
                                                  string.Concat("'", nom[i].modified_by, "'"),
                                                  string.IsNullOrWhiteSpace(nom[i].sanction_date.ToString()) ? string.Concat("null") : string.Concat("to_date('", nom[i].sanction_date.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                                  string.IsNullOrWhiteSpace(nom[i].validity_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", nom[i].validity_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                                  string.Concat("'", nom[i].credit_limit_no, "'")
                                                   );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }
        internal bool UpdateKccMemberDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            string _query = " UPDATE MM_KCC_MEMBER_DTLS SET "
+" BANK_MEMBER_ID        = NVL({0}, BANK_MEMBER_ID        ),"  
+" MEMBER_NAME           = NVL({1}, MEMBER_NAME           ),"  
+" KCC_NO                = NVL({2}, KCC_NO                ),"  
+" MEMO_NO               = NVL({3}, MEMO_NO               ),"  
+" KCC_ACC_NO            = NVL({4}, KCC_ACC_NO            ),"  
+" LAND_QTY              = NVL({5}, LAND_QTY              ),"
+" LAND_VALUATION        = NVL({6}, LAND_VALUATION        ),"  
+" MODIFIED_BY           = NVL({7}, MODIFIED_BY           ),"  
+" MODIFIED_DT           = SYSDATE,"  
+" KARBANAMA_NO          = NVL({8}, KARBANAMA_NO          ),"  
+" MORTGAGE_DT           = NVL({9}, MORTGAGE_DT           ),"  
+" M_LAND_QTY            = NVL({10}, M_LAND_QTY            ),"  
+" M_LAND_VAL            = NVL({11}, M_LAND_VAL            ),"  
+" KARBANNAMA_VALIDITY_DT= NVL({12}, KARBANNAMA_VALIDITY_DT),"
+" BSBD_NO               = NVL({13}, BSBD_NO               )" 
+" WHERE MEMBER_ID = {14} " ;

             _statement = string.Format(_query,
string.Concat("'", dep.bank_member_id , "'"),       
string.Concat("'", dep.member_name    , "'"),       
string.Concat("'", dep.kcc_no         , "'"),       
string.Concat("'", dep.memo_no        , "'"),       
string.Concat("'", dep.kcc_acc_no     , "'"),       
string.Concat("'", dep.land_qty       , "'"),       
string.Concat("'", dep.land_valuation , "'"),       
string.Concat("'", dep.modified_by    , "'"),       
string.Concat("'", dep.karbanama_no   , "'"),       
string.IsNullOrWhiteSpace(dep.mortgage_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.mortgage_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
string.Concat("'", dep.m_land_qty     , "'"),       
string.Concat("'", dep.m_land_val     , "'"),       
string.IsNullOrWhiteSpace(dep.karbannama_validity_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", dep.karbannama_validity_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
string.Concat("'", dep.bsbd_no  , "'"),
string.Concat("'", dep.member_id , "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool UpdateLandRegister(DbConnection connection, List<mm_land_register> ind)
        {
            string _queryd=" DELETE FROM MM_LAND_REGISTER "
             +" WHERE CUST_CD = {0} ";

                   _statement = string.Format(_queryd,
                                          !string.IsNullOrWhiteSpace(ind[0].cust_cd.ToString()) ? string.Concat("'", ind[0].cust_cd.ToString(), "'") : "0"
                                         );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                           
                        }
                   string _query = "INSERT INTO MM_LAND_REGISTER ( CUST_CD, DAG_NO, TYPE, MOUZA_NAME, KHATIAN_NO, PLOT_NO, LAND_AREA,LF_NO) "
                         + " VALUES( {0},{1},{2},{3}, {4}, {5} , {6} ,{7} ) ";
            for (int i = 0; i < ind.Count; i++)
            {
                _statement = string.Format(_query,
                                                       string.Concat("'", ind[i].cust_cd, "'"),
                                                       string.Concat("'", ind[i].dag_no, "'"),
                                                       string.Concat("'", ind[i].type, "'"),
                                                       string.Concat("'", ind[i].mouza_name, "'"),
                                                       string.Concat("'", ind[i].khatian_no, "'"),
                                                       string.Concat("'", ind[i].plot_no, "'"),
                                                       string.Concat("'", ind[i].land_area, "'"),
                                                        string.Concat("'", ind[i].lf_no, "'")
                                                        );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();

                }
            }

            return true;
        }
        internal bool UpdateKccSanctionDtls(DbConnection connection, List<td_kcc_sanction_dtls> nom)
        {
            string _queryd=" DELETE FROM TD_KCC_SANCTION_DTLS "
                         +"  WHERE MEMBER_ID = {0} ";

                    _statement = string.Format(_queryd,
                                          nom[0].member_id != 0 ? Convert.ToString(nom[0].member_id) : "0"
                                           );
           
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                        }
                    string _query = "INSERT INTO TD_KCC_SANCTION_DTLS (MEMBER_ID,ACTIVITY_CD,CROP_CD,SANCTION_AMT,EFFECTIVE_DT,CREATED_BY, "
                           +" CREATED_DT,MODIFIED_BY,MODIFIED_DT,SANCTION_DATE,VALIDITY_DT,CREDIT_LIMIT_NO )"
                          + " VALUES( {0},{1},{2},{3},SYSDATE,{4},SYSDATE,{5},SYSDATE,{6},{7},{8} ) ";

            for (int i = 0; i < nom.Count; i++)
            {
                _statement = string.Format(_query,
                                                  string.Concat("'", nom[i].member_id, "'"),
                                                  string.Concat("'", nom[i].activity_cd, "'"),
                                                  string.Concat("'", nom[i].crop_cd, "'"),
                                                  string.Concat("'", nom[i].sanction_amt, "'"),
                                                  string.Concat("'", nom[i].created_by, "'"),
                                                  string.Concat("'", nom[i].modified_by, "'"),
                                                  string.IsNullOrWhiteSpace(nom[i].sanction_date.ToString()) ? string.Concat("null") : string.Concat("to_date('", nom[i].sanction_date.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                                  string.IsNullOrWhiteSpace(nom[i].validity_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", nom[i].validity_dt.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                                  string.Concat("'", nom[i].credit_limit_no, "'")
                                                   );

                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    command.ExecuteNonQuery();
                }
            }
            return true;
        }
        internal bool DeleteKccMemberDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            string _query = " DELETE FROM MM_KCC_MEMBER_DTLS "
                            +" WHERE MEMBER_ID = {0} " ;

             _statement = string.Format(_query,
                         string.Concat("'", dep.member_id , "'"));

            using (var command = OrclDbConnection.Command(connection, _statement))
            {
                command.ExecuteNonQuery();
            }
            return true;
        }
        internal bool DeleteLandRegister(DbConnection connection, mm_kcc_member_dtls dep)
        {
            string _queryd=" DELETE FROM MM_LAND_REGISTER "
             +" WHERE CUST_CD = {0} ";

                   _statement = string.Format(_queryd,
                         string.Concat("'", dep.member_id , "'"));

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                           
                        }                   
            
            return true;
        }
        internal bool DeleteKccSanctionDtls(DbConnection connection, mm_kcc_member_dtls dep)
        {
            string _queryd=" DELETE FROM TD_KCC_SANCTION_DTLS "
                         +"  WHERE MEMBER_ID = {0} ";

                     _statement = string.Format(_queryd,
                         string.Concat("'", dep.member_id , "'"));
           
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                        }
                    
            return true;
        }   
    }
}   
    
