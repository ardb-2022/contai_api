using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class TransferDL
    {
        string _statement;
        AccountOpenDL _dac = new AccountOpenDL();
        DepTransactionDL _dep = new DepTransactionDL();
        internal TransferDM GetTransferData(td_def_trans_trf trf)
        {
            TransferDM TransferDMRet = new TransferDM();
            List<tm_transfer> tm_transferRet=new List<tm_transfer>();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        TransferDMRet.tddeftrans = _dep.GetDepTransSingle(connection, trf);
                        if (!String.IsNullOrWhiteSpace(trf.trans_cd.ToString()) && trf.trans_cd > 0)
                        {
                            tm_deposit td=new tm_deposit();
                            td.trans_cd = trf.trans_cd;
                            td.trans_dt = trf.trans_dt;
                            td.ardb_cd =  trf.ardb_cd;
                            td.brn_cd  = trf.brn_cd;
                            
                            tm_transferRet = _dac.GetTransfer(connection, td);
                            if (tm_transferRet.Count>0)
                            TransferDMRet.tmtransfer=tm_transferRet[0];
                            TransferDMRet.tddeftranstrf = _dac.GetDepTransTrf(connection, td);
                        }

                        return TransferDMRet;
                    }
                    catch (Exception ex)
                    {
                        // transaction.Rollback();
                        return null;
                    }

                }
            }
        }

        internal string InsertTransferData(TransferDM acc)
        {
            string _section = null;
           List<tm_transfer> tm_transferRet=new List<tm_transfer>();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        _section = "GetTransCDMaxId";
                        int maxTransCD = _dac.GetTransCDMaxId(connection, acc.tddeftrans);
                       _section = "InsertTransfer";
                       tm_transferRet.Add(acc.tmtransfer);
                        if (tm_transferRet.Count > 0)
                            _dac.InsertTransfer(connection, tm_transferRet, maxTransCD);
                        _section = "InsertDepTransTrf";
                        if (acc.tddeftranstrf.Count > 0)
                            _dac.InsertDepTransTrf(connection, acc.tddeftranstrf, maxTransCD);
                        _section = "InsertDepTrans";
                        if (!String.IsNullOrWhiteSpace(maxTransCD.ToString()) && maxTransCD != 0)
                            _dac.InsertDepTrans(connection, acc.tddeftrans, maxTransCD);
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

        internal int UpdateTransferData(TransferDM acc)
        {
             List<tm_transfer> tm_transferRet=new List<tm_transfer>();
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                         tm_transferRet.Add(acc.tmtransfer);
                        if (tm_transferRet.Count > 0)
                            _dac.UpdateTransfer(connection, tm_transferRet);
                        if (acc.tddeftranstrf.Count > 0)
                            _dac.UpdateDepTransTrf(connection, acc.tddeftranstrf);
                        if (!String.IsNullOrWhiteSpace(acc.tddeftrans.trans_cd.ToString()))
                            _dac.UpdateDepTrans(connection, acc.tddeftrans);
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

        internal int DeleteTransferData(td_def_trans_trf acc)
        {
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        if (!String.IsNullOrWhiteSpace(acc.trans_cd.ToString()))
                        {
                            _dac.DeleteTransfer(connection, acc);

                            _dac.DeleteDepTransTrf(connection, acc);

                            _dac.DeleteDepTrans(connection, acc);
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

         internal string ApproveTransfer(p_gen_param pgp)
        {
            string _ret = null;
            string _query3 = "P_TRANSFER";
            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        var updateTdDepTransSuccess = 0;
                        _statement = string.Format(_query3);
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm1.Value = pgp.ardb_cd;
                            command.Parameters.Add(parm1);
                            var parm2 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm2.Value = pgp.brn_cd;
                            command.Parameters.Add(parm2);
                            var parm3 = new OracleParameter("ad_trf_cd", OracleDbType.Int32, ParameterDirection.Input);
                            parm3.Value = pgp.ad_trans_cd;
                            command.Parameters.Add(parm3);
                            var parm4 = new OracleParameter("as_approved_by", OracleDbType.Varchar2, ParameterDirection.Input);
                            parm4.Value = pgp.gs_user_id;
                            command.Parameters.Add(parm4);
                            var parm5 = new OracleParameter("adt_approved_dt", OracleDbType.Date, ParameterDirection.Input);
                            parm5.Value = pgp.adt_trans_dt;
                            command.Parameters.Add(parm5);
                            updateTdDepTransSuccess=command.ExecuteNonQuery();

                        }
                        
                        
                        string _query1 = "UPDATE TD_DEP_TRANS SET "
                                               + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                               + " MODIFIED_DT            =SYSDATE,"
                                               + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                               + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                               + " APPROVED_DT            =SYSDATE"
                                               + " WHERE (BRN_CD = {3}) AND "
                                               + " (TRANS_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                               + " (  TRANS_CD = {5} ) AND ( ARDB_CD = {6} ) AND (DEL_FLAG='N') ";
                        _statement = string.Format(_query1,
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", "A", "'"),
                                string.Concat("'", pgp.gs_user_id, "'"),
                                string.Concat("'", pgp.brn_cd, "'"),
                                string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                string.Concat(pgp.ad_trans_cd),
                                string.Concat("'", pgp.ardb_cd, "'")
                                );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {
                            updateTdDepTransSuccess = command.ExecuteNonQuery();
                        }
                        if (updateTdDepTransSuccess > 0)
                        {
                            
                                    string _query2 = "UPDATE TD_DEP_TRANS_TRF SET "
                                              + " MODIFIED_BY            =NVL({0},MODIFIED_BY    ),"
                                              + " MODIFIED_DT            =SYSDATE,"
                                              + " APPROVAL_STATUS        =NVL({1},APPROVAL_STATUS),"
                                              + " APPROVED_BY            =NVL({2},APPROVED_BY    ),"
                                              + " APPROVED_DT            =SYSDATE"
                                              + " WHERE (BRN_CD = {3}) AND "
                                              + " (TRANS_DT = to_date('{4}','dd-mm-yyyy' )) AND  "
                                              + " (  TRANS_CD = {5}) AND ( ARDB_CD = {6} ) AND (DEL_FLAG = 'N') ";
                                    _statement = string.Format(_query2,
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", "A", "'"),
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", pgp.brn_cd, "'"),
                                            string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                            string.Concat(pgp.ad_trans_cd),
                                            string.Concat("'", pgp.ardb_cd, "'")
                                            );
                                    using (var command = OrclDbConnection.Command(connection, _statement))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    string _query = "UPDATE TM_TRANSFER SET "
                                              + " APPROVAL_STATUS        =NVL({0},APPROVAL_STATUS),"
                                              + " APPROVED_BY            =NVL({1},APPROVED_BY    ),"
                                              + " APPROVED_DT            =SYSDATE"
                                              + " WHERE (BRN_CD = {2}) AND "
                                              + " (TRF_DT = to_date('{3}','dd-mm-yyyy' )) AND  "
                                              + " (  TRANS_CD = {4} ) AND ( ARDB_CD = {5} ) AND (DEL_FLAG = 'N')";
                                    _statement = string.Format(_query,
                                            string.Concat("'", "A", "'"),
                                            string.Concat("'", pgp.gs_user_id, "'"),
                                            string.Concat("'", pgp.brn_cd, "'"),
                                            string.Concat(pgp.adt_trans_dt.ToString("dd/MM/yyyy")),
                                            string.Concat(pgp.ad_trans_cd),
                                            string.Concat("'", pgp.ardb_cd, "'")
                                            );
                                    using (var command = OrclDbConnection.Command(connection, _statement))
                                    {
                                        command.ExecuteNonQuery();
                                    }
                                    transaction.Commit();
                                    return "0";

                        }
                        else
                        {
                            transaction.Rollback();
                            return _ret;
                        }

                    }
                    catch (Exception ex)
                    {

                        transaction.Rollback();
                        return ex.Message.ToString();
                    }

                }
            }
        }
        
         internal List<tm_transfer> GetUnapproveTransfer(tm_transfer tdt)
        {
            List<tm_transfer> tdtRets=new List<tm_transfer>();
            string _query="SELECT  TRF_DT,"   
         +" TRF_CD,"   
         +" TRANS_CD,"   
         +" CREATED_BY,"   
         +" CREATED_DT,"   
         +" APPROVAL_STATUS,"   
         +" APPROVED_BY,"   
         +" APPROVED_DT,"   
         +" BRN_CD"  
    +" FROM TM_TRANSFER"  
    +" WHERE (BRN_CD = {0}) AND " 
    +" (TRF_DT = to_date('{1}','dd-mm-yyyy' )) AND  "
    +" (  APPROVAL_STATUS = 'U' ) AND (ARDB_CD = {2}) AND (DEL_FLAG = 'N') ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace( tdt.brn_cd) ? "brn_cd" : string.Concat("'",  tdt.brn_cd , "'"),
                                            tdt.trf_dt!= null ? tdt.trf_dt.Value.ToString("dd/MM/yyyy"): "trf_dt",
                                             string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'")
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
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }        
            internal List<tm_transfer> GetTransfer(tm_transfer tdt)
        {
            List<tm_transfer> tdtRets=new List<tm_transfer>();
            string _query="SELECT  TRF_DT,"   
         +" TRF_CD,"   
         +" TRANS_CD,"   
         +" CREATED_BY,"   
         +" CREATED_DT,"   
         +" APPROVAL_STATUS,"   
         +" APPROVED_BY,"   
         +" APPROVED_DT,"   
         +" BRN_CD"  
    +" FROM TM_TRANSFER"  
    +" WHERE (BRN_CD = {0}) AND " 
    +" (TRF_DT = to_date('{1}','dd-mm-yyyy' )) AND  "
    +" (  TRF_CD = {2} ) AND (ARDB_CD = {3}) AND (DEL_FLAG = 'N') ";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace( tdt.brn_cd) ? "brn_cd" : string.Concat("'",  tdt.brn_cd , "'"),
                                            tdt.trf_dt!= null ? tdt.trf_dt.Value.ToString("dd/MM/yyyy"): "trf_dt",
                                            tdt.trf_cd !=0 ? Convert.ToString(tdt.trf_cd) : "trf_cd",
                                            string.IsNullOrWhiteSpace(tdt.ardb_cd) ? "ardb_cd" : string.Concat("'", tdt.ardb_cd, "'")
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
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }        
    internal int InsertTransfer(List<tm_transfer> tdt)
    {
            int _ret=0;
            List<td_def_trans_trf> tdtRets=new List<td_def_trans_trf>();
            string _query="INSERT INTO TM_TRANSFER (TRF_DT,TRF_CD,TRANS_CD,CREATED_BY,"
                        +" CREATED_DT,APPROVAL_STATUS,APPROVED_BY,APPROVED_DT"
                        +" BRN_CD,ARDB_CD,DEL_FLAG)"
                        +" VALUES (to_date('{0}','dd-mm-yyyy'),{1},{2},{3},"
                        +" to_date('{4}','dd-mm-yyyy'),{5},{6},to_date('{7}','dd-mm-yyyy'),"
                        +" {8},{9},'N')";

            //int VoucherIdMax=GetTVoucherDtlsMaxId(tdt[0]);
            using (var connection = OrclDbConnection.NewConnection)
            {
                 
                using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                    for (int i=0;i<tdt.Count;i++)
                    {
                             _statement = string.Format(_query,
                                          string.Concat(tdt[i].trf_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat(tdt[i].trf_cd),
                                          string.Concat(tdt[i].trans_cd),
                                          string.Concat("'",tdt[i].created_by, "'"),
                                          string.Concat(tdt[i].created_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat("'",tdt[i].approval_status, "'"),
                                          string.Concat("'",tdt[i].approved_by, "'"),
                                          string.Concat(tdt[i].approved_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat("'", tdt[i].brn_cd, "'"),
                                          string.Concat("'", tdt[i].ardb_cd, "'")
                                          );

                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                            transaction.Commit();
                            _ret=0;
                        }
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
    internal int UpdateTransfer(List<tm_transfer> tdt)
        {
            int _ret=0;
            string _query="UPDATE TM_TRANSFER SET "
         +" TRF_DT               =NVL(to_date('{0}','dd-mm-yyyy'),TRF_DT       )," 
         +" TRF_CD               =NVL({1},TRF_CD       ),"    
         +" TRANS_CD               =NVL({2},TRANS_CD       ),"   
         +" CREATED_BY             =NVL({3},CREATED_BY     ),"   
         +" CREATED_DT             =NVL(to_date('{4}','dd-mm-yyyy'),CREATED_DT     ),"   
         +" APPROVAL_STATUS        =NVL({5},APPROVAL_STATUS),"   
         +" APPROVED_BY            =NVL({6},APPROVED_BY    ),"   
         +" APPROVED_DT            =NVL(to_date('{7}','dd-mm-yyyy'),APPROVED_DT    ),"   
         +" BRN_CD                 =NVL({8},BRN_CD         )"
    +" WHERE (BRN_CD = {9}) AND " 
    +" (TRF_DT = to_date('{10}','dd-mm-yyyy' )) AND  "
    +" (  TRF_CD = {11} )  AND (ARDB_CD = {12}) AND (DEL_FLAG = 'N') ";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                    for (int i=0;i<tdt.Count;i++)
                    {
                             _statement = string.Format(_query,
                                          string.Concat(tdt[i].trf_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat(tdt[i].trf_cd),
                                          string.Concat(tdt[i].trans_cd),
                                          string.Concat("'",tdt[i].created_by, "'"),
                                          string.Concat(tdt[i].created_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat("'",tdt[i].approval_status, "'"),
                                          string.Concat("'",tdt[i].approved_by, "'"),
                                          string.Concat(tdt[i].approved_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat("'", tdt[i].brn_cd, "'"),
                                          string.Concat("'", tdt[i].brn_cd, "'"),
                                          string.Concat(tdt[i].trf_dt.Value.ToString("dd/MM/yyyy")),
                                          string.Concat(tdt[i].trf_cd),
                                          string.Concat("'", tdt[i].ardb_cd, "'")
                                          );
                        using (var command = OrclDbConnection.Command(connection, _statement))
                        {                   
                            command.ExecuteNonQuery();
                             transaction.Commit();
                            _ret=0;
                        }
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
    internal int DeleteTransfer(tm_transfer tdt)
        {
            int _ret=0;
            string _query="UPDATE TM_TRANSFER SET DEL_FLAG = 'Y'  "
    +" WHERE (BRN_CD = {0}) AND " 
    +" (TRF_DT = {1}) AND  "
    +" (  TRF_CD = {2} ) AND (ARDB_CD = {3}) AND DEL_FLAG = 'N' ";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                             _statement = string.Format(_query,
                                          string.Concat("'", tdt.brn_cd, "'"),
                                          string.IsNullOrWhiteSpace(tdt.trf_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trf_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                          string.Concat(tdt.trf_cd),
                                          string.Concat("'", tdt.ardb_cd, "'")
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

        internal List<td_def_trans_trf> GetDepTransTrfwithChild(td_def_trans_trf tdt)
        {
            List<td_def_trans_trf> tdtRets = new List<td_def_trans_trf>();
            string _query = " SELECT a.brn_cd,a.trans_dt,a.trans_cd,a.acc_type_cd,a.ACC_NUM,b.acc_type_desc,c.ACC_NAME ,(SELECT D.CUST_NAME FROM MM_CUSTOMER D,TM_DEPOSIT E WHERE D.ARDB_CD = E.ARDB_CD AND D.CUST_CD=E.CUST_CD AND E.ACC_TYPE_CD = A.ACC_TYPE_CD AND E.ACC_NUM = A.ACC_NUM ) CUST_NAME,A.amount "
                          + " FROM TD_DEP_TRANS_TRF A, "
                          + " MM_ACC_TYPE B, "
                          + " M_ACC_MASTER C "
                          + " WHERE  (A.ARDB_CD = {0}) AND (A.BRN_CD = {1}) AND "
                          + " (A.TRANS_DT = to_date('{2}','dd-mm-yyyy' )) AND  "
                          + " (  A.TRANS_CD = {3} )   "
                          + " AND A.ACC_TYPE_CD=B.ACC_TYPE_CD (+) "
                          + " AND A.ACC_TYPE_CD=C.ACC_CD(+) "
                          + " AND A.ARDB_CD=C.ARDB_CD(+)  AND A.DEL_FLAG = 'N' ";
            using (var connection = OrclDbConnection.NewConnection)
            {

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
                                tdtr.amount = UtilityM.CheckNull<double>(reader["AMOUNT"]);
                                tdtr.remarks = UtilityM.CheckNull<string>(reader["acc_type_desc"]);
                                tdtr.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                tdtr.created_by = UtilityM.CheckNull<string>(reader["CUST_NAME"]);
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }


        internal mm_dashboard GetDashBoardInfo(p_gen_param td)
        {
            mm_dashboard tdtr=new mm_dashboard();
            string _alter = "ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query= " SELECT F_GET_OPENING_GL_BAL ({0},{1},21101) TodaysOpening,"
       +" F_GET_CASH_RECEIVED({2},{3}) CashReceived,"
       +" F_GET_CASH_PAYMENT({4},{5}) CashPaid,"
       +" F_GET_CLOSING_GL_BAL({6},{7},21101) TodayClosing,"
       +" F_COUNT_OPENED_ACCOUNT({8},{9}) Accountopened,"
       +" F_GET_OPENED_ACCOUNT_SUM({10},{11}) AccountopenedAmount,"
       +" F_COUNT_CLOSED_ACCOUNT({12},{13}) TodayMaturity,"
       +" F_GET_CLOSED_ACCOUNT_SUM({14},{15}) TodayMaturityAmount,"
       +" F_COUNT_DISB({16},{17}) LoanDisbursed,"
       +" F_GET_DISB_SUM({18},{19}) LoanDisbursedAmount,"
       +" F_COUNT_RECOV({20},{21}) LoanRecovered,"
       +" F_GET_RECOV_SUM({22},{23}) LoanRecoveredAmount FROM DUAL";
            using (var connection = OrclDbConnection.NewConnection)
            {              
             
              using (var command = OrclDbConnection.Command(connection, _alter))
                        {
                            command.ExecuteNonQuery();
                        }

                _statement = string.Format(_query,                
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'"),
                                            string.Concat("'",  td.ardb_cd, "'"),
                                            string.Concat("'",  td.brn_cd, "'")
                                            );

                // _statement = string.Format(_query,
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'"),
                //                             string.IsNullOrWhiteSpace( td.brn_cd) ? "101" : string.Concat("'",  td.brn_cd , "'")
                //                             );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                               
                                 tdtr.TodaysOpening = UtilityM.CheckNull<string>(reader["TodaysOpening"]);   
                                 tdtr.CashReceived = UtilityM.CheckNull<string>(reader["CashReceived"]);   
                                 tdtr.CashPaid = UtilityM.CheckNull<string>(reader["CashPaid"]);   
                                 tdtr.TodayClosing = UtilityM.CheckNull<string>(reader["TodayClosing"]);    
                                 tdtr.Accountopened = UtilityM.CheckNull<string>(reader["Accountopened"]); 
                                 tdtr.AccountopenedAmount = UtilityM.CheckNull<string>(reader["AccountopenedAmount"]);   
                                 tdtr.TodayMaturity = UtilityM.CheckNull<string>(reader["TodayMaturity"]);   
                                 tdtr.TodayMaturityAmount = UtilityM.CheckNull<string>(reader["TodayMaturityAmount"]); 
                                 tdtr.LoanDisbursed = UtilityM.CheckNull<string>(reader["LoanDisbursed"]); 
                                 tdtr.LoanDisbursedAmount = UtilityM.CheckNull<string>(reader["LoanDisbursedAmount"]); 
                                 tdtr.LoanRecovered = UtilityM.CheckNull<string>(reader["LoanRecovered"]); 
                                 tdtr.LoanRecoveredAmount = UtilityM.CheckNull<string>(reader["LoanRecoveredAmount"]); 
                             }
                        }
                    }
                }
            }
            return tdtr;
        }
        
    }
}