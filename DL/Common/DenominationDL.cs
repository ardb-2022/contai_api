using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class DenominationDL
    {
        string _statement;
        internal List<tm_denomination_trans> GetDenominationDtls(tm_denomination_trans tdt)
        {
            List<tm_denomination_trans> tdtRets=new List<tm_denomination_trans>();
            string _query="SELECT TM_DENOMINATION_TRANS.BRN_CD,TM_DENOMINATION_TRANS.TRANS_DT,TM_DENOMINATION_TRANS.TRANS_CD,TM_DENOMINATION_TRANS.RUPEES,"  
                          +" TM_DENOMINATION_TRANS.COUNT,TM_DENOMINATION_TRANS.CREATED_DT,TM_DENOMINATION_TRANS.CREATED_BY,TM_DENOMINATION_TRANS.TOTAL"
                          +" FROM TM_DENOMINATION_TRANS WHERE  TM_DENOMINATION_TRANS.BRN_CD= {0}  AND TM_DENOMINATION_TRANS.TRANS_DT =  to_date('{1}','dd-mm-yyyy' ) AND"
                          +" TM_DENOMINATION_TRANS.TRANS_CD = {2}";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace( tdt.brn_cd) ? "brn_cd" : string.Concat("'",  tdt.brn_cd , "'"),
                                            tdt.trans_dt!= null ? tdt.trans_dt.Value.ToString("dd/MM/yyyy"): "trans_dt",
                                            tdt.trans_cd !=0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
                                            );
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                               var tdtr = new tm_denomination_trans();
                                tdtr.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                tdtr.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                tdtr.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                tdtr.rupees = UtilityM.CheckNull<Double>(reader["RUPEES"]);
                                tdtr.count = UtilityM.CheckNull<Int64>(reader["COUNT"]);
                                tdtr.created_dt = UtilityM.CheckNull<DateTime>(reader["CREATED_DT"]);
                                tdtr.created_by = UtilityM.CheckNull<string>(reader["CREATED_BY"]);
                                tdtr.total = UtilityM.CheckNull<Double>(reader["TOTAL"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }        

internal List<tt_denomination> GetDenomination()
        {
            List<tt_denomination> tdtRets=new List<tt_denomination>();
            string _query="SELECT RUPEES,VALUE FROM TT_DENOMINATION";
            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query);
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)     
                        {
                            while (reader.Read())
                            {                                
                               var tdtr = new tt_denomination();
                                tdtr.rupees = UtilityM.CheckNull<string>(reader["RUPEES"]);
                                tdtr.value = UtilityM.CheckNull<double>(reader["VALUE"]);
                                tdtRets.Add(tdtr);
                            }
                        }
                    }
                }
            }
            return tdtRets;
        }
    internal int InsertDenominationDtls(List<tm_denomination_trans> tdt)
    {
            int _ret=0;
            List<tm_denomination_trans> tdtRets=new List<tm_denomination_trans>();
            string _query="INSERT INTO TM_DENOMINATION_TRANS (BRN_CD, TRANS_DT, TRANS_CD, RUPEES, COUNT, TOTAL, CREATED_DT, CREATED_BY)"
                            +" VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})";

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
                                          string.Concat("'", tdt[i].brn_cd, "'"), 
                                          string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                          string.Concat(tdt[i].trans_cd),
                                          string.Concat(tdt[i].rupees),
                                          string.Concat(tdt[i].count),
                                          string.Concat(tdt[i].total),
                                          string.IsNullOrWhiteSpace(tdt[i].created_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].created_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                          string.Concat("'", tdt[i].created_by, "'")
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

    // internal int GetTVoucherDtlsMaxId(t_voucher_dtls tvd)
    //     {
    //         int maxVoucherId=0;
    //         string _query="Select Nvl(max(voucher_id) + 1, 1) max_voucher_id"
    //                         +" From   t_voucher_narration"
    //                         +" Where  voucher_dt =  to_date('{0}','dd-mm-yyyy' ) "
	//                         +" And    brn_cd = {1}";
    //         using (var connection = OrclDbConnection.NewConnection)
    //         {              
    //             _statement = string.Format(_query,
    //                                         tvd.voucher_dt!= null ? Convert.ToString(tvd.voucher_dt).Substring(0, 10): "cr_dt",
    //                                         string.IsNullOrWhiteSpace( tvd.brn_cd) ? "brn_cd" : string.Concat("'",  tvd.brn_cd , "'")
    //                                         );
    //             using (var command = OrclDbConnection.Command(connection, _statement))
    //             {
    //                 using (var reader = command.ExecuteReader())
    //                 {
    //                     if (reader.HasRows)     
    //                     {
    //                         while (reader.Read())
    //                         {                     
    //                            maxVoucherId = Convert.ToInt32(UtilityM.CheckNull<decimal>(reader["MAX_VOUCHER_ID"]));
    //                         }
    //                     }
    //                 }
    //             }            
    //         }

    //         return maxVoucherId;
    //     }

        internal int UpdateDenominationDtls(List<tm_denomination_trans> tdt)
        {
            int _ret=0;
            string _query="UPDATE TM_DENOMINATION_TRANS SET RUPEES = {0},COUNT = {1},TOTAL = {3} "
                            +" WHERE BRN_CD = {4} AND TRANS_DT = {5} AND TRANS_CD = {6} ";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                    for (int i=0;i<tdt.Count;i++)
                    {
                             _statement = string.Format(_query,
                                            string.Concat(tdt[i].rupees),
                                            string.Concat(tdt[i].count),
                                            string.Concat(tdt[i].total),
                                            string.IsNullOrWhiteSpace( tdt[i].brn_cd) ? "brn_cd" : string.Concat("'",  tdt[i].brn_cd , "'"),
                                            string.IsNullOrWhiteSpace(tdt[i].trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt[i].trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            tdt[i].trans_cd !=0 ? Convert.ToString(tdt[i].trans_cd) : "trans_cd"
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

        internal int DeleteDenominationDtls(tm_denomination_trans tdt)
        {
            int _ret=0;
            string _query="DELETE FROM TM_DENOMINATION_TRANS "
                            +" WHERE BRN_CD = {0} AND TRANS_DT = {1} AND TRANS_CD = {2} ";
            using (var connection = OrclDbConnection.NewConnection)
            {              
                 using (var transaction = connection.BeginTransaction())
                {               
                    try
                    {
                               _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace( tdt.brn_cd) ? "brn_cd" : string.Concat("'",  tdt.brn_cd , "'"),
                                            string.IsNullOrWhiteSpace(tdt.trans_dt.ToString()) ? string.Concat("null") : string.Concat("to_date('", tdt.trans_dt.Value.ToString("dd/MM/yyyy"), "','dd-mm-yyyy' )"),
                                            tdt.trans_cd !=0 ? Convert.ToString(tdt.trans_cd) : "trans_cd"
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

   internal string P_UPDATE_DENOMINATION(DbConnection connection,p_gen_param prp)
        {
            string _alter="ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY HH24:MI:SS'";
            string _query="P_UPDATE_DENOMINATION";
        
                    try{    
                            using (var command = OrclDbConnection.Command(connection, _alter))
                            {
                                    command.ExecuteNonQuery();
                             }     
                            _statement = string.Format(_query);
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    var parm1 = new OracleParameter("AS_BRN_CD", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm1.Value = prp.brn_cd;
                                    command.Parameters.Add(parm1);
                                    var parm2 = new OracleParameter("ADT_DT", OracleDbType.Date, ParameterDirection.Input);
                                    parm2.Value = prp.adt_trans_dt;
                                    command.Parameters.Add(parm2);
                                    var parm3 = new OracleParameter("AD_TRANS_CD", OracleDbType.Int16, ParameterDirection.Input);
                                    parm3.Value = prp.ad_trans_cd;
                                    command.Parameters.Add(parm3);
                                    var parm4 = new OracleParameter("FLAG", OracleDbType.Char, ParameterDirection.Input);
                                    parm4.Value = prp.flag;
                                    command.Parameters.Add(parm4);
                                    command.ExecuteNonQuery();
                                    return "0";
                            }                             
                        }
                        catch (Exception ex)
                        {
                            return ex.Message.ToString();
                        }
                
    }

    }
}