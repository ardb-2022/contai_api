using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class DayScrollBookDL
    {
        string _statement;
        //New Line
        internal List<tt_day_scroll> PopulateDayScrollBook(p_report_param prp)
        {

            List<tt_day_scroll> tdsRet = new List<tt_day_scroll>();
            string _query1="P_SCROLL_BK";

            string _query2= " SELECT TT_DAY_SCROLL.TRANS_DT,"
       +" TT_DAY_SCROLL.VOUCHER_ID,"
       +" TT_DAY_SCROLL.TRANS_CD,"
       +" TT_DAY_SCROLL.ACC_CD,"
       +" TT_DAY_SCROLL.ACC_NUM,"
       +" TT_DAY_SCROLL.CUST_NARRATION,"
       +" TT_DAY_SCROLL.TRANS_TYPE,"
       +" TT_DAY_SCROLL.TRF_TYPE,"
       +" TT_DAY_SCROLL.ACC_TYPE,"
       +" TT_DAY_SCROLL.CASH_RECP,"
       +" TT_DAY_SCROLL.TRF_RECP,"
       +" TT_DAY_SCROLL.CASH_PAY,"
       +" TT_DAY_SCROLL.TRF_PAY,"
       +" (SELECT ABS(NVL(TM_ACC_BALANCE.BALANCE_AMT, 0))"
       +"   FROM TM_ACC_BALANCE                          "
       + "  WHERE TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD = {1}     "
       + "    AND TM_ACC_BALANCE.BALANCE_DT =             "
       +"        (SELECT MAX(TM_ACC_BALANCE.BALANCE_DT)  "
       +"           FROM TM_ACC_BALANCE                  "
       + "          WHERE TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD = {1}     "
       + "            AND TM_ACC_BALANCE.BALANCE_DT <     "
       +"                TT_DAY_SCROLL.TRANS_DT)         "
       +"    AND TM_ACC_BALANCE.ACC_CD = TT_DAY_SCROLL.CASH_ACC_CD) CASH_OPNG_BAL, "
       +" (SELECT ABS(NVL(TM_ACC_BALANCE.BALANCE_AMT, 0))                          "
       +"   FROM TM_ACC_BALANCE                                                    "
       +"  WHERE TM_ACC_BALANCE.BALANCE_DT = TT_DAY_SCROLL.TRANS_DT                "
       + "    AND TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD = {1}                                       "
       + "    AND TM_ACC_BALANCE.BALANCE_DT BETWEEN to_date('{2}','dd-mm-yyyy' ) AND to_date('{3}','dd-mm-yyyy' ) "
       +"    AND TM_ACC_BALANCE.ACC_CD = TT_DAY_SCROLL.CASH_ACC_CD) CASH_CLOS_BAL, "
       +" TT_DAY_SCROLL.CASH_ACC_CD,                                               "
       +" (SELECT ABS(NVL(TM_ACC_BALANCE.BALANCE_AMT, 0))                          "
       +"    FROM TM_ACC_BALANCE                                                   "
       + "   WHERE TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD = {1}                                      "
       + "     AND TM_ACC_BALANCE.BALANCE_DT =                                      "
       +"         (SELECT MAX(TM_ACC_BALANCE.BALANCE_DT)                           "
       +"            FROM TM_ACC_BALANCE                                           "
       + "           WHERE TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD= {1} "
       + "             AND TM_ACC_BALANCE.BALANCE_DT <                              "
       +"                 TT_DAY_SCROLL.TRANS_DT)                                  "
       +"     AND TM_ACC_BALANCE.ACC_CD = TT_DAY_SCROLL.CASH_ACC_CD_SL) CASH_OPNG_BAL_SL, "
       +" (SELECT ABS(NVL(TM_ACC_BALANCE.BALANCE_AMT, 0))                                 "
       +"    FROM TM_ACC_BALANCE                                                          "
       +"   WHERE TM_ACC_BALANCE.BALANCE_DT = TT_DAY_SCROLL.TRANS_DT                      "
       + "     AND TM_ACC_BALANCE.ARDB_CD = {0} AND TM_ACC_BALANCE.BRN_CD = {1}                                             "
       + "     AND TM_ACC_BALANCE.BALANCE_DT BETWEEN to_date('{2}','dd-mm-yyyy' ) AND to_date('{3}','dd-mm-yyyy' ) "
       +"     AND TM_ACC_BALANCE.ACC_CD = TT_DAY_SCROLL.CASH_ACC_CD_SL) CASH_CLOS_BAL_SL, "
       +" TT_DAY_SCROLL.CASH_ACC_CD_SL                                                    "
       +"  FROM TT_DAY_SCROLL                                                                   "
       +" WHERE TT_DAY_SCROLL.TRANS_DT BETWEEN to_date('{2}','dd-mm-yyyy' ) AND to_date('{3}','dd-mm-yyyy' )"
       +"   AND (TT_DAY_SCROLL.CASH_RECP + TT_DAY_SCROLL.TRF_RECP +                             "
       +"       TT_DAY_SCROLL.CASH_PAY + TT_DAY_SCROLL.TRF_PAY) > 0                             "
       +" ORDER BY TT_DAY_SCROLL.TRANS_DT ASC,                                                  "
       +"          TT_DAY_SCROLL.ACC_CD ASC,                                                  "
       +"          TT_DAY_SCROLL.TRANS_CD ASC                                                   ";

            using (var connection = OrclDbConnection.NewConnection)
            {   
                using (var transaction = connection.BeginTransaction())
                {   
                    try{                        
                            _statement = string.Format(_query1);
                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;

                                    var parm1 = new OracleParameter("as_ardb_cd", OracleDbType.Varchar2, ParameterDirection.Input);
                                    parm1.Value = prp.ardb_cd;
                                    command.Parameters.Add(parm1);

                                    var parm2 = new OracleParameter("adt_from_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm2.Value = prp.from_dt;
                                    command.Parameters.Add(parm2);

                                     var parm3 = new OracleParameter("adt_to_dt", OracleDbType.Date, ParameterDirection.Input);
                                    parm3.Value = prp.to_dt;
                                    command.Parameters.Add(parm3);
                                    
                                    var parm4 = new OracleParameter("as_brn_cd", OracleDbType.Varchar2, ParameterDirection.Input);

                                    parm4.Value = prp.brn_cd;
                                    command.Parameters.Add(parm4);
                                    command.ExecuteNonQuery();
                                    //transaction.Commit();
                            }

                             _statement = string.Format(_query2,
                                            string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace( prp.brn_cd) ? "brn_cd" : string.Concat("'",  prp.brn_cd , "'"),
                                            prp.from_dt!= null ? prp.from_dt.ToString("dd/MM/yyyy"): "from_dt",
                                            prp.to_dt != null ?  prp.to_dt.ToString("dd/MM/yyyy"): "to_dt"
                                            );

                            using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)     
                                    {
                                        while (reader.Read())
                                        {                                
                                        var tds = new tt_day_scroll();

                                        tds.trans_dt = UtilityM.CheckNull<DateTime>(reader["TRANS_DT"]);
                                        tds.voucher_id = UtilityM.CheckNull<Int64>(reader["VOUCHER_ID"]);
                                        tds.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                        tds.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                        tds.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                        tds.cust_narration = UtilityM.CheckNull<string>(reader["CUST_NARRATION"]);
                                        tds.trans_type = UtilityM.CheckNull<string>(reader["TRANS_TYPE"]);
                                        tds.trf_type = UtilityM.CheckNull<string>(reader["TRF_TYPE"]);
                                        tds.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                        tds.cash_recp = UtilityM.CheckNull<decimal>(reader["CASH_RECP"]);
                                        tds.trf_recp = UtilityM.CheckNull<decimal>(reader["TRF_RECP"]);
                                        tds.cash_pay = UtilityM.CheckNull<decimal>(reader["CASH_PAY"]);
                                        tds.trf_pay = UtilityM.CheckNull<decimal>(reader["TRF_PAY"]);
                                        tds.cash_opng_bal = UtilityM.CheckNull<decimal>(reader["CASH_OPNG_BAL"]);
                                        tds.cash_clos_bal = UtilityM.CheckNull<decimal>(reader["CASH_CLOS_BAL"]);
                                        tds.cash_acc_cd = UtilityM.CheckNull<Int32>(reader["CASH_ACC_CD"]);
                                        tds.cash_opng_bal_sl = UtilityM.CheckNull<decimal>(reader["CASH_OPNG_BAL_SL"]);
                                        tds.cash_clos_bal_sl = UtilityM.CheckNull<decimal>(reader["CASH_CLOS_BAL_SL"]);
                                        tds.cash_acc_cd_sl = UtilityM.CheckNull<Int32>(reader["CASH_ACC_CD_SL"]);

                                      tdsRet.Add(tds);
                                    }
                                    
                                    }
                                }
                        }
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            tdsRet=null;
                        }
                }            
            }
            return tdsRet;
    }  
}
}