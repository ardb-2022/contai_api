using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class TrialBalanceDL
    {
        string _statement;
        string _statement1;
        internal List<tt_trial_balance> PopulateTrialBalance(p_report_param prp)
        {
            List<tt_trial_balance> tcaRet=new List<tt_trial_balance>();
            string _query=" SELECT TM_ACC_BALANCE.BALANCE_DT,                  "
    +" TM_ACC_BALANCE.ACC_CD,"
	+" M_ACC_MASTER.ACC_NAME,"
    +" TM_ACC_BALANCE.BALANCE_AMT DEBIT,"
	+" 0.00 CREDIT,"
	+" M_ACC_MASTER.ACC_TYPE"
    +" FROM TM_ACC_BALANCE,"
	+" M_ACC_MASTER"
	+ " WHERE TM_ACC_BALANCE.ARDB_CD = M_ACC_MASTER.ARDB_CD AND TM_ACC_BALANCE.ACC_CD = M_ACC_MASTER.ACC_CD"
    + " AND TM_ACC_BALANCE.ARDB_CD ={0} AND TM_ACC_BALANCE.BRN_CD ={1}"
    + " AND TM_ACC_BALANCE.BALANCE_DT = to_date('{2}','dd-mm-yyyy' ) "
	+" AND TM_ACC_BALANCE.BALANCE_AMT > 0"
	+" AND TM_ACC_BALANCE.ACC_CD <> {3}"
 	+" UNION"
	+" SELECT TM_ACC_BALANCE.BALANCE_DT,"
    +" TM_ACC_BALANCE.ACC_CD,"
  	+" M_ACC_MASTER.ACC_NAME,"
    +" 0.00 DEBIT,"
    +" ABS(TM_ACC_BALANCE.BALANCE_AMT) CREDIT,"
  	+" M_ACC_MASTER.ACC_TYPE"
    +" FROM TM_ACC_BALANCE,"
  	+" M_ACC_MASTER"
  	+ " WHERE TM_ACC_BALANCE.ARDB_CD = M_ACC_MASTER.ARDB_CD AND TM_ACC_BALANCE.ACC_CD = M_ACC_MASTER.ACC_CD"
    + " AND TM_ACC_BALANCE.ARDB_CD ={0} AND TM_ACC_BALANCE.BRN_CD = {1}"
      + " AND TM_ACC_BALANCE.BALANCE_DT =to_date('{2}','dd-mm-yyyy' )"
  	+" AND TM_ACC_BALANCE.BALANCE_AMT < 0"
  	+" AND TM_ACC_BALANCE.ACC_CD <>{3}"
    +" ORDER BY 6";
            using (var connection = OrclDbConnection.NewConnection)
            {   
                using (var transaction = connection.BeginTransaction())
                {   
                    try{         
                            _statement = string.Format(_query,
                                            string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                            string.IsNullOrWhiteSpace( prp.brn_cd) ? "brn_cd" : string.Concat("'",  prp.brn_cd , "'"),
                                            prp.trial_dt!= null ? prp.trial_dt.ToString("dd/MM/yyyy"): "trial_dt",
                                            prp.pl_acc_cd !=0 ? Convert.ToString(prp.pl_acc_cd) : "pl_acc_cd"
                                            );                        


                        using (var command = OrclDbConnection.Command(connection, _statement))
                            {
                                 using (var reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)     
                                    {
                                        while (reader.Read())
                                        {                                
                                                var tca = new tt_trial_balance();
                                                tca.balance_dt = UtilityM.CheckNull<DateTime>(reader["BALANCE_DT"]);
                                                tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                                tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                tca.dr = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                                tca.cr = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                                tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                                tcaRet.Add(tca);
                                        }
                                    }
                                }
                            }
                            
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            tcaRet=null;
                        }
                }            
            }
            return tcaRet;
    }



        internal List<trailDM> PopulateTrialGroupwise(p_report_param prp)
        {
            List<trailDM> tcaRet = new List<trailDM>();

            string _query = " SELECT TM_ACC_BALANCE.BALANCE_DT,                  "
                                + " TM_ACC_BALANCE.ACC_CD,"
                                + " M_ACC_MASTER.ACC_NAME,"
                                + " TM_ACC_BALANCE.BALANCE_AMT DEBIT,"
                                + " 0.00 CREDIT,"
                                + " M_ACC_MASTER.ACC_TYPE"
                                + " FROM TM_ACC_BALANCE,"
                                + " M_ACC_MASTER"
                                + " WHERE TM_ACC_BALANCE.ARDB_CD = M_ACC_MASTER.ARDB_CD AND TM_ACC_BALANCE.ACC_CD = M_ACC_MASTER.ACC_CD"
                                + " AND TM_ACC_BALANCE.ARDB_CD ={0} AND TM_ACC_BALANCE.BRN_CD ={1}"
                                + " AND TM_ACC_BALANCE.BALANCE_DT = to_date('{2}','dd-mm-yyyy' ) "
                                + " AND TM_ACC_BALANCE.BALANCE_AMT > 0"
                                + " AND TM_ACC_BALANCE.ACC_CD <> {3}"
                                + " AND M_ACC_MASTER.ACC_TYPE = {4} "
                                + " UNION"
                                + " SELECT TM_ACC_BALANCE.BALANCE_DT,"
                                + " TM_ACC_BALANCE.ACC_CD,"
                                + " M_ACC_MASTER.ACC_NAME,"
                                + " 0.00 DEBIT,"
                                + " ABS(TM_ACC_BALANCE.BALANCE_AMT) CREDIT,"
                                + " M_ACC_MASTER.ACC_TYPE"
                                + " FROM TM_ACC_BALANCE,"
                                + " M_ACC_MASTER"
                                + " WHERE TM_ACC_BALANCE.ARDB_CD = M_ACC_MASTER.ARDB_CD AND TM_ACC_BALANCE.ACC_CD = M_ACC_MASTER.ACC_CD"
                                + " AND TM_ACC_BALANCE.ARDB_CD ={0} AND TM_ACC_BALANCE.BRN_CD = {1}"
                                + " AND TM_ACC_BALANCE.BALANCE_DT =to_date('{2}','dd-mm-yyyy' )"
                                + " AND TM_ACC_BALANCE.BALANCE_AMT < 0"
                                + " AND TM_ACC_BALANCE.ACC_CD <>{3}"
                                + " AND M_ACC_MASTER.ACC_TYPE = {4} "
                                + " ORDER BY 6,2";

            string _query1 = " Select DISTINCT acc_type, decode( a.ACC_TYPE,1,'Liabiliy',2,'Asset',3,'Revenue',4,'Expanse') acctype "
                            + " FROM M_ACC_MASTER a "
                            + " WHERE a.ARDB_CD= {0} ORDER BY acc_type ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {                         

                        _statement1 = string.Format(_query1,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'")
                                        );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        trailDM tcaRet1 = new trailDM();
                                        var tca1 = new trail_type();
                                        tca1.acc_type = UtilityM.CheckNull<string>(reader1["ACCTYPE"]);
                                        
                                        _statement = string.Format(_query,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                        string.IsNullOrWhiteSpace(prp.brn_cd) ? "brn_cd" : string.Concat("'", prp.brn_cd, "'"),
                                        prp.trial_dt != null ? prp.trial_dt.ToString("dd/MM/yyyy") : "trial_dt",
                                        prp.pl_acc_cd != 0 ? Convert.ToString(prp.pl_acc_cd) : "pl_acc_cd",
                                        string.Concat("'", UtilityM.CheckNull<string>(reader1["ACC_TYPE"]), "'")
                                        );

                                        tcaRet1.acc_type = tca1;


                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_trial_balance();
                                                        tca.balance_dt = UtilityM.CheckNull<DateTime>(reader["BALANCE_DT"]);
                                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                        tca.dr = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                                        tca.cr = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                                        tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                                        tcaRet1.trailbalance.Add(tca);                                                       
                                                    }                                                   
                                                }
                                            }
                                        }

                                        tcaRet.Add(tcaRet1);
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


        internal List<trailDM> PopulateTrialGroupwiseConso(p_report_param prp)
        {
            List<trailDM> tcaRet = new List<trailDM>();
            

            string _query = "SELECT v_acc_balance_all.balance_dt, "
                             + "  v_acc_balance_all.acc_cd, "
                             + " m_acc_master.acc_name,"
                             + " sum(Decode(v_acc_balance_all.balance_amt + ABS(v_acc_balance_all.balance_amt), 0, 0,"
                             + " v_acc_balance_all.balance_amt)) Debit,"
                             + " sum(Decode(v_acc_balance_all.balance_amt + ABS(v_acc_balance_all.balance_amt), 0,"
                             + " ABS(v_acc_balance_all.balance_amt), 0)) Credit,"
                             + " m_acc_master.acc_type"
                             + " FROM   v_acc_balance_all,  "
                             + " m_acc_master"
                             + " WHERE v_acc_balance_all.ardb_cd = m_acc_master.ardb_cd AND v_acc_balance_all.acc_cd = m_acc_master.acc_cd "
                             + " AND v_acc_balance_all.ardb_cd = {0} AND v_acc_balance_all.balance_dt = to_date('{1}','dd-mm-yyyy' ) "
                             + " AND v_acc_balance_all.balance_amt <> 0 "
                             + " AND v_acc_balance_all.acc_cd <> {2} "
                             + " AND m_acc_master.acc_type = {3} "
                             + " GROUP BY  v_acc_balance_all.balance_dt, v_acc_balance_all.acc_cd, m_acc_master.acc_name, m_acc_master.acc_type "
                             + " ORDER BY 6,2 ";

            string _query1 = " Select DISTINCT acc_type, decode( a.ACC_TYPE,1,'Liabiliy',2,'Asset',3,'Revenue',4,'Expanse') acctype "
                            + " FROM M_ACC_MASTER a "
                            + " WHERE a.ARDB_CD= {0} ORDER BY acc_type ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {

                        _statement1 = string.Format(_query1,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'")
                                        );

                        using (var command1 = OrclDbConnection.Command(connection, _statement1))
                        {
                            using (var reader1 = command1.ExecuteReader())
                            {
                                if (reader1.HasRows)
                                {
                                    while (reader1.Read())
                                    {
                                        trailDM tcaRet1 = new trailDM();
                                        var tca1 = new trail_type();
                                        tca1.acc_type = UtilityM.CheckNull<string>(reader1["ACCTYPE"]);

                                        _statement = string.Format(_query,
                                        string.IsNullOrWhiteSpace(prp.ardb_cd) ? "ardb_cd" : string.Concat("'", prp.ardb_cd, "'"),
                                       prp.trial_dt != null ? prp.trial_dt.ToString("dd/MM/yyyy") : "trial_dt",
                                        prp.pl_acc_cd != 0 ? Convert.ToString(prp.pl_acc_cd) : "pl_acc_cd",
                                        string.Concat("'", UtilityM.CheckNull<string>(reader1["ACC_TYPE"]), "'")
                                        );

                                        tcaRet1.acc_type = tca1;


                                        using (var command = OrclDbConnection.Command(connection, _statement))
                                        {
                                            using (var reader = command.ExecuteReader())
                                            {
                                                if (reader.HasRows)
                                                {
                                                    while (reader.Read())
                                                    {
                                                        var tca = new tt_trial_balance();
                                                        tca.balance_dt = UtilityM.CheckNull<DateTime>(reader["BALANCE_DT"]);
                                                        tca.acc_cd = UtilityM.CheckNull<int>(reader["ACC_CD"]);
                                                        tca.acc_name = UtilityM.CheckNull<string>(reader["ACC_NAME"]);
                                                        tca.dr = UtilityM.CheckNull<decimal>(reader["DEBIT"]);
                                                        tca.cr = UtilityM.CheckNull<decimal>(reader["CREDIT"]);
                                                        tca.acc_type = UtilityM.CheckNull<string>(reader["ACC_TYPE"]);
                                                        tcaRet1.trailbalance.Add(tca);
                                                    }
                                                }
                                            }
                                        }

                                        tcaRet.Add(tcaRet1);
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

    }
}