using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class InttDetailsDL
    {
        string _statement;
        internal List<td_intt_dtls> GetInttDetails(td_intt_dtls dep)
        {
            List<td_intt_dtls> inttDtls = new List<td_intt_dtls>();

            string _query = " SELECT BRN_CD, ACC_TYPE_CD, ACC_NUM, RENW_ID, INTT_AMT, CALC_DT,PAID_STATUS,PAID_DT,TRANS_CD  "
                            + " FROM TD_TDINTT_DTLS"
                            + " WHERE ARDB_CD = {0} AND BRN_CD={1} AND ACC_NUM={2}  AND ACC_TYPE_CD = {3} AND DEL_FLAG ='N'  "
                            + " AND RENW_ID = (SELECT max(renw_id) FROM TD_TDINTT_DTLS WHERE ARDB_CD = {4} AND BRN_CD= {5} AND ACC_NUM={6}  AND ACC_TYPE_CD = {7} AND DEL_FLAG ='N') "
                            + "ORDER BY CALC_DT ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          (dep.acc_type_cd > 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD"),
                                          !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                                          !string.IsNullOrWhiteSpace(dep.brn_cd) ? string.Concat("'", dep.brn_cd, "'") : "brn_cd",
                                          !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num",
                                          (dep.acc_type_cd > 0 ? dep.acc_type_cd.ToString() : "ACC_TYPE_CD"));
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            {
                                while (reader.Read())
                                {
                                    var d = new td_intt_dtls();
                                    d.brn_cd = UtilityM.CheckNull<string>(reader["BRN_CD"]);
                                    d.acc_type_cd = UtilityM.CheckNull<int>(reader["ACC_TYPE_CD"]);
                                    d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    d.renew_id = UtilityM.CheckNull<int>(reader["RENW_ID"]);
                                    d.intt_amt = UtilityM.CheckNull<double>(reader["INTT_AMT"]);
                                    d.trans_cd = UtilityM.CheckNull<Int64>(reader["TRANS_CD"]);
                                    d.paid_status = UtilityM.CheckNull<string>(reader["PAID_STATUS"]);
                                    d.paid_dt = UtilityM.CheckNull<DateTime>(reader["PAID_DT"]);
                                    d.calc_dt = UtilityM.CheckNull<DateTime>(reader["CALC_DT"]);
                                    inttDtls.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return inttDtls;
        }
    
    }
}
