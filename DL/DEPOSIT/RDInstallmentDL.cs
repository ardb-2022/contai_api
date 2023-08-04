using System;
using System.Collections.Generic;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Utility;

namespace SBWSDepositApi.Deposit
{
    public class RDInstallmentDL
    {
        string _statement;
        internal List<td_rd_installment> GetRDInstallment(td_rd_installment dep)
        {
            List<td_rd_installment> rdInstlList = new List<td_rd_installment>();

            string _query = " SELECT ACC_NUM,INSTL_NUM,DUE_DT,INSTL_DT,STATUS"
                            + " FROM TD_RD_INSTALLMENT"
                            + " WHERE   ARDB_CD = {0} AND ACC_NUM= {1} AND DEL_FLAG = 'N' ORDER BY DUE_DT ";

            using (var connection = OrclDbConnection.NewConnection)
            {
                _statement = string.Format(_query,
                !string.IsNullOrWhiteSpace(dep.ardb_cd) ? string.Concat("'", dep.ardb_cd, "'") : "ardb_cd",
                !string.IsNullOrWhiteSpace(dep.acc_num) ? string.Concat("'", dep.acc_num, "'") : "acc_num"
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
                                    var d = new td_rd_installment();
                                    d.acc_num = UtilityM.CheckNull<string>(reader["ACC_NUM"]);
                                    d.instl_no = UtilityM.CheckNull<Int16>(reader["INSTL_NUM"]);
                                    d.due_dt = UtilityM.CheckNull<DateTime>(reader["DUE_DT"]);
                                    d.instl_dt = UtilityM.CheckNull<DateTime>(reader["INSTL_DT"]);
                                    d.status = UtilityM.CheckNull<string>(reader["STATUS"]);
                                    rdInstlList.Add(d);
                                }
                            }
                        }
                    }
                }
            }

            return rdInstlList;
        }
        }
}
