using System;
using System.Collections.Generic;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.DL.INVESTMENT;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class InvOpenLL
    {
        InvOpenDL _dac = new InvOpenDL();
        internal InvOpenDM GetInvOpeningData(tm_deposit_inv td)
        {
            return _dac.GetInvOpeningData(td);
        }

        internal string InsertInvOpeningData(InvOpenDM td)
        {
            return _dac.InsertInvOpeningData(td);
        }
        
        internal int UpdateInvOpeningData(InvOpenDM td)
        {
            return _dac.UpdateInvOpeningData(td);
        }

        internal List<conswise_sb_dl> PopulateDLFixedDepositInvAll(p_report_param dep)
        {
            return _dac.PopulateDLFixedDepositInvAll(dep);
        }

        internal int DeleteInvOpeningData(td_def_trans_trf td)
        {
            return _dac.DeleteInvOpeningData(td);
        }

        internal decimal F_CALCTDINTT_INV_REG(p_gen_param pmc)
        {
            return _dac.F_CALCTDINTT_INV_REG(pmc);
        }
        
        internal string ApproveInvTranaction(p_gen_param td)
        {
            return _dac.ApproveInvTranaction(td);
        }

        internal List<tt_sbca_dtl_list> PopulateNearInvReport(p_report_param dep)
        {
            return _dac.PopulateNearInvReport(dep);
        }
    }
}
