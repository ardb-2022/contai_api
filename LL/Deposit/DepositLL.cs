
using System;
using System.Collections.Generic;
using SBWSDepositApi.Deposit;
using SBWSDepositApi.Models;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class DepositLL
    {
        DepositDL _dac = new DepositDL();
        internal List<tm_deposit> GetDeposit(tm_deposit pmc)
        {
            if (pmc.temp_flag == 1)
                return _dac.GetDepositTemp(pmc);
            else
                return _dac.GetDeposit(pmc);
        }

        internal List<UserwisetransDM> GetUserwiseTransDepositDtls(p_report_param prp)
        {
            return _dac.GetUserwiseTransDepositDtls(prp);
        }

        internal decimal InsertDeposit(tm_deposit pmc)
        {
            if (pmc.temp_flag == 1)
                return _dac.InsertDepositTemp(pmc);
            else
                return _dac.InsertDeposit(pmc);
        }

        internal decimal GetInttRate(p_gen_param pmc)
        {
            return _dac.GetInttRate(pmc);
        }

        

        internal decimal GetInttRatePremature(p_gen_param pmc)
        {
            return _dac.GetInttRatePremature(pmc);
        }

        internal int UpdateDeposit(tm_deposit pmc)
        {
            if (pmc.temp_flag == 1)
                return _dac.UpdateDepositTemp(pmc);
            else
                return _dac.UpdateDeposit(pmc);
        }

        internal int DeleteDeposit(tm_deposit pmc)
        {
            if (pmc.temp_flag == 1)
                return _dac.DeleteDepositTemp(pmc);
            else
                return _dac.DeleteDeposit(pmc);
        }

        internal List<tm_deposit> GetDepositView(tm_deposit pmc)
        {

            return _dac.GetDepositView(pmc);
        }

        internal List<tm_depositall> GetDepositWithChild(tm_depositall dep)
        {
            return _dac.GetDepositWithChild(dep);
        }

        

        internal List<sb_intt_list> POPULATE_SB_INTT(p_gen_param dep)
        {
            return _dac.POPULATE_SB_INTT(dep);
        }

        internal List<sb_intt_list> POPULATE_SMS_CHARGE(p_gen_param dep)
        {
            return _dac.POPULATE_SMS_CHARGE(dep);
        }

        internal int POST_SB_INTT(p_gen_param dep)
        {
            return _dac.POST_SB_INTT(dep);
        }

        internal int POST_SMS_CHARGE(p_gen_param dep)
        {
            return _dac.POST_SMS_CHARGE(dep);
        }

        internal List<tt_agent_comm> GetAgentCommission(p_gen_param dep)
        {
            return _dac.GetAgentCommission(dep);
        }

        internal int PostAgentCommission(p_gen_param dep)
        {
            return _dac.PostAgentCommission(dep);
        }

        internal List<standing_instr> PopulateActiveSIList(p_report_param prp)
        {
            return _dac.PopulateActiveSIList(prp);
        }

        
        internal List<standing_instr> PopulateExecutedSIList(p_report_param prp)
        {
            return _dac.PopulateExecutedSIList(prp);
        }

        internal List<mm_agent> GetAgentData(mm_agent dep)
        {
            return _dac.GetAgentData(dep);
        }

        internal int UpdateUnapprovedAgentTrans(mm_agent_trans dep)
        {
            return _dac.UpdateUnapprovedAgentTrans(dep);
        }

        internal int UpdateUnapprovedDdsTrans(List<tm_daily_deposit> dep)
        {
            return _dac.UpdateUnapprovedDdsTrans(dep);
        }


        internal List<export_data> GetExportData(export_data dep)
        {
            return _dac.GetExportData(dep);
        }

        internal int PopulateImportData(export_data dep)
        {
            return _dac.PopulateImportData(dep);
        }

        internal decimal CheckDuplicateAgentData(mm_agent_trans dep)
        {
            return _dac.CheckDuplicateAgentData(dep);
        }

        

        internal mm_agent_trans GetUnapprovedAgentTrans(mm_agent_trans dep)
        {
            return _dac.GetUnapprovedAgentTrans(dep);
        }

        internal List<tm_daily_deposit> GetUnapprovedDdsTrans(mm_agent_trans dep)
        {
            return _dac.GetUnapprovedDdsTrans(dep);
        }

        internal List<tm_daily_deposit> GetDdsAccountData(export_data dep)
        {
            return _dac.GetDdsAccountData(dep);
        }

        internal int ApproveDdsImportData(mm_agent_trans dep)
        {
            return _dac.ApproveDdsImportData(dep);
        }

        internal List<string> GetDataForFile(export_data dep)
        {
            return _dac.GetDataForFile(dep);
        }

        internal int InsertImportDataFile(List<string> dep)
        {
            return _dac.InsertImportDataFile(dep);
        }

        internal string ApproveAccountTranaction(p_gen_param pgp)
        {
            return _dac.ApproveAccountTranaction(pgp);
        }
        

        internal int isDormantAccount(tm_deposit dep)
        {
            return _dac.isDormantAccount(dep);
        }
        internal List<td_def_trans_trf> GetPrevTransaction(tm_deposit tvd)
        {
            return _dac.GetPrevTransaction(tvd);
        }

        internal int UpdateDepositLockUnlock(tm_deposit pmc)
        {
            return _dac.UpdateDepositLockUnlock(pmc);
        }

        internal AccOpenDM GetDepositAddlInfo(tm_deposit td)
        {
            return _dac.GetDepositAddlInfo(td);
        }
        internal List<AccDtlsLov> GetAccDtls(p_gen_param prm)
        {
            return _dac.GetAccDtls(prm);
        }
        internal List<mm_customer> GetCustDtls(p_gen_param prm)
        {
            return _dac.GetCustDtls(prm);
        }
         internal List<tm_daily_deposit> GetDailyDeposit(tm_deposit dep)
        {
            return _dac.GetDailyDeposit(dep);
        }

        internal List<accwisesubcashbook> PopulateSubCashBookDeposit(p_report_param dep)
        {
            return _dac.PopulateSubCashBookDeposit(dep);
        }

        internal List<tt_sbca_dtl_list> PopulateDLSavings(p_report_param dep)
        {
            return _dac.PopulateDLSavings(dep);
        }

        internal List<conswise_sb_dl> PopulateDLSavingsAll(p_report_param dep)
        {
            return _dac.PopulateDLSavingsAll(dep);
        }
        
        internal decimal GetDdsInterest(p_report_param dep)
        {
            return _dac.GetDdsInterest(dep);
        }

        internal int GetPassbookline(p_report_param dep)
        {
            return _dac.GetPassbookline(dep);
        }
        
        internal int UpdatePassbookline(p_report_param dep)
        {
            return _dac.UpdatePassbookline(dep);
        }

        internal string GetCertificateStatus(p_report_param dep)
        {
            return _dac.GetCertificateStatus(dep);
        }

        internal int UpdateCertificateStatus(p_report_param dep)
        {
            return _dac.UpdateCertificateStatus(dep);
        }

        internal List<agentwiseDL> PopulateDLDds(p_report_param dep)
        {
            return _dac.PopulateDLDds(dep);
        }

        internal List<agentwiseDL> PopulateSlabwiseDeposit(p_report_param dep)
        {
            return _dac.PopulateSlabwiseDeposit(dep);
        }


        internal List<tt_sbca_dtl_list> PopulateDLRecuring(p_report_param dep)
        {
            return _dac.PopulateDLRecuring(dep);
        }


        internal List<tt_sbca_dtl_list> PopulateDLFixedDeposit(p_report_param dep)
        {
            return _dac.PopulateDLFixedDeposit(dep);
        }
        
        internal List<conswise_sb_dl> PopulateDLFixedDepositAll(p_report_param dep)
        {
            return _dac.PopulateDLFixedDepositAll(dep);
        }

        internal List<tt_acc_stmt> PopulateASSaving(p_report_param dep)
        {
            return _dac.PopulateASSaving(dep);
        }

        internal List<tt_dds_statement> PopulateASDds(p_report_param dep)
        {
            return _dac.PopulateASDds(dep);
        }

        internal List<tm_deposit> PopulateASRecuring(p_report_param dep)
        {
            return _dac.PopulateASRecuring(dep);
        }
        
        internal List<tm_deposit> PopulateASFixedDeposit(p_report_param dep)
        {
            return _dac.PopulateASFixedDeposit(dep);
        }
        
        internal List<tt_opn_cls_register> PopulateOpenCloseRegister(p_report_param dep)
        {
            return _dac.PopulateOpenCloseRegister(dep);
        }
        
        internal List<tm_deposit> PopulateNearMatDetails(p_report_param dep)
        {
            return _dac.PopulateNearMatDetails(dep);
        }

        internal List<passbook_print> PassBookPrint(p_report_param dep)
        {
            return _dac.PassBookPrint(dep);
        }        

        internal List<passbook_print> GetUpdatePassbookData(p_report_param dep)
        {
            return _dac.GetUpdatePassbookData(dep);
        }        
        internal int UpdatePassbookData(List<passbook_print> dep)
        {
            return _dac.UpdatePassbookData(dep);
        }

        internal List<tt_td_intt_cert> GetInterestCertificate(p_report_param dep)
        {
            return _dac.GetInterestCertificate(dep);
        }

    }
}