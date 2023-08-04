
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class FinanceReportLL
    {
       CashCumTrialDL _dacCashCumTrialDL = new CashCumTrialDL(); 
        internal List<tt_cash_cum_trial> PopulateCashCumTrial(p_report_param prp)
        {         
            return _dacCashCumTrialDL.PopulateCashCumTrial(prp);
        }

        internal List<tt_cash_cum_trial> PopulateCashCumTrialConso(p_report_param prp)
        {
            return _dacCashCumTrialDL.PopulateCashCumTrialConso(prp);
        }
        

        internal List<tt_cash_cum_trial> PopulateCashCumTrialConsoNew(p_report_param prp)
        {
            return _dacCashCumTrialDL.PopulateCashCumTrialConsoNew(prp);
        }

        Weekly_ReturnDL _dacWeeklyReturnDL = new Weekly_ReturnDL();
        internal List<weekly_return> PopulateWeeklyReturn(p_report_param prp)
        {
            return _dacWeeklyReturnDL.PopulateWeeklyReturn(prp);
        }

        TrialBalanceDL _dacTrialBalanceDL = new TrialBalanceDL(); 
        internal List<tt_trial_balance> PopulateTrialBalance(p_report_param prp)
        {         
            return _dacTrialBalanceDL.PopulateTrialBalance(prp);
        }
        
       internal List<trailDM> PopulateTrialGroupwise(p_report_param prp)
        {
            return _dacTrialBalanceDL.PopulateTrialGroupwise(prp);
        }

        internal List<trailDM> PopulateTrialGroupwiseConso(p_report_param prp)
        {
            return _dacTrialBalanceDL.PopulateTrialGroupwiseConso(prp);
        }

        DailyCashBookDL _dacDailyCashBookDL = new DailyCashBookDL(); 
        internal List<tt_cash_account> PopulateDailyCashBook(p_report_param prp)
        {         
            return _dacDailyCashBookDL.PopulateDailyCashBook(prp);
        }

        internal List<tt_cash_account> PopulateDailyCashAccount(p_report_param prp)
        {
            return _dacDailyCashBookDL.PopulateDailyCashAccount(prp);
        }

        internal List<tt_cash_account> PopulateDailyCashBookConso(p_report_param prp)
        {
            return _dacDailyCashBookDL.PopulateDailyCashBookConso(prp);
        }

        internal List<tt_cash_account> PopulateDailyCashAccountConso(p_report_param prp)
        {
            return _dacDailyCashBookDL.PopulateDailyCashAccountConso(prp);
        }

        internal List<cashaccountDM> PopulateDailyCashAccountConsoNew(p_report_param prp)
        {
            return _dacDailyCashBookDL.PopulateDailyCashAccountConsoNew(prp);
        }

        DayScrollBookDL _dacDayScrollBookDL = new DayScrollBookDL(); 
        internal List<tt_day_scroll> PopulateDayScrollBook(p_report_param prp)
        {     
              return _dacDayScrollBookDL.PopulateDayScrollBook(prp);          
        }

        BalanceSheet _dacBalanceSheetDL = new BalanceSheet();
        internal List<tt_balance_sheet> PopulateBalanceSheet(p_report_param prp)
        {
            return _dacBalanceSheetDL.PopulateBalanceSheet(prp);
        }

        internal List<tt_balance_sheet> PopulateBalanceSheetConso(p_report_param prp)
        {
            return _dacBalanceSheetDL.PopulateBalanceSheetConso(prp);
        }

        ProfitandLoss _dacProfitandLossDL = new ProfitandLoss();
        internal List<tt_pl_book> PopulateProfitandLoss(p_report_param prp)
        {
            return _dacProfitandLossDL.PopulateProfitandLoss(prp);
        }

        internal List<tt_pl_book> PopulateProfitandLossConso(p_report_param prp)
        {
            return _dacProfitandLossDL.PopulateProfitandLossConso(prp);
        }

        internal List<accwisegl> GetGeneralLedger(p_report_param prm)
        {
            var _dac = new RptGeneralLedgerTransactionDtlsDL();
            return _dac.GetGeneralLedger(prm);
        }

        internal List<tt_gl_trans> GetGLTransDtls(p_report_param prm)
        {
            var _dac = new RptGeneralLedgerTransactionDtlsDL();
            return _dac.GetGLTransDtls(prm);
        }
        
        internal List<tt_gl_trans> GetGLTransDtlsConso(p_report_param prm)
        {
            var _dac = new RptGeneralLedgerTransactionDtlsDL();
            return _dac.GetGLTransDtlsConso(prm);
        }
    }
}