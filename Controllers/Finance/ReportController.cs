using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Controllers
{
    [EnableCors("AllowOrigin")] 
    [Route("api/Finance")]
    [ApiController]
    public class ReportController : ControllerBase
    {
        FinanceReportLL _ll = new FinanceReportLL(); 
        [Route("PopulateDailyCashBook")]
        [HttpPost]
        public List<tt_cash_account> PopulateDailyCashBook([FromBody] p_report_param prp)
        {
           return _ll.PopulateDailyCashBook(prp);
        }

        [Route("PopulateDailyCashAccount")]
        [HttpPost]
        public List<tt_cash_account> PopulateDailyCashAccount([FromBody] p_report_param prp)
        {
            return _ll.PopulateDailyCashAccount(prp);
        }

        [Route("PopulateWeeklyReturn")]
        [HttpPost]
        public List<weekly_return> PopulateWeeklyReturn([FromBody] p_report_param prp)
        {
            return _ll.PopulateWeeklyReturn(prp);
        }

        [Route("PopulateDailyCashBookConso")]
        [HttpPost]
        public List<tt_cash_account> PopulateDailyCashBookConso([FromBody] p_report_param prp)
        {
            return _ll.PopulateDailyCashBookConso(prp);
        }

        [Route("PopulateDailyCashAccountConso")]
        [HttpPost]
        public List<tt_cash_account> PopulateDailyCashAccountConso([FromBody] p_report_param prp)
        {
            return _ll.PopulateDailyCashAccountConso(prp);
        }

        [Route("PopulateDailyCashAccountConsoNew")]
        [HttpPost]
        public List<cashaccountDM> PopulateDailyCashAccountConsoNew([FromBody] p_report_param prp)
        {
            return _ll.PopulateDailyCashAccountConsoNew(prp);
        }

        [Route("PopulateCashCumTrial")]
        [HttpPost]
        public List<tt_cash_cum_trial> PopulateCashCumTrial([FromBody] p_report_param prp)
        {
           return _ll.PopulateCashCumTrial(prp);
        }

        [Route("PopulateCashCumTrialConso")]
        [HttpPost]
        public List<tt_cash_cum_trial> PopulateCashCumTrialConso([FromBody] p_report_param prp)
        {
            return _ll.PopulateCashCumTrialConso(prp);
        }

        [Route("PopulateCashCumTrialConsoNew")]
        [HttpPost]
        public List<tt_cash_cum_trial> PopulateCashCumTrialConsoNew([FromBody] p_report_param prp)
        {
            return _ll.PopulateCashCumTrialConsoNew(prp);
        }

        [Route("PopulateDayScrollBook")]
        [HttpPost]
        public List<tt_day_scroll> PopulateDayScrollBook([FromBody] p_report_param prp)
        {      
           return _ll.PopulateDayScrollBook(prp);
        }

        [Route("PopulateBalanceSheet")]
        [HttpPost]
        public List<tt_balance_sheet> PopulateBalanceSheet([FromBody] p_report_param prp)
        {
            return _ll.PopulateBalanceSheet(prp);
        }

        [Route("PopulateBalanceSheetConso")]
        [HttpPost]
        public List<tt_balance_sheet> PopulateBalanceSheetConso([FromBody] p_report_param prp)
        {
            return _ll.PopulateBalanceSheetConso(prp);
        }

        [Route("PopulateProfitandLoss")]
        [HttpPost]
        public List<tt_pl_book> PopulateProfitandLoss([FromBody] p_report_param prp)
        {
            return _ll.PopulateProfitandLoss(prp);
        }

        [Route("PopulateProfitandLossConso")]
        [HttpPost]
        public List<tt_pl_book> PopulateProfitandLossConso([FromBody] p_report_param prp)
        {
            return _ll.PopulateProfitandLossConso(prp);
        }

      [Route("GetGeneralLedger")]
      [HttpPost]
      public List<accwisegl> GetGeneralLedger([FromBody] p_report_param prm)
      {
         return _ll.GetGeneralLedger(prm);
      }

        
      [Route("GetGLTransDtls")]
      [HttpPost]
      public List<tt_gl_trans> GetGLTransDtls([FromBody] p_report_param prm)
        {
            return _ll.GetGLTransDtls(prm);
        }

        
      [Route("GetGLTransDtlsConso")]
      [HttpPost]
      public List<tt_gl_trans> GetGLTransDtlsConso([FromBody] p_report_param prm)
        {
            return _ll.GetGLTransDtlsConso(prm);
        }


        [Route("PopulateTrialBalance")]
      [HttpPost]
      public List<tt_trial_balance> PopulateTrialBalance([FromBody] p_report_param prm)
      {
         return _ll.PopulateTrialBalance(prm);
      }
        

      [Route("PopulateTrialGroupwise")]
      [HttpPost]
      public List<trailDM> PopulateTrialGroupwise([FromBody] p_report_param prm)
        {
            return _ll.PopulateTrialGroupwise(prm);
        }        

      [Route("PopulateTrialGroupwiseConso")]
      [HttpPost]
      public List<trailDM> PopulateTrialGroupwiseConso([FromBody] p_report_param prm)
        {
            return _ll.PopulateTrialGroupwiseConso(prm);
        }

    }
}

