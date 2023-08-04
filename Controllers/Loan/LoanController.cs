using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SBWSDepositApi.Models;
using SBWSFinanceApi.LL;
using SBWSFinanceApi.Models;

namespace SBWSFinanceApi.Controllers
{
    
    [Route("api/Loan")]
    [ApiController]  
    [EnableCors("AllowOrigin")] 
    public class LoanController : ControllerBase
    {
        LoanOpenLL _ll = new LoanOpenLL(); 
         
        [Route("F_GET_EFF_INTT_RT")]
        [HttpPost]
        public decimal F_GET_EFF_INTT_RT(p_loan_param prp)
        {         
            return _ll.F_GET_EFF_INTT_RT(prp);
        } 
        
        [Route("PopulateCropAmtDueDt")]
        [HttpPost]
        public p_loan_param PopulateCropAmtDueDt(p_loan_param prp)
        {         
            return _ll.PopulateCropAmtDueDt(prp); 
        }
        

        [Route("PopulateLoanDetailedList")]
        [HttpPost]
        public List<tt_detailed_list_loan> PopulateLoanDetailedList(p_report_param prp)
        {
            return _ll.PopulateLoanDetailedList(prp);
        }
        
        [Route("PopulateInterestSubsidy")]
        [HttpPost]
        public List<tt_int_subsidy> PopulateInterestSubsidy(p_report_param prp)
        {
            return _ll.PopulateInterestSubsidy(prp);
        }

        [Route("PopulateLoanDetailedListAll")]
        [HttpPost]
        public List<tt_detailed_list_loan> PopulateLoanDetailedListAll(p_report_param prp)
        {
            return _ll.PopulateLoanDetailedListAll(prp);
        }


        [Route("GetDefaultList")]
        [HttpPost]
        public List<tt_detailed_list_loan> GetDefaultList(p_report_param prp)
        {
            return _ll.GetDefaultList(prp);
        }

        
        [Route("GetUserwiseTransDtls")]
        [HttpPost]
        public List<UserwisetransDM> GetUserwiseTransDtls(p_report_param prp)
        {
            return _ll.GetUserwiseTransDtls(prp);
        }


        [Route("PopulateDcStatement")]
        [HttpPost]
        public List<activitywisedc_type> PopulateDcStatement(p_report_param prp)
        {
            return _ll.PopulateDcStatement(prp);
        }


        [Route("PopulateLoanDisburseReg")]
        [HttpPost]
        public List<blockwisedisb_type> PopulateLoanDisburseReg(p_report_param prp)
        {
            return _ll.PopulateLoanDisburseReg(prp);
        }

        [Route("PopulateLoanDisburseRegAccwise")]
        [HttpPost]
        public List<tm_loan_all> PopulateLoanDisburseRegAccwise(p_report_param prp)
        {
            return _ll.PopulateLoanDisburseRegAccwise(prp);
        }
        
        [Route("PopulateAdvRecovStmt")]
        [HttpPost]
        public List<gm_loan_trans> PopulateAdvRecovStmt(p_report_param prp)
        {
            return _ll.PopulateAdvRecovStmt(prp);
        }
        
        [Route("PopulateInttRecovStmt")]
        [HttpPost]
        public List<gm_loan_trans> PopulateInttRecovStmt(p_report_param prp)
        {
            return _ll.PopulateInttRecovStmt(prp);
        }
        
        [Route("PopulateLoanOpenRegister")]
        [HttpPost]
        public List<tt_loan_opn_cls> PopulateLoanOpenRegister(p_report_param prp)
        {
            return _ll.PopulateLoanOpenRegister(prp);
        }
        
        [Route("PopulateNPAList")]
        [HttpPost]
        public List<tt_npa> PopulateNPAList(p_report_param prp)
        {
            return _ll.PopulateNPAList(prp);
        }


        [Route("PopulateNPAListAll")]
        [HttpPost]
        public List<tt_npa> PopulateNPAListAll(p_report_param prp)
        {
            return _ll.PopulateNPAListAll(prp);
        }

        [Route("PopulateLoanCloseRegister")]
        [HttpPost]
        public List<tt_loan_opn_cls> PopulateLoanCloseRegister(p_report_param prp)
        {
            return _ll.PopulateLoanCloseRegister(prp);
        }


        [Route("PopulateRecoveryRegister")]
        [HttpPost]
        public List<accwiserecovery_type> PopulateRecoveryRegister(p_report_param prp)
        {
            return _ll.PopulateRecoveryRegister(prp);
        }

        
        [Route("PopulateRecoveryRegisterVillWise")]
        [HttpPost]
        public List<accwiserecovery_type> PopulateRecoveryRegisterVillWise(p_report_param prp)
        {
            return _ll.PopulateRecoveryRegisterVillWise(prp);
        }


        [Route("PopulateRecoveryRegisterFundwise")]
        [HttpPost]
        public List<accwiserecovery_type> PopulateRecoveryRegisterFundwise(p_report_param prp)
        {
            return _ll.PopulateRecoveryRegisterFundwise(prp);
        }
        
        [Route("PopulateRecoveryRegisterFundwiseBlockwise")]
        [HttpPost]
        public List<blockwiserecovery_type> PopulateRecoveryRegisterFundwiseBlockwise(p_report_param prp)
        {
            return _ll.PopulateRecoveryRegisterFundwiseBlockwise(prp);
        }

        [Route("GetDemandNotice")]
        [HttpPost]
        public List<demand_notice> GetDemandNotice(p_report_param prp)
        {
            return _ll.GetDemandNotice(prp);
        }

        [Route("PopulateRecoveryRegisterAccwise")]
        [HttpPost]
        public List<gm_loan_trans> PopulateRecoveryRegisterAccwise(p_report_param prp)
        {
            return _ll.PopulateRecoveryRegisterAccwise(prp);
        }

        [Route("PopulateLoanStatement")]
        [HttpPost]
        public List<gm_loan_trans> PopulateLoanStatement(p_report_param prp)
        {
            return _ll.PopulateLoanStatement(prp);
        }
        

        [Route("PopulateLoanStatementBmardb")]
        [HttpPost]
        public List<gm_loan_trans> PopulateLoanStatementBmardb(p_report_param prp)
        {
            return _ll.PopulateLoanStatementBmardb(prp);
        }

        [Route("PopulateOvdTrfDtls")]
        [HttpPost]
        public List<gm_loan_trans> PopulateOvdTrfDtls(p_report_param prp)
        {
            return _ll.PopulateOvdTrfDtls(prp);
        }


        [Route("GetDemandList")]
        [HttpPost]
        public List<demand_list> GetDemandList(p_report_param prp)
        {
            return _ll.GetDemandList(prp);
        }

        
        [Route("GetDemandListSingle")]
        [HttpPost]
        public List<demand_list> GetDemandListSingle(p_report_param prp)
        {
            return _ll.GetDemandListSingle(prp);
        }

        [Route("GetDemandListMemberwise")]
        [HttpPost]
        public List<demandDM> GetDemandListMemberwise(p_report_param prp)
        {
            return _ll.GetDemandListMemberwise(prp);
        }

        
        [Route("GetRecoveryListGroupwise")]
        [HttpPost]
        public List<recoveryDM> GetRecoveryListGroupwise(p_report_param prp)
        {
            return _ll.GetRecoveryListGroupwise(prp);
        }


        [Route("GetDemandBlockwisegroup")]
        [HttpPost]
        public List<blockwise_type> GetDemandBlockwisegroup(p_report_param prp)
        {
            return _ll.GetDemandBlockwisegroup(prp);
        }


        [Route("GetDemandBlockwise")]
        [HttpPost]
        public List<demand_list> GetDemandBlockwise(p_report_param prp)
        {
            return _ll.GetDemandBlockwise(prp);
        }

        [Route("GetDemandActivitywise")]
        [HttpPost]
        public List<demand_list> GetDemandActivitywise(p_report_param prp)
        {
            return _ll.GetDemandActivitywise(prp);
        }

        [Route("GetRecoveryList")]
        [HttpPost]
        public List<recovery_list> GetRecoveryList(p_report_param prp)
        {
            return _ll.GetRecoveryList(prp);
        }

        [Route("GetDemandCollectionBlockwise")]
        [HttpPost]
        public List<recovery_list> GetDemandCollectionBlockwise(p_report_param prp)
        {
            return _ll.GetDemandCollectionBlockwise(prp);
        }

        [Route("GetDemandCollectionActivitywise")]
        [HttpPost]
        public List<recovery_list> GetDemandCollectionActivitywise(p_report_param prp)
        {
            return _ll.GetDemandCollectionActivitywise(prp);
        }

        [Route("PopulateLoanSubCashBook")]
        [HttpPost]
        public List<accwiseloansubcashbook> PopulateLoanSubCashBook(p_report_param prp)
        {
            return _ll.PopulateLoanSubCashBook(prp);
        }


        [Route("InsertSubsidyData")]
        [HttpPost]
        public int InsertSubsidyData(tm_subsidy prp)
        {
            return _ll.InsertSubsidyData(prp);
        }

        [Route("UpdateSubsidyData")]
        [HttpPost]
        public int UpdateSubsidyData(tm_subsidy prp)
        {
            return _ll.UpdateSubsidyData(prp);
        }
        
        [Route("UpdateLoanValuationData")]
        [HttpPost]
        public int UpdateLoanValuationData(LoanValuationDM prp)
        {
            return _ll.UpdateLoanValuationData(prp);
        }

        [Route("DeleteSubsidyData")]
        [HttpPost]
        public int DeleteSubsidyData(tm_subsidy prp)
        {
            return _ll.DeleteSubsidyData(prp);
        }


        [Route("GetSubsidyData")]
        [HttpPost]
        public tm_subsidy GetSubsidyData(tm_subsidy prp)
        {
            return _ll.GetSubsidyData(prp);
        }

        [Route("GetHostName1")]
        [HttpPost]
        public String GetHostName1()
        {
            return _ll.GetHostName1();
        }

        [Route("GetLoanData")]
        [HttpPost]
         public LoanOpenDM GetLoanData(tm_loan_all loan)
        {         
           
            return _ll.GetLoanData(loan);
        }

        [Route("GetGoldMaster")]
        [HttpPost]
        public tm_gold_master GetGoldMaster(tm_gold_master pm)
        {

            return _ll.GetGoldMaster(pm);
        }

        [Route("GetGoldMasterDtls")]
        [HttpPost]
        public List<tm_gold_master_dtls> GetGoldMasterDtls(tm_gold_master_dtls pm)
        {

            return _ll.GetGoldMasterDtls(pm);
        }

        [Route("InsertLoanAccountOpeningData")]
        [HttpPost]
         public String InsertLoanAccountOpeningData(LoanOpenDM loan)
        {         
           return _ll.InsertLoanAccountOpeningData(loan);
        }

        [Route("InsertLoanValuationData")]
        [HttpPost]
        public String InsertLoanValuationData(LoanValuationDM loan)
        {
            return _ll.InsertLoanValuationData(loan);
        }

        [Route("InsertLoanTransactionData")]
        [HttpPost]
         public String InsertLoanTransactionData(LoanOpenDM loan)
        {         
           return _ll.InsertLoanTransactionData(loan);
        } 
        
        
         [Route("PopulateLoanAccountNumber")]
        [HttpPost]
         public String PopulateLoanAccountNumber(p_gen_param prp)
        {         
           
            return _ll.PopulateLoanAccountNumber(prp);
        }

         [Route("UpdateLoanAccountOpeningData")]
        [HttpPost]
         public int UpdateLoanAccountOpeningData(LoanOpenDM loan)
        {         
           
            return _ll.UpdateLoanAccountOpeningData(loan);
        } 
        [Route("GetLoanAllWithChild")]
        [HttpPost]
         public tm_loan_all GetLoanAllWithChild(tm_loan_all loan)
        {         
           
            return _ll.GetLoanAllWithChild(loan);
        }
         [Route("CalculateLoanInterest")]
        [HttpPost]
         public p_loan_param CalculateLoanInterest(p_loan_param loan)
        {         
           
            return _ll.CalculateLoanInterest(loan);
        }

        [Route("CalculateLoanInterestYearend")]
        [HttpPost]
        public p_loan_param CalculateLoanInterestYearend(p_loan_param loan)
        {

            return _ll.CalculateLoanInterestYearend(loan);
        }

        

         [Route("CalculateLoanAccWiseInterest")]
        [HttpPost]
         public List<p_loan_param> CalculateLoanAccWiseInterest(List<p_loan_param> loan)
        {         
           
            return _ll.CalculateLoanAccWiseInterest(loan);
        }
         [Route("GetSmKccParam")]
        [HttpPost]
         public List<sm_kcc_param> GetSmKccParam()
        {         
           
            return _ll.GetSmKccParam();
        }

        [Route("ApproveLoanAccountTranaction")]
        [HttpPost]
         public string ApproveLoanAccountTranaction(p_gen_param pgp)
        {         
           
            return _ll.ApproveLoanAccountTranaction(pgp);
        }

        [Route("GetSmLoanSanctionList")]
        [HttpPost]
        public List<sm_loan_sanction> GetSmLoanSanctionList()
        { 
           return _ll.GetSmLoanSanctionList();
        }
        
        [Route("GetLoanDtls")]
        [HttpPost]
         public List<AccDtlsLov> GetLoanDtls(p_gen_param pgp)
        {         
           
            return _ll.GetLoanDtls(pgp);
        }


        [Route("GetLoanDtls1")]
        [HttpPost]
        public List<AccDtlsLov> GetLoanDtls1(p_gen_param pgp)
        {

            return _ll.GetLoanDtls1(pgp);
        }


        [Route("GetLoanDtlsByID")]
        [HttpPost]
         public List<AccDtlsLov> GetLoanDtlsByID(p_gen_param pgp)
        {         
           
            return _ll.GetLoanDtlsByID(pgp);
        }
        
        [Route("PopulateLoanRepSch")]
        [HttpPost]
         public List<tt_rep_sch> PopulateLoanRepSch(p_loan_param prp)
        {         
            return _ll.PopulateLoanRepSch(prp);
        }
        
        [Route("GetKccData")]
        [HttpPost]
         public KccMstDM GetKccData(mm_kcc_member_dtls loan)
        {         
           
            return _ll.GetKccData(loan);
        } 

        [Route("InsertKccData")]
        [HttpPost]
         public String InsertKccData(KccMstDM loan)
        {         
           return _ll.InsertKccData(loan);
        } 
        [Route("UpdateKccData")]
        [HttpPost]
         public int UpdateKccData(KccMstDM loan)
        {         
           return _ll.UpdateKccData(loan);
        } 
        
        
        [Route("DeleteKccData")]
        [HttpPost]
         public int DeleteKccData(mm_kcc_member_dtls loan)
        {         
           
            return _ll.DeleteKccData(loan);
        }

        [Route("LoanGetPassbookline")]
        [HttpPost]
        public int LoanGetPassbookline([FromBody] p_report_param pgp)
        {
            return _ll.LoanGetPassbookline(pgp);
        }

        [Route("LoanUpdatePassbookline")]
        [HttpPost]
        public int LoanUpdatePassbookline([FromBody] p_report_param pgp)
        {
            return _ll.LoanUpdatePassbookline(pgp);
        }

        [Route("LoanPassBookPrint")]
        [HttpPost]
        public List<LoanPassbook_Print> LoanPassBookPrint([FromBody] p_report_param pgp)
        {
            return _ll.LoanPassBookPrint(pgp);
        }

        [Route("LoanGetUpdatePassbookData")]
        [HttpPost]
        public List<LoanPassbook_Print> LoanGetUpdatePassbookData([FromBody] p_report_param pgp)
        {
            return _ll.LoanGetUpdatePassbookData(pgp);
        }

        [Route("LoanUpdatePassbookData")]
        [HttpPost]
        public int LoanUpdatePassbookData([FromBody] List<LoanPassbook_Print> pgp)
        {
            return _ll.LoanUpdatePassbookData(pgp);
        }

        [Route("GetLoanCharges")]
        [HttpPost]
        public List<td_loan_charges> GetLoanCharges([FromBody]  p_report_param loan)
        {

            return _ll.GetLoanCharges(loan);
        }

        [Route("InsertLoanChargesData")]
        [HttpPost]
        public int InsertLoanChargesData([FromBody] td_loan_charges loan)
        {

            return _ll.InsertLoanChargesData(loan);
        }
        
        [Route("UpdateLoanChargesData")]
        [HttpPost]
        public int UpdateLoanChargesData([FromBody] td_loan_charges loan)
        {

            return _ll.UpdateLoanChargesData(loan);
        }

        
        [Route("ApproveLoanChargesData")]
        [HttpPost]
        public int ApproveLoanChargesData([FromBody] td_loan_charges loan)
        {

            return _ll.ApproveLoanChargesData(loan);
        }

        
        [Route("DeleteLoanChargesData")]
        [HttpPost]
        public int DeleteLoanChargesData([FromBody] td_loan_charges loan)
        {

            return _ll.DeleteLoanChargesData(loan);
        }




    }
}
   