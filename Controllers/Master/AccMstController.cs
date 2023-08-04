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
    [Route("api/Mst")]
    [ApiController]
    public class AccMstController : ControllerBase
    {
        AccMstLL _ll = new AccMstLL(); 
        [Route("GetAccountMaster")]
        [HttpPost]
        public List<m_acc_master> GetAccountMaster(m_acc_master mum)
        {
           return _ll.GetAccountMaster(mum);
        }

       
        [Route("GetRolePermission")]
        [HttpPost]
        public List<mm_role_permission> GetRolePermission(p_gen_param mum)
        {
            return _ll.GetRolePermission(mum);
        }

        
        [Route("GetRoleMaster")]
        [HttpPost]
        public List<mm_role_permission> GetRoleMaster(p_gen_param mum)
        {
            return _ll.GetRoleMaster(mum);
        }

        [Route("InsertRolePermission")]
        [HttpPost]
        public int InsertRolePermission(mm_role_permission mum)
        {
            return _ll.InsertRolePermission(mum);
        }

        [Route("UpdateRolePermission")]
        [HttpPost]
        public int UpdateRolePermission(mm_role_permission mum)
        {
            return _ll.UpdateRolePermission(mum);
        }

        [Route("GetAccountTypeMaster")]
        [HttpPost]
        public List<mm_acc_type> GetAccountTypeMaster()
        { 
           return _ll.GetAccountTypeMaster();
        }

         [Route("GetConstitution")]
        [HttpPost]
        public List<mm_constitution> GetConstitution()
        {
           return _ll.GetConstitution();
        }

        [Route("GetOprationalInstr")]
        [HttpPost]
        public List<mm_oprational_intr> GetOprationalInstr()
        {
           return _ll.GetOprationalInstr();
        }

        [Route("GetUserDtls")]
        [HttpPost]
        public List<m_user_master> GetUserDtls(m_user_master mum)
        {
           return _ll.GetUserDtls(mum);
        }

        [Route("GetUserType")]
        [HttpPost]
        public List<m_user_type> GetUserDtls(m_user_type mum)
        {
           return _ll.GetUserType(mum);
        }


        [Route("UpdateUserstatus")]
        [HttpPost]
        public int UpdateUserstatus(m_user_master mum)
        {
           return _ll.UpdateUserstatus(mum);
        }

        [Route("GetCategoryMaster")]
        [HttpPost]
        public List<mm_category> GetCategoryMaster()
        {
           return _ll.GetCategoryMaster();
        }

        [Route("GetStateMaster")]
        [HttpPost]
        public List<mm_state> GetStateMaster()
        {
           return _ll.GetStateMaster();
        }

        [Route("GetDistMaster")]
        [HttpPost]
        public List<mm_dist> GetDistMaster()
        {
           return _ll.GetDistMaster();
        }

        [Route("GetVillageMaster")]
        [HttpPost]
        public List<mm_vill> GetVillageMaster(mm_vill mum)
        {
           return _ll.GetVillageMaster(mum);
        }

        [Route("GetPsMaster")]
        [HttpPost]
        public List<mm_ps> GetPsMaster(mm_ps mum)
        {
            return _ll.GetPsMaster(mum);
        }

        [Route("GetPoMaster")]
        [HttpPost]
        public List<mm_po> GetPoMaster(mm_po mum)
        {
            return _ll.GetPoMaster(mum);
        }

        [Route("GetBankInvMaster")]
        [HttpPost]
        public List<mm_bank_inv> GetBankInvMaster(mm_bank_inv mum)
        {
            return _ll.GetBankInvMaster(mum);
        }

        [Route("GetBranchInvMaster")]
        [HttpPost]
        public List<mm_branch_inv> GetBranchInvMaster(mm_branch_inv mum)
        {
            return _ll.GetBranchInvMaster(mum);
        }

        [Route("GetServiceAreaMaster")]
        [HttpPost]
        public List<mm_service_area> GetServiceAreaMaster(mm_service_area mum)
        {
           return _ll.GetServiceAreaMaster(mum);
        }

        [Route("GetBlockMaster")]
        [HttpPost]
        public List<mm_block> GetBlockMaster(mm_block mum)
        {
           return _ll.GetBlockMaster(mum);
        }

        [Route("GetAccGlhead")]
        [HttpPost]
        public List<m_acc_master> GetAccGlhead(m_acc_master mum)
        {
            return _ll.GetAccGlhead(mum);
        }


        [Route("InsertServiceAreaMaster")]
        [HttpPost]
        public int InsertServiceAreaMaster(mm_service_area mum)
        {
            return _ll.InsertServiceAreaMaster(mum);
        }

        [Route("InsertBlockMaster")]
        [HttpPost]
        public int InsertBlockMaster(mm_block mum)
        {
            return _ll.InsertBlockMaster(mum);
        }

        [Route("InsertAccGlHead")]
        [HttpPost]
        public int InsertAccGlHead(m_acc_master mum)
        {
            return _ll.InsertAccGlHead(mum);
        }


        [Route("InsertVillageMaster")]
        [HttpPost]
        public int InsertVillageMaster(mm_vill mum)
        {
            return _ll.InsertVillageMaster(mum);
        }

        [Route("InsertPoMaster")]
        [HttpPost]
        public int InsertPoMaster(mm_po mum)
        {
            return _ll.InsertPoMaster(mum);
        }

        [Route("InsertPsMaster")]
        [HttpPost]
        public int InsertPsMaster(mm_ps mum)
        {
            return _ll.InsertPsMaster(mum);
        }

        [Route("InsertBankInvMaster")]
        [HttpPost]
        public int InsertBankInvMaster(mm_bank_inv mum)
        {
            return _ll.InsertBankInvMaster(mum);
        }

        [Route("InsertBranchInvMaster")]
        [HttpPost]
        public int InsertBranchInvMaster(mm_branch_inv mum)
        {
            return _ll.InsertBranchInvMaster(mum);
        }

        [Route("UpdateBlock")]
        [HttpPost]
        public int UpdateBlock(mm_block mum)
        {
            return _ll.UpdateBlock(mum);
        }

        [Route("UpdateAccGlHead")]
        [HttpPost]
        public int UpdateAccGlHead(m_acc_master mum)
        {
            return _ll.UpdateAccGlHead(mum);
        }


        [Route("UpdateServiceArea")]
        [HttpPost]
        public int UpdateServiceArea(mm_service_area mum)
        {
            return _ll.UpdateServiceArea(mum);
        }

        
        [Route("UpdateVillage")]
        [HttpPost]
        public int UpdateVillage(mm_vill mum)
        {
            return _ll.UpdateVillage(mum);
        }

        [Route("UpdatePo")]
        [HttpPost]
        public int UpdatePo(mm_po mum)
        {
            return _ll.UpdatePo(mum);
        }

        [Route("UpdatePs")]
        [HttpPost]
        public int UpdatePs(mm_ps mum)
        {
            return _ll.UpdatePs(mum);
        }


        [Route("UpdateBankInv")]
        [HttpPost]
        public int UpdateBankInv(mm_bank_inv mum)
        {
            return _ll.UpdateBankInv(mum);
        }


        [Route("UpdateBranchInv")]
        [HttpPost]
        public int UpdateBranchInv(mm_branch_inv mum)
        {
            return _ll.UpdateBranchInv(mum);
        }

        [Route("GetSystemDate")]
        [HttpPost]
        public day_initialize GetSystemDate(m_branch mum)
        {
            return _ll.GetSystemDate(mum);
        }

        [Route("GetKycMaster")]
        [HttpPost]
        public List<mm_kyc> GetKycMaster()
        {
           return _ll.GetKycMaster();
        }
        [Route("GetTitleMaster")]
        [HttpPost]
        public List<mm_title> GetTitleMaster()
        {
           return _ll.GetTitleMaster();
        }

        [Route("GetBranchMaster")]
        [HttpPost]
        public List<m_branch> GetBranchMaster(m_branch mum)
        {
           return _ll.GetBranchMaster(mum);
        }

        [Route("GetARDBMaster")]
        [HttpPost]
        public List<mm_ardb> GetARDBMaster()
        {
           return _ll.GetARDBMaster();
        }

        [Route("GetSystemParameter")]
        [HttpPost]
        public List<sm_parameter> GetSystemParameter()
        {
           return _ll.GetSystemParameter();
        }

        [Route("GetOperationMaster")]
        [HttpPost]
        public List<mm_operation> GetOperationMaster()
        {
           return _ll.GetOperationMaster();
        }

        [Route("GetOperationDtls")]
        [HttpPost]
        public List<mm_operation> GetOperationDtls()
        {
           return _ll.GetOperationDtls();
        }

        [Route("GetInstalmentTypeMaster")]
        [HttpPost]
        public List<mm_instalment_type> GetInstalmentTypeMaster()
        {
            return _ll.GetInstalmentTypeMaster();
        }
        [Route("GetCropMaster")]
        [HttpPost]
        public List<mm_crop> GetCropMaster()
        {
            return _ll.GetCropMaster();
        }
        [Route("GetActivityMaster")]
        [HttpPost]
        public List<mm_activity> GetActivityMaster()
        {
            return _ll.GetActivityMaster();
        }
        [Route("GetSectorMaster")]
        [HttpPost]
        public List<mm_sector> GetSectorMaster()
        {
            return _ll.GetSectorMaster();
        }


        [Route("UpdateSystemParameter")]
        [HttpPost]
        public int UpdateSystemParameter(sm_parameter smParam)
        {
            return _ll.UpdateSystemParameter(smParam);
        }
          
    }
}
