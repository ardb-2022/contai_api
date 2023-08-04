
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{   
    public class AccMstLL
    {
       AccMstDL _dac = new AccMstDL();
        
        public List<m_acc_master> GetAccountMaster(m_acc_master mum)
        {
           return _dac.GetAccountMaster(mum);
        }

        public List<mm_role_permission> GetRolePermission(p_gen_param mum)
        {
            return _dac.GetRolePermission(mum);
        }      

        public List<mm_role_permission> GetRoleMaster(p_gen_param mum)
        {
            return _dac.GetRoleMaster(mum);
        }

        public int InsertRolePermission(mm_role_permission mum)
        {
            return _dac.InsertRolePermission(mum);
        }

        public int UpdateRolePermission(mm_role_permission mum)
        {
            return _dac.UpdateRolePermission(mum);
        }
        public List<mm_acc_type> GetAccountTypeMaster()
        {
           return _dac.GetAccountTypeMaster();
        }
        public List<mm_constitution> GetConstitution()
        {
           return _dac.GetConstitution();
        }
         public List<mm_oprational_intr> GetOprationalInstr()
        {
           return _dac.GetOprationalInstr();
        }
        
        public List<m_user_master> GetUserDtls(m_user_master mum)
        {
           return _dac.GetUserDtls(mum);
        }

        public List<m_user_type> GetUserType(m_user_type mum)
        {
           return _dac.GetUserType(mum);
        }

        public int UpdateUserstatus(m_user_master mum)
         {
           return _dac.UpdateUserstatus(mum);
        }
        
        public int UpdateBankInv(mm_bank_inv mum)
        {
            return _dac.UpdateBankInv(mum);
        }

        public int UpdateBranchInv(mm_branch_inv mum)
        {
            return _dac.UpdateBranchInv(mum);
        }

        public List<mm_category> GetCategoryMaster()
        {
           return _dac.GetCategoryMaster();
        }
        public List<mm_state> GetStateMaster()
        {
           return _dac.GetStateMaster();
        }
        public List<mm_dist> GetDistMaster()
        {
           return _dac.GetDistMaster();
        }
        public List<mm_vill> GetVillageMaster(mm_vill mum)
        {
           return _dac.GetVillageMaster(mum);
        }

        public List<mm_po> GetPoMaster(mm_po mum)
        {
            return _dac.GetPoMaster(mum);
        }

        public List<mm_ps> GetPsMaster(mm_ps mum)
        {
            return _dac.GetPsMaster(mum);
        }

        public List<mm_bank_inv> GetBankInvMaster(mm_bank_inv mum)
        {
            return _dac.GetBankInvMaster(mum);
        }

        public List<mm_branch_inv> GetBranchInvMaster(mm_branch_inv mum)
        {
            return _dac.GetBranchInvMaster(mum);
        }
        public List<mm_service_area> GetServiceAreaMaster(mm_service_area mum)
        {
           return _dac.GetServiceAreaMaster(mum);
        }
        public List<mm_block> GetBlockMaster(mm_block mum)
        {
           return _dac.GetBlockMaster(mum);
        }

        public List<m_acc_master> GetAccGlhead(m_acc_master mum)
        {
            return _dac.GetAccGlhead(mum);
        }

        public int InsertServiceAreaMaster(mm_service_area mum)
        {
            return _dac.InsertServiceAreaMaster(mum);
        }

        public int InsertVillageMaster(mm_vill mum)
        {
            return _dac.InsertVillageMaster(mum);
        }

        public int InsertPoMaster(mm_po mum)
        {
            return _dac.InsertPoMaster(mum);
        }

        public int InsertPsMaster(mm_ps mum)
        {
            return _dac.InsertPsMaster(mum);
        }

        public int InsertBankInvMaster(mm_bank_inv mum)
        {
            return _dac.InsertBankInvMaster(mum);
        }

        public int InsertBranchInvMaster(mm_branch_inv mum)
        {
            return _dac.InsertBranchInvMaster(mum);
        }

        public int InsertBlockMaster(mm_block mum)
        {
            return _dac.InsertBlockMaster(mum);
        }        
        public int UpdateBlock(mm_block mum)
        {
            return _dac.UpdateBlock(mum);
        }

        public int InsertAccGlHead(m_acc_master mum)
        {
            return _dac.InsertAccGlHead(mum);
        }
        public int UpdateAccGlHead(m_acc_master mum)
        {
            return _dac.UpdateAccGlHead(mum);
        }

        public int UpdateServiceArea(mm_service_area mum)
        {
            return _dac.UpdateServiceArea(mum);
        }
        
        public day_initialize GetSystemDate(m_branch mum)
        {
            return _dac.GetSystemDate(mum);
        }

        public int UpdateVillage(mm_vill mum)
        {
            return _dac.UpdateVillage(mum);
        }

        public int UpdatePo(mm_po mum)
        {
            return _dac.UpdatePo(mum);
        }

        public int UpdatePs(mm_ps mum)
        {
            return _dac.UpdatePs(mum);
        }

        public List<mm_kyc> GetKycMaster()
        {
           return _dac.GetKycMaster();
        }
        public List<mm_title> GetTitleMaster()
        {
           return _dac.GetTitleMaster();
        }
         public List<m_branch> GetBranchMaster(m_branch mum)
        {
           return _dac.GetBranchMaster(mum);
        }

        public List<mm_ardb> GetARDBMaster()
        {
           return _dac.GetARDBMaster();
        }
        public List<sm_parameter> GetSystemParameter()
        {
           return _dac.GetSystemParameter();
        }
        internal List<mm_operation> GetOperationMaster()
        {
            return _dac.GetOperationMaster();
        }
        internal List<mm_operation> GetOperationDtls()
        {
            return _dac.GetOperationDtls();
        }
        
        internal List<mm_instalment_type> GetInstalmentTypeMaster()
        {
            return _dac.GetInstalmentTypeMaster();
        }

        internal List<mm_crop> GetCropMaster()
        {
            return _dac.GetCropMaster();
        }

        internal List<mm_activity> GetActivityMaster()
        {
            return _dac.GetActivityMaster();
        }

        internal List<mm_sector> GetSectorMaster()
        {
            return _dac.GetSectorMaster();
        }
        
          
        internal int UpdateSystemParameter(sm_parameter smParam)
        {
            return _dac.UpdateSystemParameter(smParam);
        }

       

    }
}