
using System;
using System.Collections.Generic;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.DL;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.LL
{
    public class UserLL
    {
       UserDL _dac = new UserDL(); 
       internal List<m_user_master> GetUserIDDtls(m_user_master mum)
        {         
           
            return _dac.GetUserIDDtls(mum);
        }  
        internal int DeleteUserMaster(m_user_master mum)
        { 
            return _dac.DeleteUserMaster(mum);
        }
        internal int UpdateUserMaster(m_user_master mum)
        {
             return _dac.UpdateUserMaster(mum);
        }
        
        internal int UpdateUserIdStatus(List<m_user_master> mum)
        {
            return _dac.UpdateUserIdStatus(mum);
        }

        internal List<m_user_master> GetUserIDStatus(m_user_master mum)
        {

            return _dac.GetUserIDStatus(mum);
        }

        internal List<m_user_master> GetUserIDStatusAll(m_user_master mum)
        {

            return _dac.GetUserIDStatusAll(mum);
        }

        internal int InsertUserMaster(m_user_master mum)
       {
            return _dac.InsertUserMaster(mum);
       }

        internal int InsertUserTransfer(td_user_transfer mum)
        {
            return _dac.InsertUserTransfer(mum);
        }

        internal int ApproveUserTransfer(td_user_transfer mum)
        {
            return _dac.ApproveUserTransfer(mum);
        }

    }
}