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
    public class LockerOpenLL
    {
        LockerOpenDL _dac = new LockerOpenDL();
        internal List<mm_locker> GetLockerMaster(p_report_param td)
        {
            return _dac.GetLockerMaster(td);
        }
        internal int InsertLockerMaster(List<mm_locker> td)
        {
            return _dac.InsertLockerMaster(td);
        }

        internal List<locker_rent> GetLockerRent()
        {
            return _dac.GetLockerRent();
        }

        internal List<locker_rent> GetLockerRentlist()
        {
            return _dac.GetLockerRentlist();
        }
        
        internal List<td_def_trans_trf> GetUnapprovedLockerTrans(tm_locker td)
        {
            return _dac.GetUnapprovedLockerTrans(td);
        }

        internal LockerOpenDM GetLockerOpeningData(tm_locker td)
        {
            return _dac.GetLockerOpeningData(td);
        }

        internal LockerOpenDM GetLockerOpeningDataView(tm_locker td)
        {
            return _dac.GetLockerOpeningDataView(td);
        }

        internal string InsertLockerOpeningData(LockerOpenDM td)
        {
            return _dac.InsertLockerOpeningData(td);
        }

        internal int UpdateLockerOpeningData(LockerOpenDM td)
        {
            return _dac.UpdateLockerOpeningData(td);
        }

        internal int DeleteLockerOpeningData(td_def_trans_trf td)
        {
            return _dac.DeleteLockerOpeningData(td);
        }

        internal string ApproveLockerTranaction(p_gen_param td)
        {
            return _dac.ApproveLockerTranaction(td);
        }


        internal tm_locker GetLockerDtls(tm_locker td)
        {
            return _dac.GetLockerDtls(td);
        }
        
        internal tm_locker GetLockerDtlsView(tm_locker td)
        {
            return _dac.GetLockerDtlsView(td);
        }
        internal int InsertLockerRentMaster(List<locker_rent>  td)
        {
            return _dac.InsertLockerRentMaster(td);
        }

        internal List<locker_dtls_report> PopulateLockerDtlsRep(p_report_param td)
        {
            return _dac.PopulateLockerDtlsRep(td);
        }
        internal List<tm_locker> PopulateLockerHistoryRep(p_report_param td)
        {
            return _dac.PopulateLockerHistoryRep(td);
        }
        internal List<tm_locker> PopulateLockerShouldBeRenewedRep(p_report_param td)
        {
            return _dac.PopulateLockerShouldBeRenewedRep(td);
        }
    }
}
