using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using SBWSFinanceApi.Config;
using SBWSFinanceApi.Models;
using SBWSFinanceApi.Utility;

namespace SBWSFinanceApi.DL
{
    public class KycSigDL
    {
        public kyc_sig WriteKycSig(kyc_sig ks)
        {

            kyc_sig retKyc = new kyc_sig();
            if(String.IsNullOrEmpty(ks.img_cont))
            {
                return null;
            }

            string _statement1 = "";
            string _statement2 = "";
            string _statement3 = "";

            decimal ret = 0;
            if (ks.cust_cd == null || ks.cust_cd == 0)
            {
                retKyc.status = "No Customer code provided";
                return retKyc;
            }

            string _checkSig = "SELECT COUNT(*) REC_COUNT FROM TM_SIGNATURE"
                             + " WHERE ARDB_CD = {0} AND CUST_CD = {1}";
            string _insertSig = "INSERT INTO TM_SIGNATURE(ARDB_CD,CUST_CD, IMG_PHOTO , IMG_SIG , CREATED_BY, CREATED_DT) VALUES({0},{1}, EMPTY_BLOB(), EMPTY_BLOB() , '{2}', to_date('{3}','dd-mm-yyyy') )";
            string _updateSig = "UPDATE TM_SIGNATURE SET IMG_SIG= :1 "
                                      + " WHERE ARDB_CD = :2 AND CUST_CD = :3";

            string _updatePhoto = "UPDATE TM_SIGNATURE SET IMG_PHOTO= :1 "
                                      + " WHERE ARDB_CD = :2 AND CUST_CD = :3 ";

            string _checkKyc = "SELECT COUNT(*) REC_COUNT FROM TM_KYC"
                                    + " WHERE ARDB_CD = {0} AND CUST_CD = {1}";
            string _insertKyc = "INSERT INTO TM_KYC(ARDB_CD,CUST_CD, CREATED_BY, CREATED_DT) VALUES({0},{1},'{2}', to_date('{3}','dd-mm-yyyy') )";
            string _updateKyc = "UPDATE TM_KYC SET IMG_PHOTO= :1 "
                             + " WHERE ARDB_CD = :2 AND CUST_CD = :3 ";
            string _updateAddr = "UPDATE TM_KYC SET IMG_ADDRESS= :1 "
                             + " WHERE ARDB_CD = :2 AND CUST_CD = :3 ";

            if (ks.img_typ.Equals("PHOTO") || ks.img_typ.Equals("SIGNATURE"))
            {
                _statement1 = string.Format(_checkSig,
                                            ks.ardb_cd,
                                            ks.cust_cd);

                _statement2 = string.Format(_insertSig,
                                            ks.ardb_cd,
                                            ks.cust_cd,
                                            ks.created_by,
                                            ks.created_dt);
                if (ks.img_typ.Equals("SIGNATURE"))
                {
                    _statement3 = _updateSig;
                }

                if (ks.img_typ.Equals("PHOTO"))
                {
                    _statement3 = _updatePhoto;
                }
            }

            if (ks.img_typ.Equals("KYC") || ks.img_typ.Equals("ADDRESS"))
            {
                _statement1 = string.Format(_checkKyc,
                                            ks.ardb_cd,
                                            ks.cust_cd);

                _statement2 = string.Format(_insertKyc,
                                            ks.ardb_cd,
                                            ks.cust_cd,
                                            ks.created_by,
                                            ks.created_dt);
                if (ks.img_typ.Equals("KYC"))
                {
                    _statement3 = _updateKyc;
                }

                if (ks.img_typ.Equals("ADDRESS"))
                {
                    _statement3 = _updateAddr;
                }
            }


            using (var connection = OrclDbConnection.NewConnection2)
            {
                using (var command = OrclDbConnection.Command(connection, _statement1))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                ret = UtilityM.CheckNull<decimal>(reader["REC_COUNT"]);
                            }
                        }
                    }
                }

                if (ret == 0)
                {
                    using (var command = OrclDbConnection.Command(connection, _statement2))
                    {  using (var reader = command.ExecuteReader())
                        { ret = 1; }
                    }
                }


                using (var cmd = OrclDbConnection.Command(connection, _statement3))  
                    {  
                        cmd.Connection = connection; 
                        cmd.CommandType = System.Data.CommandType.Text;
                        var parm = new OracleParameter(":1", OracleDbType.Blob, ParameterDirection.Input);
                        parm.Value = System.Convert.FromBase64String(ks.img_cont.Substring(23));
                        cmd.Parameters.Add(parm);

                        parm = new OracleParameter(":2", OracleDbType.Varchar2, ParameterDirection.Input);
                        parm.Value = ks.ardb_cd;
                        cmd.Parameters.Add(parm);

                        parm = new OracleParameter(":3", OracleDbType.Int32, ParameterDirection.Input);
                        parm.Value = ks.cust_cd;
                        cmd.Parameters.Add(parm);
                        // cmd.Parameters.Add(":1", ks.img_cont_byte);  
                        // cmd.Parameters.Add(":2", ks.cust_cd);   
 
                        cmd.ExecuteNonQuery();
                        connection.Close();  
                    }

            }

            retKyc.ardb_cd = ks.ardb_cd;
            retKyc.cust_cd = ks.cust_cd;
            retKyc.img_typ = ks.img_typ;
            retKyc.status = "Record Inserted Successfully";
            return retKyc;
        }

        //===========================================================================================
        internal kyc_sig ReadKycSig(kyc_sig ks)
        {
            kyc_sig retKyc = new kyc_sig();
            string _statement = "";

            if (ks.cust_cd == null || ks.cust_cd == 0)
            {
                retKyc.status = "No Customer code provided";
                return retKyc;
            }

            string _getPhoto = "SELECT IMG_PHOTO PHOTO FROM TM_SIGNATURE WHERE ARDB_CD = {0} AND CUST_CD = {1} ";
            string _getSig = "SELECT IMG_SIG PHOTO FROM TM_SIGNATURE WHERE ARDB_CD = {0} AND CUST_CD = {1}";

            string _getKyc = "SELECT IMG_PHOTO PHOTO FROM TM_KYC WHERE ARDB_CD = {0} AND CUST_CD = {1}";
            string _getAddress = "SELECT IMG_ADDRESS PHOTO FROM TM_KYC WHERE  ARDB_CD = {0} AND CUST_CD = {1}";

            if (ks.img_typ.Equals("PHOTO") || ks.img_typ.Equals("SIGNATURE"))
            {
                if (ks.img_typ.Equals("PHOTO"))
                {
                    _statement = string.Format(_getPhoto,
                                                ks.ardb_cd,
                                                ks.cust_cd);
                }

                if (ks.img_typ.Equals("SIGNATURE"))
                {
                    _statement = string.Format(_getSig,
                                                ks.ardb_cd,
                                                ks.cust_cd);
                }
            }

            if (ks.img_typ.Equals("KYC") || ks.img_typ.Equals("ADDRESS"))
            {
                if (ks.img_typ.Equals("KYC"))
                {
                    _statement = string.Format(_getKyc,
                                               ks.ardb_cd, 
                                               ks.cust_cd);
                }

                if (ks.img_typ.Equals("ADDRESS"))
                {
                    _statement = string.Format(_getAddress,
                                               ks.ardb_cd,
                                               ks.cust_cd);
                }
            }


            using (var connection = OrclDbConnection.NewConnection2)
            {
                using (var command = OrclDbConnection.Command(connection, _statement))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var img_cont_byte = UtilityM.CheckNull<byte[]>(reader["PHOTO"]);
                                retKyc.img_cont = Convert.ToBase64String(img_cont_byte);
                            }
                        }
                    }
                }

            }

            retKyc.ardb_cd = ks.ardb_cd;
            retKyc.cust_cd = ks.cust_cd;
            retKyc.img_typ = ks.img_typ;
            retKyc.status = "Record Fetched Successfully";
            return retKyc;
        }

    }
}
