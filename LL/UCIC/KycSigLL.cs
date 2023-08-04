using System.IO;
using SBWSFinanceApi.Models;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using SBWSFinanceApi.DL;


namespace SBWSFinanceApi.LL
{
    public class KycSigLL
    {
      KycSigDL ksd = new KycSigDL();
        internal kyc_sig WriteKycSig(kyc_sig ks)
        {

            // ks.img_cont = ks.img_cont.Substring(ks.img_cont.IndexOf(',') + 1);
            //ks.img_cont_byte = System.Convert.FromBase64String(ks.img_cont);
            return  ksd.WriteKycSig(ks);
        }

        internal kyc_sig ReadKycSig(kyc_sig ks)
        {
            return ksd.ReadKycSig(ks);
        }

    }
}