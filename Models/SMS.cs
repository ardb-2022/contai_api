namespace SBWSAdminApi.Models
{
    public class SMSDtls
    {
        public string Mobile { get; set; }
        public string Text { get; set; }
        public string AccountNumber { get; set; }
        public string OTP { get; set; }
        public string Key { get; set; }
        public int bankConfigId { get; set; }
    }
}