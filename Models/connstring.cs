namespace SBWSFinanceApi.Models
{
    /// Model to depict connection string object
    ///
    public class connstring
    {
        // DB server name
        public string Server { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
        public string SigUserId { get; set; }
        public string SigPassword { get; set; }
    }
}