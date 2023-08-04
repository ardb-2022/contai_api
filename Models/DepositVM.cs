using System;

namespace SBWSDepositApi.Models
{
    public sealed class DepositVM
    {
        public td_accholder td_accholder { get; set; }
        public td_introducer td_introducer { get; set; }
        public td_nominee td_nominee { get; set; }
        public td_signatory td_signatory { get; set; }
        public tm_deposit tm_deposit { get; set; }
    }
}