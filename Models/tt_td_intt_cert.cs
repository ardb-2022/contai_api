using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SBWSFinanceApi.Models
{
	public class tt_td_intt_cert
	{
		public string acc_type_cd { get; set;}

		public string acc_num { get; set;}
		public string cust_name {get;set;}

		public string guardian_name { get; set;}

		public string address { get; set;}

		public DateTime opening_dt { get; set;}
		public DateTime mat_dt { get; set;}

		public decimal prn_amt { get; set;}
		public Single intt_rt { get; set; }

		public decimal prov_intt_amt { get; set;}
		public DateTime acc_close_dt { get; set; }
		
	}
}
