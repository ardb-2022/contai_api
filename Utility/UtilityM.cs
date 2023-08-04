using System;

namespace SBWSFinanceApi.Utility
{
    internal static class UtilityM
    {
        internal static T CheckNull<T>(object obj)
        {
            return (obj == DBNull.Value ? default(T) : (T)obj);
        }
    }
}