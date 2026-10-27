using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MailConsumerRabbitMQ.Helper
{
    public static class HelperGmail
    {
        public static bool IsGmailAuthBlocked(Exception ex)
        {
            var s = ex.ToString();

            return
                s.Contains("4.7.0", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("Too many login attempts", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("5.7.9", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("WebLoginRequired", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("Please log in with your web browser", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("Cannot authenticate due to a temporary system problem", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("Application-specific password required", StringComparison.OrdinalIgnoreCase) ||
                s.Contains("Username and Password not accepted", StringComparison.OrdinalIgnoreCase);
        }
    }
}
