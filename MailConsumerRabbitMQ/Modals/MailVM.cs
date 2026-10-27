using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MailConsumerRabbitMQ.Modals
{
    public class MailVM
    {//
        public Guid MailId { get; set; }
        /// <summary>
        /// Gets or Sets the subjects.
        /// </summary>
        /// <value>A string.</value>
        public string Subject { get; set; }
        /// <summary>
        /// Gets or Sets the converts to.
        /// </summary>
        /// <value>A string.</value>
        public string To { get; set; }
        /// <summary>
        /// Gets or Sets the body.
        /// </summary>
        /// <value>A string.</value>
        public string Body { get; set; }
        /// <summary>
        /// Gets or Sets a value indicating whether body is html.
        /// </summary>
        /// <value>A bool.</value>
        public bool IsBodyHtml { get; set; }
        /// <summary>
        /// Gets or Sets the cc.
        /// </summary>
        /// <value>A string.</value>
        public string Cc { get; set; }
        /// <summary>
        /// Gets or Sets the bcc.
        /// </summary>
        /// <value>A string.</value>
        public string Bcc { get; set; }
        /// <summary>
        /// Gets or Sets the files.
        /// </summary>
        /// <value>A list of iformfiles.</value>
        public List<IFormFile> Files { get; set; }

        /// <summary>
        /// Name
        /// </summary>
        public string BoxName { get; set; } = "user";
        public Guid ProductId { get; set; }
    }
    public class MailVmMultiple : MailVM
    {
        /// <summary>
        /// Gets or Sets the to multiple.
        /// </summary>
        /// <value>A list of strings.</value>
        public List<string> ToMultiple { get; set; }
        public List<(string, string)> ToMultipleBoxAdress { get; set; }
    }
}
