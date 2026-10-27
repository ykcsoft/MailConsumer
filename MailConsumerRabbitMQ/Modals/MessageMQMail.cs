using MimeKit;


namespace MailConsumerRabbitMQ.Modals
{
    public class MessageMQMail
    {

        public string InternalMailAddress { get; set; }
        public string InternalConnectionKey { get; set; }
        public string InternalSmpt { get; set; }
        public int InternalPort { get; set; }
        public bool InternalSsl { get; set; }
        public string ExternalMailAddress { get; set; }
        public string ExternalConnectionKey { get; set; }
        public string ExternalSmpt { get; set; }
        public int ExternalPort { get; set; }
        public bool ExternalSsl { get; set; }
        public MailVM MailVM { get; set; }
        public MailVmMultiple MailMultiVM { get; set; }
        public List<string> fileByteArrays { get; set; }
        public List<string> fileFullName { get; set; }
    }
    public class MessageMQMailMultiple
    {


        public string InternalMailAddress { get; set; }
        public string InternalConnectionKey { get; set; }
        public string InternalSmpt { get; set; }
        public int InternalPort { get; set; }
        public bool InternalSsl { get; set; }
        public string ExternalMailAddress { get; set; }
        public string ExternalConnectionKey { get; set; }
        public string ExternalSmpt { get; set; }
        public int ExternalPort { get; set; }
        public bool ExternalSsl { get; set; }
        public MailVmMultiple MailMultiVM { get; set; }
        public MailVM MailVM { get; set; }
        public List<string> fileByteArrays { get; set; }
        public List<string> fileFullName { get; set; }
    }
}
