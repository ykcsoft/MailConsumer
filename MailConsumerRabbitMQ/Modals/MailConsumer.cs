using System.Collections.Concurrent;
using System.Text;
using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using MimeKit;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MailConsumerRabbitMQ.Modals
{
    public class MailConsumer
    {
        private ConnectionFactory _factory;
        private IConnection _connection;
        private IChannel _channel;

        public int sayac = 1;

        // SMTP session cache: host + port + user
        private readonly ConcurrentDictionary<string, SmtpSession> _smtpSessions = new();

        // SMTP temporary block/circuit breaker: host + port + user
        private readonly ConcurrentDictionary<string, DateTime> _smtpBlockedUntilUtc = new();

        // Queue names
        private const string Q_MAIN = "mail_queue";
        private const string Q_RETRY_5M = "mail_retry_5m";

        public async Task InitializeAsync()
        {
            _factory = new ConnectionFactory
            {
                Port = 5672,
                HostName = "c_rabbitmq",
                UserName = "user",
                Password = "1234567",
            };

            _connection = await _factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();

            // MAIN QUEUE
            await _channel.QueueDeclareAsync(
                queue: Q_MAIN,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            // RETRY QUEUE: 5 dakika bekler, sonra ana kuyruğa geri döner
            await _channel.QueueDeclareAsync(
                queue: Q_RETRY_5M,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    ["x-message-ttl"] = 300_000,
                    ["x-dead-letter-exchange"] = "",
                    ["x-dead-letter-routing-key"] = Q_MAIN
                });

            // Aynı anda çok mail çekip SMTP hesabına abanmasın
            await _channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false);
        }

        public void StartListening()
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);

            consumer.ReceivedAsync += async (_, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body.ToArray());

                try
                {
                    var mailSingle = JsonConvert.DeserializeObject<MessageMQMail>(message);

                    if (mailSingle != null && mailSingle.MailVM != null)
                    {
                        LogMailMq("SINGLE-RECEIVED", mailSingle);
                        await SendEmail(mailSingle);
                    }
                    else
                    {
                        var mailMultiple = JsonConvert.DeserializeObject<MessageMQMailMultiple>(message);

                        if (mailMultiple == null || mailMultiple.MailMultiVM == null)
                            throw new InvalidOperationException("MQ message parse edildi ama MailVM / MailMultiVM bulunamadı.");

                        LogMailMq("MULTI-RECEIVED", mailMultiple);
                        await SendEmailMultiple(mailMultiple);
                    }

                    await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    var retryCount = ReadRetryCount(ea.BasicProperties?.Headers);

                    Console.WriteLine(
                        $"[MAIL-ERROR] retry={retryCount} err={ex.GetType().Name} msg={ex.Message}"
                    );

                    if (retryCount == 0)
                    {
                        Console.WriteLine("[MAIL-RETRY] İlk hata. Mesaj 5 dakika sonra tekrar denenecek.");

                        await PublishWithRetryHeaderAsync(
                            routingKey: Q_RETRY_5M,
                            body: ea.Body,
                            originalProps: ea.BasicProperties,
                            retryCount: 1);

                        // Ana mesajı ACK'liyoruz ki anında tekrar tekrar dönmesin
                        await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                        return;
                    }

                    // İkinci kez de başarısız olduysa sistemden çıkar
                    Console.WriteLine("[MAIL-DROP] İkinci deneme de başarısız. Mesaj siliniyor, tekrar kuyruğa alınmayacak.");

                    await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                }
            };

            _ = _channel.BasicConsumeAsync(
                queue: Q_MAIN,
                autoAck: false,
                consumer: consumer);

            Console.WriteLine("MailConsumer çalışıyor, mail kuyruğu dinleniyor...");
        }

        // -------------------------
        // SMTP session wrapper
        // -------------------------
        private sealed class SmtpSession : IAsyncDisposable
        {
            public readonly SmtpClient Client = new();
            public readonly SemaphoreSlim Gate = new(1, 1);

            public async ValueTask DisposeAsync()
            {
                try
                {
                    if (Client.IsConnected)
                        await Client.DisconnectAsync(true);
                }
                catch
                {
                    // ignore
                }

                Client.Dispose();
                Gate.Dispose();
            }
        }

        private static string SessionKey(string host, int port, string user)
            => $"{host}:{port}:{user}".ToLowerInvariant();

        private static SecureSocketOptions ResolveSocketOptions(string host, int port, bool sslFlag)
        {
            if (port == 465)
                return SecureSocketOptions.SslOnConnect;

            if (port == 587)
                return SecureSocketOptions.StartTls;

            return sslFlag
                ? SecureSocketOptions.SslOnConnect
                : SecureSocketOptions.StartTlsWhenAvailable;
        }

        private static bool IsGmailTooManyLogin(Exception ex)
        {
            var s = ex.ToString();

            return s.Contains("4.7.0", StringComparison.OrdinalIgnoreCase) &&
                   (
                       s.Contains("Too many login attempts", StringComparison.OrdinalIgnoreCase) ||
                       s.Contains("Cannot authenticate due to a temporary system problem", StringComparison.OrdinalIgnoreCase) ||
                       s.Contains("temporar", StringComparison.OrdinalIgnoreCase)
                   );
        }

        private static bool IsCircuitBreakException(Exception ex)
        {
            return ex is SmtpProtocolException sp &&
                   sp.Message.StartsWith("CircuitBreak:", StringComparison.OrdinalIgnoreCase);
        }

        private async Task<SmtpSession> GetOrCreateSessionAsync(string host, int port, string user)
        {
            var key = SessionKey(host, port, user);
            return _smtpSessions.GetOrAdd(key, _ => new SmtpSession());
        }

        private async Task EnsureConnectedAndAuthedAsync(
            SmtpSession session,
            string host,
            int port,
            bool ssl,
            string user,
            string pass)
        {
            if (!session.Client.IsConnected)
            {
                session.Client.CheckCertificateRevocation = true;
                session.Client.Timeout = 60_000;

                // App Password kullanıyorsan XOAUTH2 denemesin
                session.Client.AuthenticationMechanisms.Remove("XOAUTH2");

                var options = ResolveSocketOptions(host, port, ssl);

                await session.Client.ConnectAsync(host, port, options);
            }

            if (!session.Client.IsAuthenticated)
            {
                await session.Client.AuthenticateAsync(user, pass);
            }
        }

        private async Task SendWithSessionAsync(
            string host,
            int port,
            bool ssl,
            string user,
            string pass,
            MimeMessage mimeMessage,
            string logPrefix)
        {
            var key = SessionKey(host, port, user);

            if (_smtpBlockedUntilUtc.TryGetValue(key, out var untilUtc) && untilUtc > DateTime.UtcNow)
            {
                Console.WriteLine($"[CircuitBreak-HIT] smtpUser={user} host={host}:{port} blockedUntil={untilUtc:o}");
                throw new SmtpProtocolException($"CircuitBreak: SMTP blocked until {untilUtc:o}");
            }

            var session = await GetOrCreateSessionAsync(host, port, user);

            await session.Gate.WaitAsync();

            try
            {
                await EnsureConnectedAndAuthedAsync(session, host, port, ssl, user, pass);
                await session.Client.SendAsync(mimeMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{logPrefix} err={ex.GetType().Name} msg={ex.Message}");

                if (IsGmailTooManyLogin(ex) || IsCircuitBreakException(ex) || !session.Client.IsConnected)
                {
                    _smtpBlockedUntilUtc[key] = DateTime.UtcNow.AddMinutes(10);

                    _smtpSessions.TryRemove(key, out _);

                    try
                    {
                        await session.DisposeAsync();
                    }
                    catch
                    {
                        // ignore
                    }

                    Console.WriteLine($"[CircuitBreak-SET] smtpUser={user} host={host}:{port} blockedFor=10m");
                }

                throw;
            }
            finally
            {
                session.Gate.Release();
            }
        }

        // -------------------------
        // RabbitMQ helpers
        // -------------------------
        private static int ReadRetryCount(IDictionary<string, object>? headers)
        {
            if (headers == null)
                return 0;

            if (!headers.TryGetValue("x-retry", out var v))
                return 0;

            try
            {
                if (v is byte[] b)
                {
                    var s = Encoding.UTF8.GetString(b);
                    return int.TryParse(s, out var n) ? n : 0;
                }

                if (v is int i)
                    return i;

                if (v is long l)
                    return (int)l;

                if (v is string str)
                    return int.TryParse(str, out var n) ? n : 0;
            }
            catch
            {
                // ignore
            }

            return 0;
        }

        private async Task PublishWithRetryHeaderAsync(
            string routingKey,
            ReadOnlyMemory<byte> body,
            IReadOnlyBasicProperties? originalProps,
            int retryCount)
        {
            var props = new BasicProperties
            {
                Persistent = true,
                ContentType = originalProps?.ContentType ?? "application/json"
            };

            var headers = new Dictionary<string, object>();

            if (originalProps?.Headers != null)
            {
                foreach (var kv in originalProps.Headers)
                    headers[kv.Key] = kv.Value;
            }

            headers["x-retry"] = Encoding.UTF8.GetBytes(retryCount.ToString());

            props.Headers = headers;

            await _channel.BasicPublishAsync(
                exchange: "",
                routingKey: routingKey,
                mandatory: false,
                basicProperties: props,
                body: body);
        }

        // -------------------------
        // Log helpers
        // -------------------------
        private static string MaskSecret(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "";

            if (value.Length <= 6)
                return "***";

            return value.Substring(0, 3) + "***" + value.Substring(value.Length - 3);
        }

        private static string SafeLog(string? value, int max = 180)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "";

            value = value
                .Replace("\r", " ")
                .Replace("\n", " ")
                .Replace("\t", " ")
                .Trim();

            return value.Length > max
                ? value.Substring(0, max) + "..."
                : value;
        }

        private static void LogMailMq(string tag, MessageMQMail mail)
        {
            Console.WriteLine(
                $"[{tag}] " +
                $"mailId={mail.MailVM?.MailId} " +
                $"to={SafeLog(mail.MailVM?.To)} " +
                $"cc={SafeLog(mail.MailVM?.Cc)} " +
                $"bcc={SafeLog(mail.MailVM?.Bcc)} " +
                $"subject=\"{SafeLog(mail.MailVM?.Subject)}\" " +
                $"boxName=\"{SafeLog(mail.MailVM?.BoxName)}\" " +
                $"isHtml={mail.MailVM?.IsBodyHtml} " +

                $"externalUser={SafeLog(mail.ExternalMailAddress)} " +
                $"externalSmtp={SafeLog(mail.ExternalSmpt)} " +
                $"externalPort={mail.ExternalPort} " +
                $"externalSsl={mail.ExternalSsl} " +
                $"externalKey={MaskSecret(mail.ExternalConnectionKey)} " +

                $"internalUser={SafeLog(mail.InternalMailAddress)} " +
                $"internalSmtp={SafeLog(mail.InternalSmpt)} " +
                $"internalPort={mail.InternalPort} " +
                $"internalSsl={mail.InternalSsl} " +
                $"internalKey={MaskSecret(mail.InternalConnectionKey)} " +

                $"fileCount={mail.fileByteArrays?.Count ?? 0}"
            );
        }

        private static void LogMailMq(string tag, MessageMQMailMultiple mail)
        {
            var toList = "";

            if (mail.MailMultiVM?.ToMultipleBoxAdress != null)
            {
                toList = string.Join(
                    ",",
                    mail.MailMultiVM.ToMultipleBoxAdress.Select(x => $"{x.Item1}<{x.Item2}>")
                );
            }

            Console.WriteLine(
                $"[{tag}] " +
                $"mailId={mail.MailMultiVM?.MailId} " +
                $"toCount={mail.MailMultiVM?.ToMultipleBoxAdress?.Count ?? 0} " +
                $"toList={SafeLog(toList, 400)} " +
                $"subject=\"{SafeLog(mail.MailMultiVM?.Subject)}\" " +
                $"boxName=\"{SafeLog(mail.MailMultiVM?.BoxName)}\" " +
                $"isHtml={mail.MailMultiVM?.IsBodyHtml} " +

                $"externalUser={SafeLog(mail.ExternalMailAddress)} " +
                $"externalSmtp={SafeLog(mail.ExternalSmpt)} " +
                $"externalPort={mail.ExternalPort} " +
                $"externalSsl={mail.ExternalSsl} " +
                $"externalKey={MaskSecret(mail.ExternalConnectionKey)} " +

                $"internalUser={SafeLog(mail.InternalMailAddress)} " +
                $"internalSmtp={SafeLog(mail.InternalSmpt)} " +
                $"internalPort={mail.InternalPort} " +
                $"internalSsl={mail.InternalSsl} " +
                $"internalKey={MaskSecret(mail.InternalConnectionKey)} " +

                $"fileCount={mail.fileByteArrays?.Count ?? 0}"
            );
        }

        // -------------------------
        // Attachment helpers
        // -------------------------
        private static void AddAttachmentsFromMailVm(BodyBuilder bodyBuilder, IList<IFormFile>? files)
        {
            if (files == null || files.Count == 0)
                return;

            foreach (var file in files)
            {
                if (file == null || file.Length <= 0)
                    continue;

                using var ms = new MemoryStream();
                file.CopyTo(ms);

                bodyBuilder.Attachments.Add(file.FileName, ms.ToArray());
            }
        }

        private static void FillFilesFromBase64IfAny(MessageMQMail mail)
        {
            if (mail.fileByteArrays == null || mail.fileByteArrays.Count == 0)
                return;

            if (mail.MailVM == null)
                mail.MailVM = new MailVM();

            mail.MailVM.Files = new List<IFormFile>();

            for (int i = 0; i < mail.fileByteArrays.Count; i++)
            {
                var base64File = mail.fileByteArrays[i];

                if (string.IsNullOrWhiteSpace(base64File))
                    continue;

                var fileFullName = mail.fileFullName != null && mail.fileFullName.Count > i
                    ? mail.fileFullName[i]
                    : $"attachment_{i + 1}";

                var fileBytes = Convert.FromBase64String(base64File);
                var stream = new MemoryStream(fileBytes);

                var file = new FormFile(stream, 0, fileBytes.Length, "file", fileFullName)
                {
                    Headers = new HeaderDictionary(),
                    ContentType = "application/octet-stream"
                };

                mail.MailVM.Files.Add(file);
            }
        }

        private static void FillFilesFromBase64IfAny(MessageMQMailMultiple mail)
        {
            if (mail.fileByteArrays == null || mail.fileByteArrays.Count == 0)
                return;

            if (mail.MailVM == null)
                mail.MailVM = new MailVM();

            mail.MailVM.Files = new List<IFormFile>();

            for (int i = 0; i < mail.fileByteArrays.Count; i++)
            {
                var base64File = mail.fileByteArrays[i];

                if (string.IsNullOrWhiteSpace(base64File))
                    continue;

                var fileFullName = mail.fileFullName != null && mail.fileFullName.Count > i
                    ? mail.fileFullName[i]
                    : $"attachment_{i + 1}";

                var fileBytes = Convert.FromBase64String(base64File);
                var stream = new MemoryStream(fileBytes);

                var file = new FormFile(stream, 0, fileBytes.Length, "file", fileFullName)
                {
                    Headers = new HeaderDictionary(),
                    ContentType = "application/octet-stream"
                };

                mail.MailVM.Files.Add(file);
            }
        }

        // -------------------------
        // SendEmail single
        // -------------------------
        private async Task SendEmail(MessageMQMail _mail)
        {
            var started = DateTime.Now;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            var to = SafeLog(_mail?.MailVM?.To, 200);
            var subject = SafeLog(_mail?.MailVM?.Subject, 200);

            int attCount = 0;
            long attBytes = 0;

            try
            {
                if (_mail == null)
                    throw new ArgumentNullException(nameof(_mail));

                if (_mail.MailVM == null)
                    throw new InvalidOperationException("MailVM null geldi.");

                if (string.IsNullOrWhiteSpace(_mail.MailVM.To))
                    throw new InvalidOperationException("MailVM.To boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalMailAddress))
                    throw new InvalidOperationException("ExternalMailAddress boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalConnectionKey))
                    throw new InvalidOperationException("ExternalConnectionKey boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalSmpt))
                    throw new InvalidOperationException("ExternalSmpt boş geldi.");

                var mimeMessage = new MimeMessage();

                mimeMessage.From.Add(
                    new MailboxAddress(_mail.ExternalMailAddress, _mail.ExternalMailAddress)
                );

                mimeMessage.To.Add(
                    new MailboxAddress(_mail.MailVM.BoxName ?? "user", _mail.MailVM.To)
                );

                if (!string.IsNullOrWhiteSpace(_mail.MailVM.Cc))
                    mimeMessage.Cc.Add(MailboxAddress.Parse(_mail.MailVM.Cc));

                if (!string.IsNullOrWhiteSpace(_mail.MailVM.Bcc))
                    mimeMessage.Bcc.Add(MailboxAddress.Parse(_mail.MailVM.Bcc));

                mimeMessage.Subject = _mail.MailVM.Subject ?? "";

                var bodyBuilder = new BodyBuilder();

                if (_mail.MailVM.IsBodyHtml)
                    bodyBuilder.HtmlBody = _mail.MailVM.Body ?? "";
                else
                    bodyBuilder.TextBody = _mail.MailVM.Body ?? "";

                FillFilesFromBase64IfAny(_mail);

                if (_mail.MailVM.Files != null)
                {
                    attCount = _mail.MailVM.Files.Count;

                    foreach (var f in _mail.MailVM.Files)
                        attBytes += f?.Length ?? 0;
                }

                AddAttachmentsFromMailVm(bodyBuilder, _mail.MailVM.Files);

                mimeMessage.Body = bodyBuilder.ToMessageBody();

                var host = _mail.ExternalSmpt;
                var port = _mail.ExternalPort;
                var ssl = _mail.ExternalSsl;
                var user = _mail.ExternalMailAddress;
                var pass = _mail.ExternalConnectionKey;

                await SendWithSessionAsync(
                    host,
                    port,
                    ssl,
                    user,
                    pass,
                    mimeMessage,
                    logPrefix: $"[SMTP-SINGLE] to={to} sub=\"{subject}\"");

                sw.Stop();

                Console.WriteLine(
                    $"[Success] ts={started:yyyy-MM-dd HH:mm:ss.fff} no={sayac} durMs={sw.ElapsedMilliseconds} " +
                    $"to={to} sub=\"{subject}\" smtp={host} port={port} ssl={ssl} att={attCount} bytes={attBytes}"
                );

                sayac++;
            }
            catch
            {
                sw.Stop();

                Console.WriteLine(
                    $"[Fail] ts={started:yyyy-MM-dd HH:mm:ss.fff} no={sayac} durMs={sw.ElapsedMilliseconds} " +
                    $"to={to} sub=\"{subject}\" smtp={_mail?.ExternalSmpt} port={_mail?.ExternalPort} ssl={_mail?.ExternalSsl} " +
                    $"att={attCount} bytes={attBytes}"
                );

                throw;
            }
        }

        // -------------------------
        // SendEmail multiple
        // -------------------------
        private async Task SendEmailMultiple(MessageMQMailMultiple _mail)
        {
            var started = DateTime.Now;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            var subject = SafeLog(_mail?.MailMultiVM?.Subject, 200);

            int attCount = 0;
            long attBytes = 0;

            int toCount = _mail?.MailMultiVM?.ToMultipleBoxAdress?.Count ?? 0;

            string toList = "";

            if (_mail?.MailMultiVM?.ToMultipleBoxAdress != null && _mail.MailMultiVM.ToMultipleBoxAdress.Count > 0)
            {
                toList = string.Join(",", _mail.MailMultiVM.ToMultipleBoxAdress.Select(x => x.Item2));
                toList = SafeLog(toList, 260);
            }

            try
            {
                if (_mail == null)
                    throw new ArgumentNullException(nameof(_mail));

                if (_mail.MailMultiVM == null)
                    throw new InvalidOperationException("MailMultiVM null geldi.");

                if (_mail.MailMultiVM.ToMultipleBoxAdress == null || _mail.MailMultiVM.ToMultipleBoxAdress.Count == 0)
                    throw new InvalidOperationException("ToMultipleBoxAdress boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalMailAddress))
                    throw new InvalidOperationException("ExternalMailAddress boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalConnectionKey))
                    throw new InvalidOperationException("ExternalConnectionKey boş geldi.");

                if (string.IsNullOrWhiteSpace(_mail.ExternalSmpt))
                    throw new InvalidOperationException("ExternalSmpt boş geldi.");

                var mimeMessage = new MimeMessage();

                mimeMessage.From.Add(
                    new MailboxAddress(_mail.ExternalMailAddress, _mail.ExternalMailAddress)
                );

                foreach (var item in _mail.MailMultiVM.ToMultipleBoxAdress)
                {
                    var name = string.IsNullOrWhiteSpace(item.Item1)
                        ? item.Item2
                        : item.Item1;

                    var address = item.Item2;

                    if (string.IsNullOrWhiteSpace(address))
                        continue;

                    mimeMessage.To.Add(new MailboxAddress(name, address));
                }

                if (mimeMessage.To.Count == 0)
                    throw new InvalidOperationException("Geçerli alıcı adresi bulunamadı.");

                if (!string.IsNullOrWhiteSpace(_mail.MailMultiVM.Cc))
                    mimeMessage.Cc.Add(MailboxAddress.Parse(_mail.MailMultiVM.Cc));

                if (!string.IsNullOrWhiteSpace(_mail.MailMultiVM.Bcc))
                    mimeMessage.Bcc.Add(MailboxAddress.Parse(_mail.MailMultiVM.Bcc));

                mimeMessage.Subject = _mail.MailMultiVM.Subject ?? "";

                var bodyBuilder = new BodyBuilder();

                if (_mail.MailMultiVM.IsBodyHtml)
                    bodyBuilder.HtmlBody = _mail.MailMultiVM.Body ?? "";
                else
                    bodyBuilder.TextBody = _mail.MailMultiVM.Body ?? "";

                FillFilesFromBase64IfAny(_mail);

                AddAttachmentsFromMailVm(bodyBuilder, _mail.MailVM?.Files);

                if (_mail.MailMultiVM.Files != null && _mail.MailMultiVM.Files.Count > 0)
                    AddAttachmentsFromMailVm(bodyBuilder, _mail.MailMultiVM.Files);

                if (_mail.MailVM?.Files != null)
                {
                    attCount += _mail.MailVM.Files.Count;

                    foreach (var f in _mail.MailVM.Files)
                        attBytes += f?.Length ?? 0;
                }

                if (_mail.MailMultiVM?.Files != null)
                {
                    attCount += _mail.MailMultiVM.Files.Count;

                    foreach (var f in _mail.MailMultiVM.Files)
                        attBytes += f?.Length ?? 0;
                }

                mimeMessage.Body = bodyBuilder.ToMessageBody();

                var host = _mail.ExternalSmpt;
                var port = _mail.ExternalPort;
                var ssl = _mail.ExternalSsl;
                var user = _mail.ExternalMailAddress;
                var pass = _mail.ExternalConnectionKey;

                await SendWithSessionAsync(
                    host,
                    port,
                    ssl,
                    user,
                    pass,
                    mimeMessage,
                    logPrefix: $"[SMTP-MULTI] toCount={toCount} to={toList} sub=\"{subject}\"");

                sw.Stop();

                Console.WriteLine(
                    $"[Success] ts={started:yyyy-MM-dd HH:mm:ss.fff} no={sayac} durMs={sw.ElapsedMilliseconds} " +
                    $"toCount={toCount} to={toList} sub=\"{subject}\" smtp={host} port={port} ssl={ssl} att={attCount} bytes={attBytes}"
                );

                sayac++;
            }
            catch
            {
                sw.Stop();

                Console.WriteLine(
                    $"[Fail] ts={started:yyyy-MM-dd HH:mm:ss.fff} no={sayac} durMs={sw.ElapsedMilliseconds} " +
                    $"toCount={toCount} to={toList} sub=\"{subject}\" smtp={_mail?.ExternalSmpt} port={_mail?.ExternalPort} ssl={_mail?.ExternalSsl} " +
                    $"att={attCount} bytes={attBytes}"
                );

                throw;
            }
        }
    }
}