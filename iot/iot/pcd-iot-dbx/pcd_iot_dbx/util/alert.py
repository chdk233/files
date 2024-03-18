from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

def send_email(email_from, email_to, email_subject, email_body):# pragma: no cover
    recipients = [x.strip() for x in email_to.split(',')]
    msg = MIMEMultipart()
    msg['Subject'] = email_subject
    msg['From'] = email_from
    msg['To'] = ','.join(recipients)
    msg.preamble = email_subject

    body = MIMEText(email_body, 'html')
    msg.attach(body)

    s = smtplib.SMTP('mail-gw.ent.nwie.net')
    s.sendmail(email_from, recipients, msg.as_string())
    s.quit()