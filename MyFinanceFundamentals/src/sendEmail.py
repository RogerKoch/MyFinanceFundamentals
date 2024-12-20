#!/usr/bin/python
# -*- coding: UTF-8 -*-


import smtplib, ssl, os

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import keyring

def sendEmail():
    subject = "Dividend financial update"
    body = """
            Hi all
            Find attached the newest update of the basic financials for the Dividend_Investments sheet.
              
            Happy day
            Roger
            """

    sender_email = "koch23roger@gmail.com"
    receiver_email = "koch23roger@gmail.com"
    password = keyring.get_password("gmail", "koch23roger@gmail.com")

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #message["Bcc"] = receiver_email  # Recommended for mass emails

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    curPath = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.join(curPath, 'export\\financials.csv')

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)
    
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )
    
    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()
    
    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
    
if __name__ == '__main__':
    sendEmail()
