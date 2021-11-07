import base64
import smtplib
import requests
from email.message import EmailMessage


# Arguments
EMAIL_ADDRESS = 'email@email.com'
contacts = ['contact@email.com']
PATH_TO_CSV_FILE = 'path of file attachment'
FILE_NAME = 'file name attachment'
SMTP_SEVER = 'smtp.com'
password = 'smtp server password'
URL_GET_FILE = 'url_get_file'


def send_mail():
    message = EmailMessage()
    message['Subject'] = 'DemoLabs Reservations Monthly Report'
    message['From'] = EMAIL_ADDRESS
    message['To'] = contacts

    # create message body as html
    message.add_alternative("""\
    <!DOCTYPE html>
    <html>
        email body
    </html>
    """, subtype='html')

    # get report csv file from web server
    result = requests.get(URL_GET_FILE)

    # get report csv file from local disk
    # with open(PATH_TO_CSV_FILE + FILE_NAME, 'rb') as f:  # new
    #     file_data = f.read()

    # add report csv file as attachment to message
    message.add_attachment(result.content, maintype='application', subtype='octet-stream', filename=FILE_NAME)

    # send message via smtp
    with smtplib.SMTP(host=SMTP_SEVER, port=587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(EMAIL_ADDRESS, password)
        smtp.send_message(message)
        smtp.quit()


if __name__ == "__main__":
    send_mail()
