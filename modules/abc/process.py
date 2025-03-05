import base64
import os
import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json')
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('YOUR_DOWNLOADED_JSON_FILE.json', SCOPES)
            creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_attachments(service, start_date, end_date):
    query = f'after:{start_date} before:{end_date} has:attachment'
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()
        for part in msg['payload']['parts']:
            if 'filename' in part and part['filename']:
                data = part['body']['data']
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                with open(part['filename'], 'wb') as f:
                    f.write(file_data)

if __name__ == '__main__':
    service = authenticate()
    start_date = datetime.date(2023, 1, 1)  # 例: 2023年1月1日
    end_date = datetime.date(2023, 12, 31)  # 例: 2023年12月31日
    get_attachments(service, start_date, end_date)