import base64
import os
import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from email.utils import parsedate_to_datetime
import re
import io
from typing import Literal, Dict

from minio import Minio
from minio.error import S3Error
import json
import base64

from pydantic import BaseModel





class MockStorage:
    def __init__(self, root: str) -> None:
        self.root = root
        self.pipes = []

    def as_mock(self):
        return MockStorage(self.root)
    
    @staticmethod
    def is_unsafe_path(path):
        if ".." in path:
            return "Invalid path"
        
        if "/" == path[:1]:
            return "Invalid path"
        
        return ""

    def write_bytes(self, __data, __path, **metadata):
        is_unsafe = self.is_unsafe_path(__path)
        if is_unsafe:
            raise Exception(is_unsafe)
        
        dest = os.path.join(self.root, __path)

        print(dest, metadata)
        self._write_bytes(__data, dest, metadata)
        
        for pipe in self.pipes:
            pipe._write_bytes(__data, dest, metadata)
    
    def append_pipe(self, storage) -> int:
        return len(self.pipes.append(storage))
    
    def _write_bytes(self, __dest, __data, metadata):
        ...


class LocalStorage(MockStorage):
    def __init__(self, root: str) -> None:
        self.root = root
        self.pipes = []

    def _write_bytes(self, __dest, __data, metadata):
        with open(__dest, 'wb') as f:
            f.write(__data)
        








def convert_to_utc(dt: "datetime.datetime"):
    if dt.tzinfo is None:
        raise Exception("dt.tzinfo is None")
    
    # datetimeオブジェクトをUTCに変換
    dt_utc = dt.astimezone(datetime.timezone.utc)
    
    return dt_utc


def extract_emails(body: str):
    return re.findall(r'[\w\.-]+@[\w\.-]+', body)


def add_fulltext(metadata: dict, ignores: set):
    fulltext =  " ".join(v for k, v in metadata.items() if k not in ignores)
    metadata["fulltext"] = fulltext


SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']






def get_bucket_client(
    type: Literal["local", "minio"] = "local",
    bucket_name: str = "",
    **kwargs
):
    if not bucket_name:
        raise Exception()
    
    if type == "local":
        raise Exception()
    elif type == "minio":
        client = MinioBucket.ready(bucket_name, **kwargs)
            
    else:
        raise Exception()
    
    return client


class FilterBucket:
    def __init__(self, bucket, filter = lambda *args, **kwargs: True) -> None:
        self.bucket = bucket
        self.filter = filter
        
    def write_bytes(self, __data: bytes, __path, **metadata):
        if self.filter(__data, __path, **metadata):
            return self.bucket.write_bytes(__data, __path, **metadata)

class MinioBucket:
    def __init__(self, client: Minio, bucket_name) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.pipes = []
    
    @classmethod
    def ready(cls, bucket_name, **kwargs):
        _client = Minio(**kwargs)
        client = cls(_client, bucket_name)
        
        if not client.bucket_exists():
            client.make_bucket()
        
        return client

    @staticmethod
    def is_unsafe_path(path):
        if ".." in path:
            return "Invalid path"
        
        if "/" == path[:1]:
            return "Invalid path"
        
        return ""

    def write_bytes(self, __data: bytes, __path, **metadata):
        is_unsafe = self.is_unsafe_path(__path)
        if is_unsafe:
            raise Exception(is_unsafe)
        
        print(__path, metadata)
        self._write_bytes(__data, __path, metadata)
        
        for pipe in self.pipes:
            pipe._write_bytes(__data, __path, metadata)

    def make_bucket(self):
        return self.client.make_bucket(self.bucket_name)
        
    def bucket_exists(self):
        return self.client.bucket_exists(self.bucket_name)

    def list_bucket(self):
        return self.client.list_buckets()

    def _write_bytes(self, __data: bytes, __dest, metadata: dict = {}):
        # マルチバイト文字は登録できないのでbase64でエンコードする
        # X-Amz-Meta- でカスタムメタ名を記述する
        meta = {
            "X-Amz-Meta-User-Meta": base64.b64encode(json.dumps(metadata).encode()).decode()
        }
        
        result = self.client.put_object(
            self.bucket_name, __dest, io.BytesIO(__data), len(__data), metadata=meta
        )

        # minio_client.fput_object  # ファイルパスを渡す場合
    

class Pipeline:
    functions: Dict[str, object] = {}

    def __init_subclass__(cls) -> None:
        cls.functions: Dict[str, object] = {}
        
    @classmethod
    def register(cls, func):
        if func.__name__ in cls.functions:
            raise Exception()
        
        cls.functions[func.__name__] = func
        return func
    
    def __init__(self, bucket: MinioBucket):
        self.bucket = bucket

    @classmethod
    def create_pipeline(cls, bucket_client, pipelines=[]):
        bucket = get_bucket_client(**bucket_client)
        bucket = cls.build_pipeline(bucket, pipelines)
        return cls(bucket)
    
    @classmethod
    def build_pipeline(cls, bucket, pipelines):
        for name in reversed(pipelines):
            func = cls.functions[name]
            bucket = FilterBucket(bucket, func)
            
        return bucket


@Pipeline.register
def ignore_ics(__data: bytes, __dest, **metadata: dict):
    filename = os.path.basename(__dest)
    return not filename.endswith(".ics")


@Pipeline.register
def ignore_domain(__data: bytes, __dest, **metadata: dict):
    domain = metadata["domain"]
    return domain not in {"progrise.jp", "fintax.jp"}



class Requirements(BaseModel):
    YOUR_DOWNLOADED_JSON_FILE: str = "YOUR_DOWNLOADED_JSON_FILE.json"
    token_json_path: str = "token.json"
    DOWNLOAD_DIR: str = "download"


def authenticate(requirement: Requirements):
    creds = None
    if os.path.exists(requirement.token_json_path):
        creds = Credentials.from_authorized_user_file(requirement.token_json_path)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(requirement.YOUR_DOWNLOADED_JSON_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(requirement.token_json_path, 'w') as token:
                token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)


class Service:
    def __init__(self, requirements: Requirements) -> None:
        self.requirements = requirements
        self.service = authenticate(requirements)
        
    def get_attachments(self, start_date, end_date):
        service = self.service
        
        def get_message_ids(service, start_date, end_date):
            """認証済みユーザーの指定された範囲のメール情報（IDやスレッドID）を取得する。
            メッセージの本文や添付ファイルなどの詳細は含まれない。
            """
            start_date = int(start_date.timestamp())
            end_date = int(end_date.timestamp())
            
            query = f'in:inbox after:{start_date} before:{end_date} has:attachment'
            page_token = None
            
            while True:
                results = service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
                messages = results.get('messages', [])
                for message in messages:
                    yield message
                    
                page_token = results.get('nextPageToken', None)
                if not page_token:
                    break
            
        def get_detail(service, message_id):
            msg = service.users().messages().get(userId='me', id=message_id).execute()
            return msg
        
        def get_attachment(service, message_id, attachment_Id):
            attachment = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=attachment_Id).execute()
            return attachment
        
        storage = LocalStorage(self.requirements.DOWNLOAD_DIR)

        for message in get_message_ids(service, start_date, end_date):
            msg = get_detail(service, message['id'])
            
            headers = {x["name"]:x["value"] for x in msg['payload']["headers"]}
            dt = parsedate_to_datetime(headers["Date"]) # RFC 2822 
            dt = convert_to_utc(dt)

            try:
                base_info = {
                    "type": "mail-attachment",
                    "sender": extract_emails(headers["From"])[0],
                    "receiver": extract_emails(headers["Delivered-To"])[0],
                    "date": dt.isoformat(),
                    "subject": headers["Subject"],
                    # "body": ""
                    # "hash": ""
                }
            
            except Exception as e:
                print(headers)
                raise
            
            if not "parts" in msg['payload']:
                continue
            
            # https://developers.google.com/gmail/api/reference/rest/v1/users.messages?hl=ja#Message
            for part in msg['payload']['parts']:
                if 'filename' in part and part['filename']:
                    info = base_info.copy()

                    filename = part['filename']
                    info["filename"] = filename
                    
                    info["domain"] = info["sender"].split("@")[1]
                    add_fulltext(info, ignores={"type", "date"})
                    
                    if 'attachmentId' in part['body']:
                        att_id = part['body']['attachmentId']
                        info["attachmentId"] = att_id
                        attachment = get_attachment(service, message['id'], att_id)
                        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                        
                    elif 'data' in part['body']:
                        raise Exception()

                    else:
                        raise Exception()
                    
                    filename = info["domain"] + "/" + info["date"] + "_" + info["filename"]
                    yield filename, file_data, info
                    


def get_sample_message():
    return [
        {'name': 'From', 'value': 'sender <sender@xxx.work>'},
        {'name': 'Subject', 'value': '[ Remogu ] 請求書 に Remoguカスタマーサクセス がコメントしました'},
        {'name': 'Date', 'value': 'Tue, 03 Oct 2023 05:10:49 +0000'},
        {'name': 'Delivered-To', 'value': 'xxx@xxx.com'},
        {'name': 'Received', 'value': 'by 2002:a05:6a10:a178:b0:4e4:2a6b:18e8 with SMTP id r24csp1908322pxe;        Mon, 2 Oct 2023 22:10:49 -0700 (PDT)'},
        {'name': 'X-Google-Smtp-Source', 'value': 'AGHT+IGcaPparQSWr4XdN3DAS4akPA5v8dB7B1E9AEyolJeQcwLI4VUD8ogJFMX9Vg8pMNf7tTF3'},
        {'name': 'X-Received', 'value': 'by 2002:a05:6902:1028:b0:d11:2a52:3f35 with SMTP id x8-20020a056902102800b00d112a523f35mr14131144ybt.20.1696309849593;        Mon, 02 Oct 2023 22:10:49 -0700 (PDT)'},
        {'name': 'ARC-Seal', 'value': 'i=1; a=rsa-sha256; t=1696309849; cv=none;        d=google.com; s=arc-20160816;        b=arEgEej4eglZzXeZtYCXwXmx9E3Pkl4065PWxMGb2cD743yPzz9sRvmnPBhvfgKY19         bpZO1KuMkct9VJCvNrPrbI/5swMH5mrSz5j73rUO6vrmgaADHGtIEDX5Tqzr9qSYl0EY         8eX7ICQrY86bROwgL9tFwOkOsR5WHGRmwCKZ+ql4eSxmn6BhrfwQ3YMa7Uhr+8sYvBbO         FBABxzu5j+O/EB2xj2SuTq4C71FESsWTyd9s8hXOv+M842Kva17K+sBpYUPGO9luziZY         YbbLe5C7x/AdwlHk6aSHC0YeaEOMo5b22Gf+Icg9NxtRHcpekmGyH9OZ7rdySaOVfUGj         L1Ig=='},
        {'name': 'ARC-Message-Signature', 'value': 'i=1; a=rsa-sha256; c=relaxed/relaxed; d=google.com; s=arc-20160816;        h=mime-version:date:feedback-id:to:message-id:subject:from         :dkim-signature:dkim-signature;        bh=f2MXBvaYq2f9oFopLx47G1mCGjDZaAJolvIy4m1Szsg=;        fh=3+W4doVxEW4ZxnF7mXJLwYw81gCD1Q1+CzkdA1JWJLI=;        b=VoeX70UL8ffp0QU+ELKYfWFXIlruOuvqSSNKZK9BM1c5udP8gW+3JlNOh9hD9gs4NP         z0CAWiU1lq1JZ0pTSidOZ6u0nOfnzFIP6s2H1uW6/DaA+mP8IiZUIxFOACFzFefhGA3l         d8jwV/62hjvVFZ+OkNsR5T087i1kTqDV/qVR5mnpPwq1zuVJSkcwjPsoUqHb1vwuHF4z         7CDWlYzJJUlUaM8BOHRQ8fcEE1qhHS0SCfNbH0J8E30x+AWrDWDmwoHB8PoHRWf1bfka         8JS8BaQGmrZ9NdvNFWIEFuOgKVTh7DY1qsmR0QfsgEeBHg7r/LN2EGPh3WbicHxNqzM0         TwKQ=='},
        {'name': 'ARC-Authentication-Results', 'value': 'i=1; mx.google.com;       dkim=pass header.i=@sender.work header.s=mandrill header.b=jAnQP1TG;       dkim=pass header.i=@mandrillapp.com header.s=mandrill header.b=UhDGDOpf;       spf=pass (google.com: domain of bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com designates 205.201.139.5 as permitted sender) smtp.mailfrom=bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com'},
        {'name': 'Return-Path', 'value': '<bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com>'},
        {'name': 'Received', 'value': 'from mail5.wdc04.mandrillapp.com (mail5.wdc04.mandrillapp.com. [205.201.139.5])        by mx.google.com with ESMTPS id 64-20020a250343000000b00d86822c0a4asi260151ybd.81.2023.10.02.22.10.49        for <xxx@xxx.com>        (version=TLS1_3 cipher=TLS_AES_256_GCM_SHA384 bits=256/256);        Mon, 02 Oct 2023 22:10:49 -0700 (PDT)'},
        {'name': 'Received-SPF', 'value': 'pass (google.com: domain of bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com designates 205.201.139.5 as permitted sender) client-ip=205.201.139.5;'},
        {'name': 'Authentication-Results', 'value': 'mx.google.com;       dkim=pass header.i=@sender.work header.s=mandrill header.b=jAnQP1TG;       dkim=pass header.i=@mandrillapp.com header.s=mandrill header.b=UhDGDOpf;       spf=pass (google.com: domain of bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com designates 205.201.139.5 as permitted sender) smtp.mailfrom=bounce-md_31165128.651ba259.v1-5b4056df89a0489896e58a7a87a57db6@mandrillapp.com'},
        {'name': 'DKIM-Signature', 'value': 'v=1; a=rsa-sha256; c=relaxed/relaxed; d=sender.work; s=mandrill; t=1696309849; x=1696570349; i=sender@xxx.work; bh=f2MXBvaYq2f9oFopLx47G1mCGjDZaAJolvIy4m1Szsg=; h=From:Subject:Message-Id:To:Feedback-ID:Date:MIME-Version:\t Content-Type:CC:Date:Subject:From; b=jAnQP1TGEocGSlA9JJzy9z/0c8vxQEX+mlr4qcFZ+r205JOR2yMk9kokg/cbeLXvC\t zG9IPaxi0dytow/2M11kINHttetb2x17Z/WmvuzJU/O/eK/wvFxbyuv5rPf58ZpWIs\t xwWqotYDjeJfjDeLAtvHA5wGPS06dOgHgrUomY1Q='},
        {'name': 'Received', 'value': 'from pmta16.mandrill.prod.suw01.rsglab.com (localhost [127.0.0.1]) by mail5.wdc04.mandrillapp.com (Mailchimp) with ESMTP id 4S05WF317tzG0DCxp for <xxx@xxx.com>; Tue,  3 Oct 2023 05:10:49 +0000 (GMT)'},
        {'name': 'DKIM-Signature', 'value': 'v=1; a=rsa-sha256; c=relaxed/relaxed; d=mandrillapp.com;  i=@mandrillapp.com; q=dns/txt; s=mandrill; t=1696309849; h=From :  Subject : Message-Id : To : Date : MIME-Version : Content-Type : From :  Subject : Date : X-Mandrill-User : List-Unsubscribe;  bh=f2MXBvaYq2f9oFopLx47G1mCGjDZaAJolvIy4m1Szsg=;  b=UhDGDOpf+NPsejvaNvozIVUZM0GDs4Nkerzx9hlHTNyqagtTIh/r52wg3XhPwaFq2Oy0fW PGooWfyMYxNTI3THv56P8lESaT/f2BuLvCkND5HzlX5FQjSL2eE13PHaP8ja60ipWafpnSnR yxhr/5/4ao/TPdJiTKOAlzojYlRZQ='},
        {'name': 'Received', 'value': 'from [18.181.134.114] by mandrillapp.com id 5b4056df89a0489896e58a7a87a57db6; Tue, 03 Oct 2023 05:10:49 +0000'},
        {'name': 'Message-Id', 'value': '<651ba259521_7189c4538477@ip-172-16-0-54.ap-northeast-1.compute.internal.mail>'},
        {'name': 'To', 'value': 'xxx@xxx.com'}, {'name': 'X-Native-Encoded', 'value': '1'},
        {'name': 'X-Report-Abuse', 'value': 'Please forward a copy of this message, including all headers, to abuse@mandrill.com. You can also report abuse here: https://mandrillapp.com/contact/abuse?id=31165128.5b4056df89a0489896e58a7a87a57db6'},
        {'name': 'X-Mandrill-User', 'value': 'md_31165128'},
        {'name': 'Feedback-ID', 'value': '31165128:31165128.20231003:md'},
        {'name': 'MIME-Version', 'value': '1.0'},
        {'name': 'Content-Type', 'value': 'multipart/alternative; boundary="_av-DFtau-wklHuLxQyIVZgMjg"'}
    ]


def get_sample_data():
    return {
        'partId': '1',
        'mimeType': 'image/png',
        'filename': 'image001.png',
        'headers': [
            {'name': 'Content-Type', 'value': 'image/png; name="image001.png"'},
            {'name': 'Content-Description', 'value': 'image001.png'},
            {'name': 'Content-Disposition', 'value': 'inline; filename="image001.png"; size=258363; creation-date="Wed, 28 Jun 2023 07:46:17 GMT"; modification-date="Wed, 28 Jun 2023 07:46:18 GMT"'},
            {'name': 'Content-ID', 'value': '<image001.png@01D9A9DF.198A3770>'},
            {'name': 'Content-Transfer-Encoding', 'value': 'base64'}
        ], 'body': {
            'attachmentId': 'ANGjdJ9nYuQG_cCYcx4CpWCGJbM80bJ8wMOvMS-zpuP2x7J7C055ga5vDG23DGCxLDuwC665KbvuWgMCFiZr1S3VkoOJtruv3X8iwwTCgkDxLV12nHGkINiiaZ9scUsN53uXacfgPnnAy8seIBPoJ4fJM2DufjTWJMJJqOjW6SKI5_t0lydTN-fr9ihphltj3qhjs1NZ2KqF2iO659WMrj-5rzos9JnXQgudt6Kly2HiIACS0oO3grp6ylFHk88aNDAUV5Lg6I-1UuRBoOcZF2T93OZu-gXd_HsmcM6EiXuMZXib-9ZxS7eAJyZbSbSW7ZSij_voK1IrCxsLhPCqxQlITpjz6pBNxGANev7UcTBVF3y2hV7669CBFC4Ry-CvIwckhhxrC_zsrRfRiv_G',
            'size': 258363,
            # "data": ""  # attachmentId か data かで分岐するようだ
        }
    }
    
    

if __name__ == '__main__':
    pipeline = Pipeline.create_pipeline(
        bucket_client=dict(
            type="minio",
            endpoint="localhost:9000",
            access_key="minioadmin123",
            secret_key="minioadmin123",
            secure=False,
            bucket_name="mybucket"
        ),
        # extractors=[]
        pipelines=[
            ignore_ics.__name__,
            ignore_domain.__name__
        ],
        # credentials=[]
    )
    
    bucket = pipeline.bucket
    
    from datetime import datetime as dt, timedelta, timezone
    JST = timezone(timedelta(hours=9))
    local_start_date = dt(2023, 1, 1, 0, 0, tzinfo=JST)
    local_end_date = dt(2023, 12, 31, 0, 0, tzinfo=JST)

    utc_start_date = local_start_date.astimezone(timezone.utc)
    utc_end_date = local_end_date.astimezone(timezone.utc)
    
    requirements = Requirements()
    service = Service(requirements)

    for filename, file_data, info in service.get_attachments(utc_start_date, utc_end_date):
        bucket.write_bytes(file_data, filename, **info)
