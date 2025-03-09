import os
import json
import base64
from typing import TypedDict, Iterable
import email.utils
import logging

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from fsspec import filesystem, AbstractFileSystem

from ._path import assert_linux_safe_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

SECRET_FILE = "google_secret.secret.json"
TOKEN_FILE = "token.json"


class CredentialResoruce:
    @classmethod
    def get_secret_path(cls):
        return SECRET_FILE

    @classmethod
    def exists_secret_path(cls):
        return os.path.exists(cls.get_secret_path())

    @classmethod
    def get_secret(cls) -> dict:
        with open(cls.get_secret_path()) as f:
            return json.load(f)

    @classmethod
    def get_token_path(cls):
        return TOKEN_FILE

    @classmethod
    def exists_token_path(cls):
        return os.path.exists(cls.get_token_path())

    @classmethod
    def get_token_or_none(cls):
        if not cls.exists_token_path():
            return None
        else:
            with open(cls.get_token_path()) as f:
                return json.load(f)

    @classmethod
    def save_token(cls, token: dict):
        with open(cls.get_token_path(), "w") as f:
            json.dump(token, f)


class OauthFlow:
    def __init__(self, resource: CredentialResoruce):
        self._res = resource

    def exec(self):
        res: CredentialResoruce = self._res

        token = res.get_token_or_none()
        if token is None:
            client_config = res.get_secret()
            creds = self.exec_oauth_flow_from_dict(client_config, SCOPES)
            res.save_token(json.loads(creds.to_json()))
            token = res.get_token_or_none()

        creds = Credentials.from_authorized_user_info(token)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())

        return creds

    @classmethod
    def exec_oauth_flow_from_dict(cls, input_secret_path, scopes) -> dict:
        flow = InstalledAppFlow.from_client_config(input_secret_path, scopes)
        creds = flow.run_local_server(port=0, open_browser=False)
        return creds


def authenticate_and_build_service(serviceName, version, **kwargs):
    loc = CredentialResoruce()
    flow = OauthFlow(loc)
    creds = flow.exec()

    return build(serviceName, version, credentials=creds, **kwargs)


class GMailInfo(TypedDict):
    id: str
    threadId: str


class GmailClient:
    @classmethod
    def authenticate_and_build_service(cls, **kwargs):
        loc = CredentialResoruce()
        flow = OauthFlow(loc)
        creds = flow.exec()

        service = build("gmail", "v1", credentials=creds, **kwargs)
        return cls(service)

    def __init__(self, service):
        self._service = service

    @classmethod
    def select(cls, message):
        _headers = message.get("payload", {}).get("headers", [])
        headers = {kv["name"].lower(): kv["value"] for kv in _headers}

        subject = headers.get("subject", "（件名なし）")
        date = headers.get("date", "（日付なし）")
        sender = headers.get("from", "（送信者なし）")

        parsed_date = email.utils.parsedate_to_datetime(date).isoformat()
        name, email_address = email.utils.parseaddr(sender)

        return {
            "title": subject,
            "date": parsed_date,
            "sender_name": name,
            "sender_address": email_address,
        }

    def extract_attachments(self, message_id):
        """メールの添付ファイルを取得"""
        client = self._service

        message = client.users().messages().get(userId="me", id=message_id).execute()
        info = self.select(message)
        payload = message.get("payload", {})
        parts = payload.get("parts", [])

        attachments = []

        for part in parts:
            filename = part.get("filename")
            mime_type = part.get("mimeType")
            body = part.get("body", {})
            attachment_id = body.get("attachmentId")

            if filename and attachment_id:
                # 添付ファイルを取得
                attachment = (
                    client.users()
                    .messages()
                    .attachments()
                    .get(userId="me", messageId=message_id, id=attachment_id)
                    .execute()
                )

                file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))
                attachments.append((filename, mime_type, file_data))

                assert_linux_safe_path(filename)

        for i, attachment in enumerate(attachments):
            filename, mime_type, file_data = attachment
            yield (
                info["date"],
                message_id,
                info["sender_name"],
                info["sender_address"],
                info["title"],
                filename,
                mime_type,
                file_data,
            )

    def query(self, query=None):
        client = self._service

        messages = []
        next_page_token = None

        while True:
            response = (
                client.users()
                .messages()
                .list(userId="me", q=query, pageToken=next_page_token)
                .execute()
            )

            messages = response.get("messages", [])
            yield from messages

            _prevToken = next_page_token
            next_page_token = response.get("nextPageToken", None)
            logger.debug(
                f"[UPDATE Page Token] prev: {_prevToken} next: {next_page_token}"
            )

            if not next_page_token:
                break


def extract_attachments(
    protocol: str,
    output_dir,
    clean: bool = False,
    query: str = None,
    excludes={
        ".ics",
        ".html",
        ".htm",
        ".css",
        ".js",
        ".gif",
        ".png",
    },  # htmlに含まれるデータなども添付ファイルとして認識されてしまうので exclude
):
    """単に添付ファイルを取得する"""
    fs: AbstractFileSystem = filesystem(protocol)
    if clean:
        if fs.exists(output_dir):
            fs.rm(output_dir, recursive=True)

    fs.mkdir(output_dir, create_parents=True)

    client = GmailClient.authenticate_and_build_service()

    def flatten_dict(d, parent_key="", sep="."):
        """ネストされた辞書をフラット化する"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def is_exclude(filename):
        for exclude in excludes:
            if exclude in filename:
                return True
        return False

    for mail in client.query(query):
        i = 0
        for (
            date,
            message_id,
            sender_name,
            sender_address,
            title,
            filename,
            mime_type,
            file_data,
        ) in client.extract_attachments(mail["id"]):
            i += 1
            domain = os.path.join(output_dir, sender_address)
            fs.mkdirs(domain, exist_ok=True)

            path = os.path.join(output_dir, sender_address, "_".join([date, filename]))

            if is_exclude(filename):
                logger.info("[SKIP   ]" + path)
                continue
            else:
                logger.info("[EXTRACT]" + path)

            with open(path, "wb") as f:
                # print(i, date, message_id, sender_name, sender_address, filename, mime_type)
                f.write(file_data)


# def filter_attachments(
#     protocol: str, output_dir, clean: bool = False, query: str = None
# ):
#     fs: AbstractFileSystem = filesystem(protocol)
#     exclude_exts = {"ics", ".html", ".htm", ".css", ".js", ".gif"}
#     all_files = fs.glob(os.path.join(output_dir, "**"))
#     for file in all_files:
#         if not fs.isfile(file):
#             continue

#         for ext in exclude_exts:
#             if file.endswith(ext):
#                 fs.rm_file(file)
#                 continue


def rm_empty_dir(protocol: str, output_dir, clean: bool = False, query: str = None):
    fs: AbstractFileSystem = filesystem(protocol)

    all_files = fs.glob(os.path.join(output_dir, "**"))
    for file in sorted(all_files, key=len, reverse=True):
        if not fs.isdir(file):
            continue

        if not fs.ls(file):
            fs.rmdir(file)
