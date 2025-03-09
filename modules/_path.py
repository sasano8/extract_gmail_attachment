import email.utils

unsafes = {"..", "*", "\\", "<", ">", "'", '"', "?", "\0"}


def assert_linux_safe_path(v: str):
    for unsafe in unsafes:
        if unsafe in v:
            raise ValueError(f"Unsafe: {unsafe} Path: {v}")


def decode_email_date(v):
    iso = email.utils.parsedate_to_datetime(v).isoformat()
    return iso.replace(":", "-")


def encode_email_date(v):
    raise NotImplementedError()


def decode_email_sender(v):
    return email.utils.parseaddr(v)


def encode_email_sender(v):
    raise NotImplementedError()
