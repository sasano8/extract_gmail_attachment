unsafes = {"..", "*", "\\", "<", ">", "'", '"', "?", "\0"}


def assert_linux_safe_path(v: str):
    for unsafe in unsafes:
        if unsafe in v:
            raise ValueError(f"Unsafe: {unsafe} Path: {v}")
