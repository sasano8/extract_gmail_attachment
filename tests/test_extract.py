from modules._path import assert_linux_safe_path


class Error(Exception):
    def __bool__(self):
        return False


def is_safe(v: str):
    try:
        assert_linux_safe_path(v)
    except Exception as e:
        return Error(str(e))

    return True


def test_safe_path():
    assert is_safe("JPIN24-4085035.pdf")
