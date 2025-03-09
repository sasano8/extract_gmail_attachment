import argparse


def convert_str_to_bool(v):
    if not v:
        return False

    _v = int(v)
    return bool(_v)


def parse_arguments():
    """コマンドライン引数を解析"""
    parser = argparse.ArgumentParser(description="コマンドライン引数の解析サンプル")

    # オプションの定義
    parser.add_argument(
        "--protocol",
        "-p",
        type=str,
        default="file",
        help="fsspec がサポートするプロトコル",
    )
    parser.add_argument(
        "--output_dir", "-o", type=str, default=".cache", help="出力ファイルのパス"
    )
    parser.add_argument(
        "--clean",
        "-c",
        type=convert_str_to_bool,
        default=False,
        help="ディレクトリをクリーンアップする",
    )
    parser.add_argument(
        "--pipelines", help="実行するパイプラインの順序をカンマ区切りで指定する"
    )
    parser.add_argument("--query", type=str, help="gmail のフィルタを指定する")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    from . import _google

    # python -m modules --protocol=file --output_dir=.cache --clean 1 --pipelines=extract_attachments,filter_attachments,rm_empty_dir
    kwargs = vars(args)
    _pipelines = kwargs.pop("pipelines")
    pipelines = _pipelines.split(",")

    for funcname in pipelines:
        func = getattr(_google, funcname)
        func(**kwargs)
