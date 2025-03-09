# 概要

## 認証認可

シークレットを用いて、 OAuth Flow を実行します。
認証されると、認可トークンが自動保存されます。

* Goggle Cloud Platformのプロジェクトを作成し、Gmail APIを有効にしてください。
* GMail API を有効化した後、シークレットをダウンロードしてください。
* シークレットは、ルートディレクトリ上に `google_secret.secret.json` として保存します。
* 認可トークンは `token.json` に自動保存されます（コマンド実行時に自動的に認可が行われます）。


## 添付ファイルの抽出

以下のコマンドで添付ファイルを抽出します。

```
make extract-2024
```

## 条件指定方法

クエリは Gmail のフィルタに基づきます。
after はその日付以降（その日付を含む）、before はその日付より過去（その日付を含まない）を抽出します。

```
python -m modules --pipelines=extract_attachments,filter_attachments,rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2024" --query="has:attachment after:2024/01/01 before:2025/01/01"
```
