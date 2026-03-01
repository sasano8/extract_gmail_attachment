format:
	@uvx ruff format .

# token.json を削除して再認証（ブラウザで認可URLが開きます）
reauth:
	@rm -f $(CURDIR)/token.json
	@python -c "from modules._google import TOKEN_FILE; print(f'Token path: {TOKEN_FILE}')"
	@python -c "from modules._google import authenticate_and_build_service; authenticate_and_build_service('gmail', 'v1'); print('Authentication successful.')"

extract-2023:
	@python -m modules --pipelines=pipe_extract_attachments,pipe_rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2023" --query="after:2023/01/01 before:2024/01/01 has:attachment smaller:1000000"

extract-2024:
	@python -m modules --pipelines=pipe_extract_attachments,pipe_rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2024" --query="after:2024/01/01 before:2025/01/01 has:attachment smaller:1000000"

extract-2025:
	@python -m modules --pipelines=pipe_extract_attachments,pipe_rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2025" --query="after:2025/01/01 before:2026/01/01 has:attachment smaller:1000000"

extract-2026:
	@python -m modules --pipelines=pipe_extract_attachments,pipe_rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2026" --query="after:2026/01/01 before:2027/01/01 has:attachment smaller:1000000"
