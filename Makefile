format:
	@uvx ruff format .

extract-2024:
	@python -m modules --pipelines=pipe_extract_attachments,pipe_rm_empty_dir --clean 1 --protocol=file --output_dir=".cache/2024" --query="has:attachment after:2024/01/01 before:2025/01/01"
