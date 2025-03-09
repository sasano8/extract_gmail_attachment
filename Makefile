extract:
	@python -m modules --protocol=file --output_dir=.cache --clean 1 --pipelines=extract_attachments,filter_attachments,rm_empty_dir
