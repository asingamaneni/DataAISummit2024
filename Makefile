black-check:
	@poetry run black --check dataaisummit

fmt:
	@poetry run black dataaisummit/

check: black-check mypy
	@poetry run prospector --no-autodetect --profile prospector.yaml

mypy:
	@poetry run mypy -p dataaisummit --exclude venv --exclude dist --exclude .idea --exclude .vscode

dev:
	@poetry install --all-extras --with dev

poetry-lock-no-update:
	@poetry lock --no-update

poetry-lock:
	@poetry lock

poetry-install:
	@pip install --upgrade setuptools && pip install poetry && poetry self add "poetry-dynamic-versioning[plugin]"