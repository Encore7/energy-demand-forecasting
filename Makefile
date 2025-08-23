.PHONY: fmt lint type test

fmt:
	ruff format .

lint:
	ruff check .

type:
	mypy .

test:
	pytest -q