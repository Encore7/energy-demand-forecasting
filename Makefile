# Docker Commands 

run-api:
	docker compose up fastapi --build

run-all:
	docker compose up --build

down:
	docker compose down -v

logs-api:
	docker compose logs -f fastapi

# Python Code Quality 

format:
	black app model orchestration tests

lint:
	ruff app model orchestration tests

typecheck:
	mypy app model orchestration tests

test:
	pytest --cov=app --cov=model --cov-report=term-missing tests

check: lint format typecheck test

# Helpers 

install-dev:
	pip install -r requirements.txt && pip install black ruff mypy pytest pytest-cov

freeze:
	pip freeze > requirements.txt
