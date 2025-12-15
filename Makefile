PROJECT_NAME=energy-demand-forecasting
COMPOSE_FILE=infra/compose/docker-compose.yml

.PHONY: help up down logs ps restart lint test fmt

help:
	@echo "make up        - start all services"
	@echo "make down      - stop all services"
	@echo "make logs      - tail logs"
	@echo "make ps        - list services"
	@echo "make restart   - restart stack"
	@echo "make lint      - run linters"
	@echo "make test      - run tests"
	@echo "make fmt       - format code"

up:
	docker compose -f $(COMPOSE_FILE) up -d --build

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

ps:
	docker compose -f $(COMPOSE_FILE) ps

restart:
	make down && make up

lint:
	ruff check .
	mypy .

fmt:
	ruff format .

test:
	pytest -v
