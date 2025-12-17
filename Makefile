SHELL := /bin/bash
COMPOSE_FILE := infra/compose/docker-compose.yml
ENV_FILE := .env

.PHONY: check-env up down restart ps logs pull clean

check-env:
	@test -f $(ENV_FILE) || (echo "ERROR: $(ENV_FILE) not found. Create it from .env.example" && exit 1)

up: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) up -d

down: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) down

restart: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) down
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) up -d

ps: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) ps

logs: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) logs -f --tail=200

pull: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) pull

clean: check-env
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) down -v