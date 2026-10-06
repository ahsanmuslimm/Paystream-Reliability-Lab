# PayStream Reliability Lab - one-command operations
# Targets follow Document 04. Run from the repository root.

SHELL := /bin/bash
COMPOSE_FILES := -f infra/compose/docker-compose.yml -f infra/compose/docker-compose.monitoring.yml
ENV_FILE := --env-file .env

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

.PHONY: up
up: env build-images ## Start the full stack: 3 brokers, Schema Registry, Postgres, services, monitoring
	docker compose $(ENV_FILE) $(COMPOSE_FILES) up -d
	$(MAKE) --no-print-directory topics

.PHONY: down
down: ## Stop the stack (volumes are kept; use 'make nuke' to remove them)
	docker compose $(ENV_FILE) $(COMPOSE_FILES) down

.PHONY: nuke
nuke: ## Stop the stack and delete all volumes (cold start rehearsal)
	docker compose $(ENV_FILE) $(COMPOSE_FILES) down -v --remove-orphans

.PHONY: ps
ps: ## Show container status
	docker compose $(ENV_FILE) $(COMPOSE_FILES) ps

.PHONY: logs
logs: ## Tail logs from all services
	docker compose $(ENV_FILE) $(COMPOSE_FILES) logs -f --tail=50

.PHONY: build
build: ## Compile and unit-test the Java services (no Docker needed)
	mvn -f services/pom.xml -B verify

.PHONY: build-images
build-images: ## Build the service container images
	docker compose $(ENV_FILE) -f infra/compose/docker-compose.yml build

.PHONY: topics
topics: ## Apply kafka-config/topics.yaml to the running cluster
	bash kafka-config/scripts/apply-topics.sh

.PHONY: acls
acls: ## Apply kafka-config/acls.yaml (requires security overlay, Stage 2)
	@echo "ACL application requires the security overlay (WP2.2, Stage 2)."

.PHONY: certs
certs: ## Generate lab PKI certificates (Stage 2, WP2.1)
	@echo "Certificate generation arrives with WP2.1 (Stage 2)."

.PHONY: validate-config
validate-config: ## Lint topics/ACLs/schemas against the Document 03 standards
	python kafka-config/scripts/validate-config.py

.PHONY: test
test: build ## Alias for build: run the full Maven test suite

.PHONY: smoke
smoke: ## T2 smoke test: one message flows producer -> alert -> notification row
	bash scripts/smoke-test.sh

.PHONY: wait-for-cluster
wait-for-cluster: ## Block until the Kafka quorum and Schema Registry are healthy
	bash scripts/wait-for-cluster.sh

.PHONY: drill-d1 drill-d2 drill-d3 drill-d4 drill-d5 drill-d6
drill-d1: ## Execute failure drill D1: single broker loss (Stage 2)
	@if [ -f chaos/d1_broker_loss.sh ]; then bash chaos/d1_broker_loss.sh; else echo "Drill D1 arrives with WP2.7 (Stage 2)."; fi
drill-d2: ## Execute failure drill D2: ISR shrink / quorum loss (Stage 2)
	@if [ -f chaos/d2_isr_shrink.sh ]; then bash chaos/d2_isr_shrink.sh; else echo "Drill D2 arrives with WP2.7 (Stage 2)."; fi
drill-d3: ## Execute failure drill D3: consumer lag / rebalance storm (Stage 2)
	@if [ -f chaos/d3_consumer_lag.sh ]; then bash chaos/d3_consumer_lag.sh; else echo "Drill D3 arrives with WP2.7 (Stage 2)."; fi
drill-d4: ## Execute failure drill D4: disk exhaustion (Stage 2)
	@if [ -f chaos/d4_disk_full.sh ]; then bash chaos/d4_disk_full.sh; else echo "Drill D4 arrives with WP2.7 (Stage 2)."; fi
drill-d5: ## Execute failure drill D5: certificate expiry (Stage 2)
	@if [ -f chaos/d5_cert_expiry.sh ]; then bash chaos/d5_cert_expiry.sh; else echo "Drill D5 arrives with WP2.7 (Stage 2)."; fi
drill-d6: ## Execute failure drill D6: poison message / DLQ (Stage 2)
	@if [ -f chaos/d6_poison_message.sh ]; then bash chaos/d6_poison_message.sh; else echo "Drill D6 arrives with WP2.7 (Stage 2)."; fi
