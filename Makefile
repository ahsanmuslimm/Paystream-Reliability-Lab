# PayStream Reliability Lab - one-command operations
# Targets follow Document 04. Run from the repository root.
#
# Two stack modes:
#   make up         plaintext MVP stack (core + monitoring)
#   make up-secure  secured stack per WP2.1/WP2.2 (mTLS, SCRAM, ACLs) +
#                   monitoring, the reference mode for Stage 2 drills

SHELL := /bin/bash
COMPOSE_FILES := -f infra/compose/docker-compose.yml -f infra/compose/docker-compose.monitoring.yml
COMPOSE_SECURE_FILES := $(COMPOSE_FILES) -f infra/compose/docker-compose.security.yml
COMPOSE_CONNECT_FILES := -f infra/compose/docker-compose.yml -f infra/compose/docker-compose.connect.yml
ENV_FILE := --env-file .env
ENV_FILE_SECURE := --env-file .env --env-file security/secrets/interpolation.env
MVN := $(if $(wildcard /d/TOOLS/apache-maven/apache-maven-3.9.11/bin/mvn),/d/TOOLS/apache-maven/apache-maven-3.9.11/bin/mvn,mvn)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

.PHONY: up
up: env build-images ## Start the plaintext MVP stack: 3 brokers, Schema Registry, Postgres, services, monitoring
	docker compose $(ENV_FILE) $(COMPOSE_FILES) up -d
	$(MAKE) --no-print-directory topics

.PHONY: up-secure
up-secure: env certs scram ## Start the secured stack: mTLS listeners, SCRAM auth, ACLs (WP2.1/WP2.2)
	docker compose $(ENV_FILE_SECURE) $(COMPOSE_SECURE_FILES) up -d

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
	$(MVN) -f services/pom.xml -B verify

.PHONY: build-images
build-images: ## Build the service container images
	docker compose $(ENV_FILE) -f infra/compose/docker-compose.yml build

.PHONY: certs
certs: ## Generate lab PKI: CA, 3 broker certs, kafka-setup + demo client certs
	bash security/scripts/gen-ca.sh
	@for n in 1 2 3; do bash security/scripts/gen-broker-cert.sh --name broker-$$n; done
	bash security/scripts/gen-client-cert.sh --name kafka-setup
	bash security/scripts/gen-client-cert.sh --name svc-demo

.PHONY: scram
scram: ## Generate SCRAM-SHA-512 credentials (idempotent, git-ignored)
	bash security/scripts/create-scram-users.sh --generate-only

.PHONY: scram-apply
scram-apply: ## Register SCRAM users on the running secured cluster (mTLS admin path)
	bash security/scripts/create-scram-users.sh --apply

.PHONY: topics
topics: ## Apply kafka-config/topics.yaml to the running cluster
	bash kafka-config/scripts/apply-topics.sh

.PHONY: acls
acls: ## Apply kafka-config/acls.yaml to the secured cluster (svc-admin-ci)
	bash kafka-config/scripts/apply-acls.sh --bootstrap localhost:19091 --command-config security/secrets/admin.properties

.PHONY: up-connect
up-connect: ## Start Kafka Connect with the accounts CDC connector stack
	docker compose $(ENV_FILE) $(COMPOSE_CONNECT_FILES) up -d connect

.PHONY: apply-connector
apply-connector: ## Register the Debezium accounts connector (POST to Connect REST)
	bash kafka-config/scripts/apply-connector.sh

.PHONY: replay-dlq
replay-dlq: ## Build and run the DLQ replay tool (pass ARGS="--dlq bank.transactions.v1.dlq --dry-run")
	$(MVN) -q -f services/pom.xml -pl dlq-replay package -DskipTests
	java -jar services/dlq-replay/target/dlq-replay-0.1.0.jar --bootstrap localhost:19091 $(ARGS)

.PHONY: cert-expiry-check
cert-expiry-check: ## Probe listener certs and push expiry metrics to Pushgateway
	bash scripts/cert-expiry-check.sh

.PHONY: drift
drift: ## Report drift between the live cluster and topics.yaml / acls.yaml
	bash kafka-config/scripts/check-drift.sh

.PHONY: t8-security
t8-security: ## T8 negative tests against the secured stack (plaintext/SCRAM/certs/ACLs)
	bash tests/security/t8-security-tests.sh

.PHONY: contract
contract: ## T7 schema-evolution contract tests against Schema Registry
	bash tests/contract/schema-evolution.sh

.PHONY: validate-config
validate-config: ## Lint topics/ACLs/schemas/connect/security/monitoring against the standards
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
