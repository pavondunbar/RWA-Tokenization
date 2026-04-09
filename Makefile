.PHONY: up down build restart ps demo logs db-balances db-ledger \
       db-audit db-states db-outbox db-dlq db-tables \
       db-immutable-test shell-pg shell-kafka topics kafka-tail \
       health test help

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

up: ## Build and start all services
	docker compose up --build -d

down: ## Stop containers and remove volumes (full reset)
	docker compose down -v

build: ## Build images without starting containers
	docker compose build

restart: ## Restart all services without rebuilding
	docker compose restart

ps: ## Show container status and health
	docker compose ps

health: ## Show health status of all containers
	@docker compose ps --format \
	  "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# ---------------------------------------------------------------------------
# Demo & Logs
# ---------------------------------------------------------------------------

demo: ## Show demo output from the rwa-service container
	docker logs rwa-service

logs: ## Tail logs from all containers
	docker compose logs -f

# ---------------------------------------------------------------------------
# Database Inspection
# ---------------------------------------------------------------------------

PSQL = docker compose exec -T postgres psql -U ledger_user -d rwa

db-balances: ## Show investor balances derived from the ledger
	@$(PSQL) -c \
	  "SELECT investor_id, asset_id, balance \
	   FROM v_investor_balance \
	   ORDER BY balance DESC;"

db-ledger: ## Show double-entry ledger journal
	@$(PSQL) -c \
	  "SELECT id::text, asset_id::text, entry_type, \
	          debit_account, credit_account, amount, created_at \
	   FROM ledger_entries ORDER BY created_at;"

db-audit: ## Show state transition audit trail
	@$(PSQL) -c \
	  "SELECT entity_type, entity_id::text, from_state, to_state, \
	          actor, request_id::text, trace_id::text, created_at \
	   FROM state_transitions ORDER BY created_at;"

db-states: ## Show current state of each entity
	@$(PSQL) -c \
	  "SELECT entity_type, entity_id::text, current_state, created_at \
	   FROM v_current_state ORDER BY entity_type, created_at;"

db-outbox: ## Show outbox events and delivery status
	@$(PSQL) -c \
	  "SELECT oe.id::text, oe.event_type, \
	          CASE WHEN op.id IS NOT NULL THEN 'PUBLISHED' \
	               WHEN dlq.id IS NOT NULL THEN 'DLQ' \
	               ELSE 'PENDING' END AS status, \
	          oe.created_at \
	   FROM outbox_events oe \
	   LEFT JOIN outbox_published op ON op.event_id = oe.id \
	   LEFT JOIN outbox_dlq dlq ON dlq.event_id = oe.id \
	   ORDER BY oe.created_at;"

db-dlq: ## Show dead-lettered events
	@$(PSQL) -c \
	  "SELECT dlq.event_id::text, oe.event_type, dlq.attempts, \
	          dlq.error_message, dlq.created_at \
	   FROM outbox_dlq dlq \
	   JOIN outbox_events oe ON oe.id = dlq.event_id \
	   ORDER BY dlq.created_at;"

db-tables: ## Show row counts for all tables
	@$(PSQL) -c \
	  "SELECT 'rwa_assets' AS t, COUNT(*) FROM rwa_assets \
	   UNION ALL SELECT 'legal_wrappers', COUNT(*) FROM legal_wrappers \
	   UNION ALL SELECT 'investor_compliance', COUNT(*) FROM investor_compliance \
	   UNION ALL SELECT 'whitelisted_wallets', COUNT(*) FROM whitelisted_wallets \
	   UNION ALL SELECT 'token_mints', COUNT(*) FROM token_mints \
	   UNION ALL SELECT 'nav_calculations', COUNT(*) FROM nav_calculations \
	   UNION ALL SELECT 'token_redemptions', COUNT(*) FROM token_redemptions \
	   UNION ALL SELECT 'state_transitions', COUNT(*) FROM state_transitions \
	   UNION ALL SELECT 'ledger_entries', COUNT(*) FROM ledger_entries \
	   UNION ALL SELECT 'outbox_events', COUNT(*) FROM outbox_events \
	   UNION ALL SELECT 'outbox_published', COUNT(*) FROM outbox_published \
	   UNION ALL SELECT 'outbox_dlq', COUNT(*) FROM outbox_dlq;"

db-immutable-test: ## Verify append-only triggers reject mutations
	@echo "Testing UPDATE on rwa_assets (should fail)..."
	@$(PSQL) -c "UPDATE rwa_assets SET name = 'HACK';" 2>&1 || true
	@echo ""
	@echo "Testing DELETE on ledger_entries (should fail)..."
	@$(PSQL) -c "DELETE FROM ledger_entries;" 2>&1 || true
	@echo ""
	@echo "Testing UPDATE on state_transitions (should fail)..."
	@$(PSQL) -c "UPDATE state_transitions SET to_state = 'HACK';" 2>&1 || true

shell-pg: ## Open an interactive psql shell
	docker compose exec postgres psql -U ledger_user -d rwa

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

topics: ## List Kafka topics
	docker exec rwa-kafka kafka-topics \
	  --bootstrap-server localhost:9092 --list

kafka-tail: ## Consume messages from all rwa.* topics
	docker exec rwa-kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 \
	  --whitelist 'rwa\..*' \
	  --from-beginning --timeout-ms 10000

shell-kafka: ## Open an interactive shell in the Kafka container
	docker exec -it rwa-kafka bash

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test: ## Run integration tests (requires: make up)
	python -m pytest tests/test_core.py -xvs
