DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/transaction_control_plane?sslmode=disable
MIGRATIONS_PATH := internal/ledger/storage/migrations

.PHONY: migrate migrate-up migrate-down check-migrate-cli

check-migrate-cli:
	@command -v migrate >/dev/null 2>&1 || { \
		echo "golang-migrate CLI not found. Install it via 'brew install golang-migrate'" \
		     "or see https://github.com/golang-migrate/migrate/tree/master/cmd/migrate"; \
		exit 1; \
	}

migrate: migrate-up

migrate-up: check-migrate-cli
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" up

migrate-down: check-migrate-cli
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" down 1
