DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/transaction_control_plane?sslmode=disable
MIGRATIONS_PATH := internal/ledger/storage/migrations

.PHONY: migrate migrate-up migrate-down check-migrate-cli check-buf-cli proto-lint proto-breaking proto-generate

check-buf-cli:
	@command -v buf >/dev/null 2>&1 || { \
		echo "buf CLI not found. Install it via 'brew install bufbuild/buf/buf'" \
		     "or see https://buf.build/docs/installation"; \
		exit 1; \
	}

# proto-lint runs buf's lint rules against proto/. proto-generate calls out
# to buf's remote plugin execution (buf.build/protocolbuffers/go and
# buf.build/grpc/go), so it needs network access but not a local
# protoc-gen-go/protoc-gen-go-grpc install.
proto-lint: check-buf-cli
	buf lint proto

# proto-breaking checks proto/ against its state on main, failing on any
# wire- or source-incompatible change per buf.yaml's breaking rules.
proto-breaking: check-buf-cli
	buf breaking proto --against '.git#branch=main,subdir=proto'

proto-generate: check-buf-cli
	buf generate proto

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
