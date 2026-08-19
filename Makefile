DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/transaction_control_plane?sslmode=disable
MIGRATIONS_PATH := internal/ledger/storage/migrations
KIND_CLUSTER_NAME := transaction-control-plane

.PHONY: migrate migrate-up migrate-down check-migrate-cli check-buf-cli proto-lint proto-breaking proto-generate \
	kind-up kind-down kind-load kind-deploy kind-verify kind-verify-isolation \
	check-kind-cli check-docker-cli check-helm-cli

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

# --- Local Kind full-stack environment (issue #51) ---------------------
#
# make kind-up      creates the cluster and installs the TradingTenant CRDs
# make kind-load    builds ingestion/processor/operator images and loads
#                    them into the cluster (re-run after any code change)
# make kind-deploy  installs Strimzi/CloudNativePG/kube-prometheus-stack via
#                    Helm and applies the ingestion/processor/operator
#                    manifests, in dependency order
# make kind-verify  sends a sample transaction through the live stack and
#                    asserts (non-zero exit on failure, not just prints) that
#                    the TradingTenant reached a known status.state AND that
#                    tradingtenant_reconcile_duration_seconds{result="success"}
#                    actually increased off the back of it — see
#                    deploy/kind/verify.sh
# make kind-verify-isolation
#                    drives tenant-b into the operator's Isolated
#                    (noisy-neighbor, dedicated-node-pool) state with a load
#                    burst, and asserts the dedicated pool, isolation
#                    transition metric, and TenantIsolatedNoisyNeighborSuspected
#                    alert all actually fired — see deploy/kind/verify-isolation.sh.
#                    Leaves the isolated tenant running for a live Grafana/
#                    Prometheus demo; run after kind-verify, not instead of it.
# make kind-down    tears the cluster down
#
# Typical flow: make kind-up kind-load kind-deploy kind-verify kind-verify-isolation

check-kind-cli:
	@command -v kind >/dev/null 2>&1 || { \
		echo "kind CLI not found. Install it via 'brew install kind'" \
		     "or see https://kind.sigs.k8s.io/docs/user/quick-start/#installation"; \
		exit 1; \
	}

check-docker-cli:
	@command -v docker >/dev/null 2>&1 || { \
		echo "docker CLI not found. Install Docker Desktop or see https://docs.docker.com/get-docker/"; \
		exit 1; \
	}

check-helm-cli:
	@command -v helm >/dev/null 2>&1 || { \
		echo "helm CLI not found. Install it via 'brew install helm'" \
		     "or see https://helm.sh/docs/intro/install/"; \
		exit 1; \
	}

kind-up: check-kind-cli
	kind create cluster --config deploy/kind/kind-cluster.yaml
	kubectl apply -f deploy/kind/namespace.yaml
	kubectl apply -f operator/config/crd/bases/tradingtenant.controlplane.anwinsenp.dev_tradingtenants.yaml

kind-down: check-kind-cli
	kind delete cluster --name $(KIND_CLUSTER_NAME)

kind-load: check-docker-cli check-kind-cli
	docker build -f cmd/ingestion/Dockerfile -t ingestion:local .
	docker build -f cmd/processor/Dockerfile -t processor:local .
	docker build -t operator:local operator
	kind load docker-image ingestion:local processor:local operator:local --name $(KIND_CLUSTER_NAME)

kind-deploy: check-helm-cli
	helm repo add strimzi https://strimzi.io/charts/
	helm repo add cnpg https://cloudnative-pg.github.io/charts
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
		-n kafka -f deploy/kind/kafka/values.yaml --wait
	kubectl apply -f deploy/kind/kafka/kafka-cluster.yaml
	kubectl wait kafka/transaction-control-plane -n kafka --for=condition=Ready --timeout=300s
	helm upgrade --install cnpg cnpg/cloudnative-pg \
		-n cnpg-system --create-namespace -f deploy/kind/postgres/values.yaml --wait
	kubectl apply -k deploy/kind/postgres
	kubectl wait cluster/transaction-control-plane -n postgres --for=condition=Ready --timeout=300s
	kubectl create configmap ledger-migrations -n postgres \
		--from-file=$(MIGRATIONS_PATH) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/kind/postgres/migrate-job.yaml
	kubectl wait job/ledger-migrate -n postgres --for=condition=Complete --timeout=120s
	kubectl create secret generic processor-db-secret -n transaction-control-plane \
		--from-literal=uri="$$(kubectl get secret transaction-control-plane-app -n postgres -o jsonpath='{.data.uri}' | base64 -d)" \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -k deploy/kind/ingestion
	kubectl apply -k deploy/kind/processor
	helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
		-n monitoring --create-namespace -f deploy/kind/prometheus/values.yaml --wait
	kubectl apply -k deploy/kind/operator
	kubectl wait deployment/controller-manager -n tradingtenant-operator-system --for=condition=Available --timeout=180s
	kubectl apply -f deploy/kind/prometheus/servicemonitors.yaml
	kubectl apply -f deploy/kind/prometheus/alerts.yaml
	kubectl apply -f operator/config/samples/tradingtenant_v1alpha1_tradingtenant.yaml -n transaction-control-plane

kind-verify:
	@bash deploy/kind/verify.sh

kind-verify-isolation:
	@bash deploy/kind/verify-isolation.sh

kind-verify-fault-injection:
	@bash deploy/kind/verify-fault-injection.sh
