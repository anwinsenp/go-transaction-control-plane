#!/usr/bin/env bash
# Drives one real transaction through the live Kind stack (ingestion -> Kafka
# -> processor -> Postgres + Prometheus -> operator reconcile) and asserts,
# not just prints, that the loop actually closed:
#   1. ingestion accepted the transaction (curl -f)
#   2. the sample TradingTenant reached a known status.state
#   3. the operator's tradingtenant_reconcile_duration_seconds{result="success"}
#      count actually increased, proving a fresh reconcile pass ran off the
#      back of this transaction rather than an earlier one
# Exits non-zero with a "FAIL: ..." message on any of these; exits 0 with a
# "PASS: ..." summary otherwise. Invoked by `make kind-verify`.
set -euo pipefail

app_namespace="transaction-control-plane"
operator_namespace="tradingtenant-operator-system"
tenant="tenant-a"
metrics_local_port=18080
reconcile_wait_attempts=20

known_states="Stable Scaling Isolated Degraded"

port_forward_pid=""
cleanup() {
	if [[ -n "$port_forward_pid" ]]; then
		kill "$port_forward_pid" 2>/dev/null || true
		wait "$port_forward_pid" 2>/dev/null || true
	fi
}
trap cleanup EXIT

reconcile_success_count() {
	curl -sf "http://127.0.0.1:${metrics_local_port}/metrics" \
		| awk -F' ' '/^tradingtenant_reconcile_duration_seconds_count\{result="success"\}/ {print $2; found=1} END {if (!found) print 0}'
}

echo "==> starting port-forward to operator metrics service"
kubectl port-forward -n "$operator_namespace" svc/controller-manager-metrics-service "${metrics_local_port}:8080" \
	>/tmp/kind-verify-portforward.log 2>&1 &
port_forward_pid=$!

metrics_ready=""
for _ in $(seq 1 20); do
	if curl -sf "http://127.0.0.1:${metrics_local_port}/metrics" >/dev/null 2>&1; then
		metrics_ready=1
		break
	fi
	if ! kill -0 "$port_forward_pid" 2>/dev/null; then
		break
	fi
	sleep 0.5
done
if [[ -z "$metrics_ready" ]]; then
	echo "FAIL: could not reach operator metrics endpoint via port-forward"
	cat /tmp/kind-verify-portforward.log || true
	exit 1
fi

success_count_before=$(reconcile_success_count)
echo "==> reconcile success count before: ${success_count_before}"

event_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
occurred_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "==> sending sample transaction for tenant ${tenant}"
kubectl run curl-verify --rm -i --restart=Never --image=curlimages/curl:8.11.1 -n "$app_namespace" -- \
	curl -sf -X POST http://ingestion:8080/v1/transactions \
	-H "Authorization: Bearer kind-local-dev-key" \
	-H "Content-Type: application/json" \
	-d "{\"event_id\":\"${event_id}\",\"tenant_id\":\"${tenant}\",\"instrument\":\"AAPL\",\"side\":\"BUY\",\"quantity\":\"10\",\"price\":\"150.25\",\"currency\":\"USD\",\"occurred_at\":\"${occurred_at}\"}"

echo "==> waiting for TradingTenant/${tenant} status.state to be set"
kubectl wait "tradingtenant/${tenant}" -n "$app_namespace" --for=jsonpath='{.status.state}' --timeout=120s

state=$(kubectl get "tradingtenant/${tenant}" -n "$app_namespace" -o jsonpath='{.status.state}')
echo "==> observed status.state: ${state}"

state_known=""
for known_state in $known_states; do
	if [[ "$state" == "$known_state" ]]; then
		state_known=1
		break
	fi
done
if [[ -z "$state_known" ]]; then
	echo "FAIL: status.state=${state} is not one of the known TradingTenantState values (${known_states})"
	exit 1
fi

echo "==> waiting for a fresh successful reconcile pass to be recorded"
success_count_after="$success_count_before"
reconciled=""
for _ in $(seq 1 "$reconcile_wait_attempts"); do
	success_count_after=$(reconcile_success_count)
	if awk -v before="$success_count_before" -v after="$success_count_after" 'BEGIN{exit !(after>before)}'; then
		reconciled=1
		break
	fi
	sleep 1
done

echo "==> reconcile success count after: ${success_count_after}"
if [[ -z "$reconciled" ]]; then
	echo "FAIL: tradingtenant_reconcile_duration_seconds_count{result=\"success\"} did not increase (before=${success_count_before}, after=${success_count_after})"
	exit 1
fi

echo "PASS: TradingTenant/${tenant} reconciled to state=${state}, reconcile success count ${success_count_before} -> ${success_count_after}"
