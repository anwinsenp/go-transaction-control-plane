#!/usr/bin/env bash
# Fences the live Postgres instance (CNPG's cnpg.io/fencedInstances
# annotation: shuts down the postgres process in place without deleting the
# pod or its data, cleanly reversible — see
# https://cloudnative-pg.io/documentation/current/fencing/), bursts
# transactions for tenant-a into a now-Postgres-less processor, and asserts,
# not just prints, that the failure paths documented in
# docs/DESIGN-processor.md actually fire on a live cluster:
#   1. the processor_postgres_circuit_breaker_state{repository="transactions"}
#      gauge (internal/ledger/breaker.go's TransactionRepositoryBreaker,
#      wrapping every reconcile's Postgres call) actually reaches Open (2)
#      after enough consecutive failures
#   2. failed reconciles actually land on the transaction-events-dlq topic
#      (internal/processor/kafka/consumer.go's publishToDLQ), checked via
#      the Kafka broker's own kafka-get-offsets.sh rather than a Prometheus
#      metric — there's no DLQ-specific counter today (see #53's sibling
#      gap around commit-after-reconcile; the DLQ path itself has no
#      dedicated metric, so this asserts against the topic directly)
#   3. once Postgres is unfenced, the breaker actually recovers to Closed
#      (0) on its own via the standard half-open probe (corebreaker.Machine),
#      not just that requests stop failing
# Exits non-zero with a "FAIL: ..." message on any of these; exits 0 with a
# "PASS: ..." summary otherwise, leaving the cluster in its normal (fenced
# off, breaker closed) state. Invoked by `make kind-verify-fault-injection`.
set -euo pipefail

app_namespace="transaction-control-plane"
kafka_namespace="kafka"
postgres_namespace="postgres"
tenant="tenant-a"
postgres_cluster="transaction-control-plane"
postgres_pod="transaction-control-plane-1"
kafka_pod="transaction-control-plane-dual-role-0"
dlq_topic="transaction-events-dlq"
processor_metrics_local_port=18454
fence_wait_seconds=60
unfence_wait_seconds=60
burst_request_count=30
burst_concurrency=15
breaker_open_wait_seconds=60
dlq_wait_seconds=60
# breaker_recovery_wait_seconds must clear the processor's OpenTimeout
# (defaultBreakerOpenTimeout in cmd/processor/main.go, 30s unless overridden
# via PROCESSOR_BREAKER_OPEN_TIMEOUT) before the probe transaction below has
# any chance of being let through as a half-open probe.
breaker_recovery_wait_seconds=40
breaker_close_wait_seconds=60

fenced=""
processor_pf_pid=""
cleanup() {
	if [[ -n "$processor_pf_pid" ]]; then
		kill "$processor_pf_pid" 2>/dev/null || true
		wait "$processor_pf_pid" 2>/dev/null || true
	fi
	if [[ -n "$fenced" ]]; then
		echo "==> cleanup: unfencing Postgres"
		kubectl annotate cluster/"$postgres_cluster" -n "$postgres_namespace" cnpg.io/fencedInstances- >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

start_port_forward() {
	local namespace="$1" target="$2" local_port="$3" remote_port="$4" log_file="$5"
	kubectl port-forward -n "$namespace" "$target" "${local_port}:${remote_port}" >"$log_file" 2>&1 &
	echo $!
}

wait_for_port_forward() {
	local local_port="$1" pid="$2" path="$3" log_file="$4"
	for _ in $(seq 1 20); do
		if curl -sf "http://127.0.0.1:${local_port}${path}" >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "$pid" 2>/dev/null; then
			break
		fi
		sleep 0.5
	done
	echo "FAIL: could not reach ${path} via port-forward on ${local_port}"
	cat "$log_file" || true
	exit 1
}

breaker_state() {
	local body=""
	for _ in $(seq 1 5); do
		if body=$(curl -sf "http://127.0.0.1:${processor_metrics_local_port}/metrics"); then
			break
		fi
		body=""
		sleep 1
	done
	if [[ -z "$body" ]]; then
		echo "FAIL: could not scrape processor metrics after 5 attempts" >&2
		exit 1
	fi
	echo "$body" | awk -F' ' '/^processor_postgres_circuit_breaker_state\{repository="transactions"\}/ {print $2; found=1} END {if (!found) print "unknown"}'
}

dlq_message_count() {
	kubectl exec -n "$kafka_namespace" "$kafka_pod" -- bin/kafka-get-offsets.sh \
		--bootstrap-server localhost:9092 --topic "$dlq_topic" \
		| awk -F: '{sum += $3} END {print sum + 0}'
}

# burst_load fires burst_request_count requests for tenant against a live
# processor whose Postgres is fenced, so every reconcile attempt fails.
# Wrapped the same way verify-isolation.sh's burst_load is: `kubectl run
# --rm -i` only cleans up its pod on a normal exit, so a stuck pod would
# otherwise hang this call (and everything after it) indefinitely.
burst_load() {
	# shellcheck disable=SC2016 # single-quoted on purpose: $TENANT/$COUNT/
	# $CONCURRENCY and the loop's own $i/$batch_end expand inside the remote
	# pod's sh, not this local shell.
	kubectl run fault-injection-loadgen --rm -i --restart=Never --image=curlimages/curl:8.11.1 -n "$app_namespace" \
		--env="TENANT=${tenant}" --env="COUNT=${burst_request_count}" --env="CONCURRENCY=${burst_concurrency}" -- \
		sh -c '
		i=0
		while [ "$i" -lt "$COUNT" ]; do
			batch_end=$((i + CONCURRENCY))
			while [ "$i" -lt "$batch_end" ] && [ "$i" -lt "$COUNT" ]; do
				event_id=$(cat /proc/sys/kernel/random/uuid)
				occurred_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
				curl -sf -X POST http://ingestion:8080/v1/transactions \
					-H "Authorization: Bearer kind-local-dev-key" \
					-H "Content-Type: application/json" \
					-d "{\"event_id\":\"${event_id}\",\"tenant_id\":\"${TENANT}\",\"instrument\":\"AAPL\",\"side\":\"BUY\",\"quantity\":\"10\",\"price\":\"150.25\",\"currency\":\"USD\",\"occurred_at\":\"${occurred_at}\"}" >/dev/null &
				i=$((i + 1))
			done
			wait
		done
		echo "load generation complete"
		' || true
	kubectl delete pod fault-injection-loadgen -n "$app_namespace" --ignore-not-found >/dev/null 2>&1 || true
}

send_one() {
	local event_id occurred_at
	event_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
	occurred_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	kubectl run fault-injection-probe --rm -i --restart=Never --image=curlimages/curl:8.11.1 -n "$app_namespace" -- \
		curl -sf -X POST http://ingestion:8080/v1/transactions \
		-H "Authorization: Bearer kind-local-dev-key" -H "Content-Type: application/json" \
		-d "{\"event_id\":\"${event_id}\",\"tenant_id\":\"${tenant}\",\"instrument\":\"AAPL\",\"side\":\"BUY\",\"quantity\":\"10\",\"price\":\"150.25\",\"currency\":\"USD\",\"occurred_at\":\"${occurred_at}\"}" \
		>/dev/null 2>&1 || true
}

echo "==> starting port-forward to processor metrics service"
processor_pf_pid=$(start_port_forward "$app_namespace" svc/processor "$processor_metrics_local_port" 8081 /tmp/kind-verify-fault-injection-processor-pf.log)
wait_for_port_forward "$processor_metrics_local_port" "$processor_pf_pid" /metrics /tmp/kind-verify-fault-injection-processor-pf.log

state_before=$(breaker_state)
echo "==> breaker state before: ${state_before} (0=closed, 1=half_open, 2=open)"
if [[ "$state_before" != "0" ]]; then
	echo "FAIL: breaker did not start closed (state=${state_before}) — cluster may already be in a degraded state"
	exit 1
fi

dlq_before=$(dlq_message_count)
echo "==> DLQ message count before: ${dlq_before}"

echo "==> fencing Postgres instance ${postgres_pod} (cnpg.io/fencedInstances=[\"*\"])"
kubectl annotate cluster/"$postgres_cluster" -n "$postgres_namespace" cnpg.io/fencedInstances='["*"]' --overwrite
fenced=1

echo "==> waiting up to ${fence_wait_seconds}s for ${postgres_pod} to report not-ready"
elapsed=0
postgres_fenced=""
while [[ "$elapsed" -lt "$fence_wait_seconds" ]]; do
	ready=$(kubectl get pod "$postgres_pod" -n "$postgres_namespace" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "")
	if [[ "$ready" == "false" ]]; then
		postgres_fenced=1
		break
	fi
	sleep 3
	elapsed=$((elapsed + 3))
done
if [[ -z "$postgres_fenced" ]]; then
	echo "FAIL: ${postgres_pod} did not report not-ready within ${fence_wait_seconds}s of fencing"
	exit 1
fi
echo "==> ${postgres_pod} confirmed fenced (not ready) after ${elapsed}s"

echo "==> bursting ${burst_request_count} transactions for tenant ${tenant} against the fenced processor"
burst_load

echo "==> waiting up to ${breaker_open_wait_seconds}s for the breaker to reach Open (2)"
elapsed=0
breaker_opened=""
state_after_burst="$state_before"
while [[ "$elapsed" -lt "$breaker_open_wait_seconds" ]]; do
	state_after_burst=$(breaker_state)
	if [[ "$state_after_burst" == "2" ]]; then
		breaker_opened=1
		break
	fi
	sleep 2
	elapsed=$((elapsed + 2))
done
echo "==> breaker state after burst: ${state_after_burst}"
if [[ -z "$breaker_opened" ]]; then
	echo "FAIL: processor_postgres_circuit_breaker_state{repository=\"transactions\"} did not reach Open (2) within ${breaker_open_wait_seconds}s (last observed: ${state_after_burst})"
	exit 1
fi

echo "==> waiting up to ${dlq_wait_seconds}s for ${dlq_topic} message count to increase"
elapsed=0
dlq_after="$dlq_before"
dlq_routed=""
while [[ "$elapsed" -lt "$dlq_wait_seconds" ]]; do
	dlq_after=$(dlq_message_count)
	if awk -v before="$dlq_before" -v after="$dlq_after" 'BEGIN{exit !(after>before)}'; then
		dlq_routed=1
		break
	fi
	sleep 3
	elapsed=$((elapsed + 3))
done
echo "==> DLQ message count after: ${dlq_after}"
if [[ -z "$dlq_routed" ]]; then
	echo "FAIL: ${dlq_topic} message count did not increase (before=${dlq_before}, after=${dlq_after})"
	exit 1
fi

echo "==> unfencing Postgres instance ${postgres_pod}"
kubectl annotate cluster/"$postgres_cluster" -n "$postgres_namespace" cnpg.io/fencedInstances- >/dev/null 2>&1
fenced=""

echo "==> waiting up to ${unfence_wait_seconds}s for ${postgres_pod} to report ready again"
elapsed=0
postgres_recovered=""
while [[ "$elapsed" -lt "$unfence_wait_seconds" ]]; do
	ready=$(kubectl get pod "$postgres_pod" -n "$postgres_namespace" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "")
	if [[ "$ready" == "true" ]]; then
		postgres_recovered=1
		break
	fi
	sleep 3
	elapsed=$((elapsed + 3))
done
if [[ -z "$postgres_recovered" ]]; then
	echo "FAIL: ${postgres_pod} did not report ready again within ${unfence_wait_seconds}s of unfencing"
	exit 1
fi
echo "==> ${postgres_pod} confirmed ready again after ${elapsed}s"

echo "==> waiting ${breaker_recovery_wait_seconds}s for the breaker's OpenTimeout to elapse, then sending a probe transaction"
sleep "$breaker_recovery_wait_seconds"
send_one

echo "==> waiting up to ${breaker_close_wait_seconds}s for the breaker to recover to Closed (0)"
elapsed=0
breaker_closed=""
state_final="$state_after_burst"
while [[ "$elapsed" -lt "$breaker_close_wait_seconds" ]]; do
	state_final=$(breaker_state)
	if [[ "$state_final" == "0" ]]; then
		breaker_closed=1
		break
	fi
	# A half-open probe only fires on the next call through the breaker, so
	# keep sending single probes rather than just polling the gauge.
	send_one
	sleep 3
	elapsed=$((elapsed + 3))
done
echo "==> breaker state after recovery: ${state_final}"
if [[ -z "$breaker_closed" ]]; then
	echo "FAIL: processor_postgres_circuit_breaker_state{repository=\"transactions\"} did not recover to Closed (0) within ${breaker_close_wait_seconds}s of unfencing (last observed: ${state_final})"
	exit 1
fi

cat <<EOF
PASS: circuit breaker closed(0) -> open(2) -> closed(0) across a real Postgres
      fence/unfence cycle; ${dlq_topic} message count ${dlq_before} -> ${dlq_after}.

View it live:
  kubectl -n monitoring port-forward svc/prometheus-grafana 3000:80
  -> import deploy/grafana/dashboard.json, see the "Kafka and Postgres
     circuit breaker state" panel show the open/half-open/closed cycle.
EOF
