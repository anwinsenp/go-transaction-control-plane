#!/usr/bin/env bash
# Drives a real tenant into the operator's Isolated (noisy-neighbor,
# dedicated-node-pool) state on a live Kind cluster and asserts, not just
# prints, that the whole loop actually closed:
#   1. scales the shared processor Deployment to 0 replicas, bursts tenant-b
#      transactions through ingestion (unaffected by processor being down,
#      it publishes straight to Kafka) so they pile up unconsumed, then
#      scales processor back to 1 — deterministically pushing
#      processor_kafka_consumer_lag_messages{tenant="tenant-b"} over its low
#      kafkaLagThreshold for however long the resumed processor takes to
#      drain the backlog. (A raw request-rate burst against a live processor
#      was tried first and dropped: on a Kind-sized cluster the processor
#      drains messages about as fast as ingestion can accept them, so lag
#      never actually builds — pausing consumption is the reliable way to
#      get there locally.) P99 latency stays under its generously high
#      p99LatencyThresholdMs throughout, since draining a backlog doesn't
#      make any single message slower to process, only delayed. This briefly
#      stops consumption for every tenant sharing the processor, not just
#      tenant-b — acceptable for a short local demo, not something to run
#      against a real deployment.
#
#      Getting the resumed drain to actually show up as a scraped Prometheus
#      sample took three changes, all reverted in cleanup:
#        - KAFKA_MAX_POLL_RECORDS on the processor: with the default
#          unbounded poll, a resumed consumer drains its entire backlog in a
#          single PollRecords call and the lag gauge jumps straight from its
#          pre-burst value to caught-up-again without ever reporting the
#          backlog in between (confirmed with sub-second polling against a
#          real cluster). Bounding records-per-poll makes the gauge update
#          incrementally instead of once. See internal/processor/kafka/
#          consumer.go's Config.MaxPollRecords doc.
#        - Even bounded, a local Kind cluster drains a multi-thousand-message
#          backlog in well under Prometheus's 10s scrape interval (confirmed:
#          25,000 messages fully reconciled in under 5s) — bounding poll size
#          alone isn't enough, the whole drain has to actually take long
#          enough for a scrape to land inside it. So the processor's CPU
#          limit is temporarily dropped to 15m (from 250m, see
#          deploy/kind/processor/deployment.yaml) for the resumed drain,
#          which is throttled by the kernel's CFS quota into taking tens of
#          seconds instead of single-digit seconds — not a code change, a
#          real (if extreme) resource constraint.
#        - ingestion's default rate limit (50 req/s, burst 100 —
#          internal/api/rate_limiter.go) is raised transiently, so the burst
#          below can actually queue up thousands of messages instead of
#          being throttled to a couple hundred.
#   2. applies deploy/kind/isolation-demo-tenant.yaml (TradingTenant/tenant-b,
#      tuned so the very first Isolated-eligible reconcile pass reaches it
#      directly, see that file's comments) right before resuming the
#      processor, not at the start: the operator's promquery.ObservedKafkaLag
#      errors on "no series matched" until the resumed processor reports its
#      first sample, so an earlier-created TradingTenant just accumulates
#      failed reconciles and, past a few of them, controller-runtime's
#      exponential backoff — confirmed to push its next retry minutes out,
#      past this script's own wait budget below, even once real data exists.
#      Creating the object fresh right before resuming keeps its retry
#      backoff cold.
#   3. waits for TradingTenant/tenant-b's status.state to become "Isolated"
#   4. asserts the dedicated ingestion/processor Deployments the reconciler
#      is supposed to create on isolation actually exist and become Available
#   5. asserts tradingtenant_isolation_transitions_total{transition="Isolated"}
#      actually increased, not just that status.state happens to read Isolated
#   6. asserts the TenantIsolatedNoisyNeighborSuspected Prometheus alert
#      (deploy/kind/prometheus/alerts.yaml) reaches state "firing"
# Exits non-zero with a "FAIL: ..." message on any of these; exits 0 with a
# "PASS: ..." summary otherwise, leaving the isolated tenant and its
# dedicated pool running so results can be viewed live in Grafana/Prometheus
# (see README.md's "Demoing noisy-tenant isolation" section). Invoked by
# `make kind-verify-isolation`.
set -euo pipefail

app_namespace="transaction-control-plane"
operator_namespace="tradingtenant-operator-system"
monitoring_namespace="monitoring"
tenant="tenant-b"
operator_metrics_local_port=18453
prometheus_local_port=19453
prometheus_svc="prometheus-kube-prometheus-prometheus"
load_request_count=6000
load_concurrency=60
processor_pause_wait_seconds=60
isolated_wait_seconds=240
alert_wait_seconds=180
demo_max_poll_records=10
demo_ingestion_rate_limit_rps=5000
demo_ingestion_rate_limit_burst=6000
demo_processor_cpu_throttled="15m"
# processor_cpu_request_original/processor_cpu_limit_original are snapshotted
# from the live Deployment right before patching (below), not hardcoded, so
# cleanup restores whatever was actually there rather than assuming
# deploy/kind/processor/deployment.yaml's current defaults.
processor_cpu_request_original=""
processor_cpu_limit_original=""

operator_pf_pid=""
prometheus_pf_pid=""
max_poll_records_set=""
ingestion_rate_limit_set=""
processor_cpu_throttled_set=""
cleanup() {
	for pid in "$operator_pf_pid" "$prometheus_pf_pid"; do
		if [[ -n "$pid" ]]; then
			kill "$pid" 2>/dev/null || true
			wait "$pid" 2>/dev/null || true
		fi
	done
	if [[ -n "$ingestion_rate_limit_set" ]]; then
		kubectl set env deployment/ingestion -n "$app_namespace" INGESTION_RATE_LIMIT_RPS- INGESTION_RATE_LIMIT_BURST- >/dev/null 2>&1 || true
	fi
	if [[ -n "$max_poll_records_set" ]]; then
		kubectl set env deployment/processor -n "$app_namespace" KAFKA_MAX_POLL_RECORDS- >/dev/null 2>&1 || true
	fi
	if [[ -n "$processor_cpu_throttled_set" ]]; then
		kubectl patch deployment/processor -n "$app_namespace" --type=json -p="[
			{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/cpu\",\"value\":\"${processor_cpu_request_original}\"},
			{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/cpu\",\"value\":\"${processor_cpu_limit_original}\"}
		]" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

# run_with_timeout runs "$@" in the background and kills it if it hasn't
# finished after seconds. Not `timeout`/`gtimeout`: neither ships on macOS
# by default (this project's Local development docs target macOS), so this
# is a portable bash-only equivalent — only used to bound burst_load's
# `kubectl run --rm -i` below, which has no built-in total-runtime timeout
# of its own.
run_with_timeout() {
	local seconds="$1"
	shift
	"$@" &
	local cmd_pid=$!
	(
		sleep "$seconds"
		kill "$cmd_pid" 2>/dev/null
	) &
	local watchdog_pid=$!
	local status=0
	wait "$cmd_pid" 2>/dev/null || status=$?
	kill "$watchdog_pid" 2>/dev/null
	wait "$watchdog_pid" 2>/dev/null
	return "$status"
}

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

# isolated_transitions_count retries on a failed scrape rather than letting
# a transient curl error (e.g. a momentary port-forward hiccup) silently
# read as a genuine "0" — awk's fallback for a metric line it can't find
# would otherwise be indistinguishable from an actual empty-response case.
isolated_transitions_count() {
	local body=""
	for _ in $(seq 1 5); do
		if body=$(curl -sf "http://127.0.0.1:${operator_metrics_local_port}/metrics"); then
			break
		fi
		body=""
		sleep 1
	done
	if [[ -z "$body" ]]; then
		echo "FAIL: could not scrape operator metrics after 5 attempts" >&2
		exit 1
	fi
	echo "$body" | awk -F' ' '/^tradingtenant_isolation_transitions_total\{[^}]*transition="Isolated"[^}]*\}/ {print $2; found=1} END {if (!found) print 0}'
}

echo "==> starting port-forward to operator metrics service"
operator_pf_pid=$(start_port_forward "$operator_namespace" svc/controller-manager-metrics-service "$operator_metrics_local_port" 8080 /tmp/kind-verify-isolation-operator-pf.log)
wait_for_port_forward "$operator_metrics_local_port" "$operator_pf_pid" /metrics /tmp/kind-verify-isolation-operator-pf.log

isolated_before=$(isolated_transitions_count)
echo "==> isolation transitions (transition=Isolated) before: ${isolated_before}"

pause_processor() {
	kubectl scale deployment/processor -n "$app_namespace" --replicas=0
	local elapsed=0 remaining
	while [[ "$elapsed" -lt "$processor_pause_wait_seconds" ]]; do
		remaining=$(kubectl get pods -n "$app_namespace" -l app=processor --no-headers 2>/dev/null | wc -l | tr -d ' ')
		if [[ "$remaining" == "0" ]]; then
			return 0
		fi
		sleep 2
		elapsed=$((elapsed + 2))
	done
	echo "FAIL: processor pod did not terminate within ${processor_pause_wait_seconds}s of scaling to 0"
	exit 1
}

# burst_load is wrapped in `timeout` and always cleans up the pod itself:
# `kubectl run --rm -i` only removes the pod on its own normal exit, so a
# stuck pod (stalled scheduling/image pull, or a hung curl inside) would
# otherwise block this call — and everything after it — indefinitely. A
# timeout here just fails this attempt; the caller's retry loop moves on to
# the next one rather than the whole script hanging.
burst_load() {
	# shellcheck disable=SC2016 # single-quoted on purpose: $TENANT/$COUNT/$CONCURRENCY
	# and the loop's own $i/$batch_end must expand inside the remote pod's sh,
	# not this local shell — the vars are passed in via --env, not interpolated here.
	if ! run_with_timeout 180 kubectl run isolation-demo-loadgen --rm -i --restart=Never --image=curlimages/curl:8.11.1 -n "$app_namespace" \
		--env="TENANT=${tenant}" --env="COUNT=${load_request_count}" --env="CONCURRENCY=${load_concurrency}" -- \
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
		'; then
		echo "WARN: load generation pod did not complete within 180s, cleaning up and letting this attempt fail"
		kubectl delete pod isolation-demo-loadgen -n "$app_namespace" --ignore-not-found >/dev/null 2>&1 || true
	fi
}

resume_processor() {
	kubectl scale deployment/processor -n "$app_namespace" --replicas=1
	if ! kubectl wait deployment/processor -n "$app_namespace" --for=condition=Available --timeout=120s; then
		echo "FAIL: processor did not become Available again after resuming"
		exit 1
	fi
}

echo "==> setting KAFKA_MAX_POLL_RECORDS=${demo_max_poll_records} on the processor so its resumed drain is observable (reverted on exit)"
kubectl set env deployment/processor -n "$app_namespace" "KAFKA_MAX_POLL_RECORDS=${demo_max_poll_records}"
max_poll_records_set=1

echo "==> throttling the processor's CPU to ${demo_processor_cpu_throttled} so the resumed drain takes long enough to be scraped (reverted on exit)"
processor_cpu_request_original=$(kubectl get deployment/processor -n "$app_namespace" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}')
processor_cpu_limit_original=$(kubectl get deployment/processor -n "$app_namespace" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}')
kubectl patch deployment/processor -n "$app_namespace" --type=json -p="[
	{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/cpu\",\"value\":\"${demo_processor_cpu_throttled}\"},
	{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/cpu\",\"value\":\"${demo_processor_cpu_throttled}\"}
]"
processor_cpu_throttled_set=1

echo "==> raising ingestion's rate limit so the burst below isn't throttled (reverted on exit)"
kubectl set env deployment/ingestion -n "$app_namespace" \
	"INGESTION_RATE_LIMIT_RPS=${demo_ingestion_rate_limit_rps}" "INGESTION_RATE_LIMIT_BURST=${demo_ingestion_rate_limit_burst}"
ingestion_rate_limit_set=1
if ! kubectl wait deployment/ingestion -n "$app_namespace" --for=condition=Available --timeout=120s; then
	echo "FAIL: ingestion did not become Available again after raising its rate limit"
	exit 1
fi

# The exact drain-vs-scrape timing needed to observe a mid-drain lag sample
# (see the block comment above) varies run to run even at fixed CPU/poll/
# volume settings — a live Kind cluster's exact scheduling isn't
# deterministic. Rather than chase one precise setting, retry the whole
# pause/burst/resume cycle up to demo_max_attempts times, each with its own
# shorter wait budget, instead of gambling everything on one attempt.
#
# TradingTenant/tenant-b is deleted and recreated fresh every attempt, not
# just once before the loop: an object left over from a failed attempt has
# already accumulated several requeues' worth of controller-runtime's
# exponential backoff, so its next scheduled retry can land well past this
# attempt's own wait budget even once real data exists — recreating it
# keeps that backoff cold for each attempt, same reasoning as applying it
# fresh right before the very first resume.
demo_max_attempts=3
attempt_wait_seconds=$((isolated_wait_seconds / demo_max_attempts))
state=""
for attempt in $(seq 1 "$demo_max_attempts"); do
	echo "==> attempt ${attempt}/${demo_max_attempts}: pausing processor and bursting ${load_request_count} transactions for tenant ${tenant} (concurrency ${load_concurrency})"
	pause_processor
	burst_load

	echo "==> applying TradingTenant/${tenant} (isolation-tuned thresholds)"
	kubectl delete -n "$app_namespace" -f deploy/kind/isolation-demo-tenant.yaml --ignore-not-found
	kubectl apply -n "$app_namespace" -f deploy/kind/isolation-demo-tenant.yaml

	resume_processor

	echo "==> waiting up to ${attempt_wait_seconds}s for TradingTenant/${tenant} status.state to become Isolated"
	elapsed=0
	while [[ "$elapsed" -lt "$attempt_wait_seconds" ]]; do
		state=$(kubectl get "tradingtenant/${tenant}" -n "$app_namespace" -o jsonpath='{.status.state}' 2>/dev/null || echo "")
		if [[ "$state" == "Isolated" ]]; then
			break
		fi
		sleep 5
		elapsed=$((elapsed + 5))
	done
	echo "==> observed status.state: ${state} (after attempt ${attempt}, ${elapsed}s)"
	if [[ "$state" == "Isolated" ]]; then
		break
	fi
done
if [[ "$state" != "Isolated" ]]; then
	echo "FAIL: TradingTenant/${tenant} did not reach status.state=Isolated within ${demo_max_attempts} attempts (last observed: ${state})"
	exit 1
fi

echo "==> asserting tradingtenant_isolation_transitions_total{transition=\"Isolated\"} increased"
isolated_after=$(isolated_transitions_count)
echo "==> isolation transitions after: ${isolated_after}"
if ! awk -v before="$isolated_before" -v after="$isolated_after" 'BEGIN{exit !(after>before)}'; then
	echo "FAIL: tradingtenant_isolation_transitions_total{transition=\"Isolated\"} did not increase (before=${isolated_before}, after=${isolated_after})"
	exit 1
fi

echo "==> asserting dedicated pool resources exist and become Available"
dedicated_ingestion="${tenant}-dedicated-ingestion"
dedicated_processor="${tenant}-dedicated-processor"
if ! kubectl wait "deployment/${dedicated_ingestion}" -n "$app_namespace" --for=condition=Available --timeout=120s; then
	echo "FAIL: Deployment/${dedicated_ingestion} did not become Available"
	exit 1
fi
if ! kubectl wait "deployment/${dedicated_processor}" -n "$app_namespace" --for=condition=Available --timeout=120s; then
	echo "FAIL: Deployment/${dedicated_processor} did not become Available"
	exit 1
fi
echo "==> dedicated pool confirmed: Deployment/${dedicated_ingestion}, Deployment/${dedicated_processor}"

echo "==> starting port-forward to Prometheus"
prometheus_pf_pid=$(start_port_forward "$monitoring_namespace" "svc/${prometheus_svc}" "$prometheus_local_port" 9090 /tmp/kind-verify-isolation-prometheus-pf.log)
wait_for_port_forward "$prometheus_local_port" "$prometheus_pf_pid" "/api/v1/query?query=up" /tmp/kind-verify-isolation-prometheus-pf.log

echo "==> waiting up to ${alert_wait_seconds}s for TenantIsolatedNoisyNeighborSuspected to reach alertstate=firing"
alert_state=""
elapsed=0
while [[ "$elapsed" -lt "$alert_wait_seconds" ]]; do
	alert_state=$(curl -sf "http://127.0.0.1:${prometheus_local_port}/api/v1/query" \
		--data-urlencode 'query=ALERTS{alertname="TenantIsolatedNoisyNeighborSuspected",alertstate="firing"}' \
		| grep -o '"alertstate":"firing"' || true)
	if [[ -n "$alert_state" ]]; then
		break
	fi
	sleep 5
	elapsed=$((elapsed + 5))
done
if [[ -z "$alert_state" ]]; then
	echo "FAIL: TenantIsolatedNoisyNeighborSuspected did not reach alertstate=firing within ${alert_wait_seconds}s"
	exit 1
fi
echo "==> TenantIsolatedNoisyNeighborSuspected is firing (after ${elapsed}s)"

cat <<EOF
PASS: TradingTenant/${tenant} reached status.state=Isolated, dedicated pool provisioned,
      isolation transitions ${isolated_before} -> ${isolated_after}, alert firing.

View it live:
  kubectl -n monitoring port-forward svc/prometheus-grafana 3000:80
  -> import deploy/grafana/dashboard.json, see the "TradingTenant Isolation
     Transitions" and "Active Operator Alerts" panels update.
  Prometheus alert UI: kubectl -n monitoring port-forward svc/${prometheus_svc} 9090:9090
  -> http://localhost:9090/alerts

Clean up (reverts isolation, reconciler tears down the dedicated pool on
its next pass — this flag never auto-reverts, per design):
  kubectl patch tradingtenant/${tenant} -n ${app_namespace} --type merge \\
    -p '{"spec":{"isolation":{"dedicatedNodePool":false}}}'
  kubectl delete tradingtenant/${tenant} -n ${app_namespace}
EOF
