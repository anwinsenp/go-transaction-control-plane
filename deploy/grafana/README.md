# Grafana dashboard

`dashboard.json` is the "Transaction Control Plane" dashboard: throughput,
P50/P99 latency, Kafka consumer lag, operator reconcile duration, and
circuit breaker state, sourced from the metrics added in #24 (ingestion),
#25 (processor), and #26 (operator). See CLAUDE.md's Telemetry section for
the minimum-expected-metrics list this satisfies.

## Importing

The dashboard uses a templated `${DS_PROMETHEUS}` datasource input instead
of a hardcoded datasource UID, so it imports cleanly against any Prometheus
datasource without manual edits:

1. In Grafana, go to **Dashboards → New → Import**.
2. Upload `dashboard.json` (or paste its contents).
3. When prompted, select the Prometheus datasource that scrapes ingestion,
   processor, and the TradingTenant operator (see
   `deploy/kind/prometheus/servicemonitors.yaml` for what that datasource
   needs to be pointed at).
4. Click **Import**.

## Local Kind cluster

Against the local Kind deploy (`deploy/kind/`, see the top-level deploy
docs), Grafana is reached via port-forward — there's no public ingress:

```
kubectl -n monitoring port-forward svc/prometheus-grafana 3000:80
```

The bundled `kube-prometheus-stack` Prometheus datasource is auto-provisioned
in that Grafana instance (Helm release `prometheus` in the `monitoring`
namespace, per the `kind-deploy` Makefile target), so step 3 above just
means picking the one datasource already listed.

## Panels

| Panel | Metric(s) |
| --- | --- |
| Ingestion / Processor Transaction Throughput | `ingestion_transactions_processed_total`, `processor_transactions_processed_total` |
| Ingestion Publish Latency / Processor Reconciliation Latency (P50/P99) | `ingestion_publish_latency_seconds`, `processor_transaction_duration_seconds` |
| Processor Kafka Consumer Lag / Active Consumers | `processor_kafka_consumer_lag_messages`, `processor_kafka_active_consumer_count` |
| TradingTenant Reconcile Duration (P50/P99) | `tradingtenant_reconcile_duration_seconds` |
| Circuit Breaker State (Ingestion→Kafka, Processor→Postgres) | `ingestion_kafka_circuit_breaker_state`, `processor_postgres_circuit_breaker_state` |
| TradingTenant Isolation Transitions | `tradingtenant_isolation_transitions_total` |
