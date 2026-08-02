# Runbook: TradingTenant Operator Alerts

This runbook covers the alerts raised by the `TradingTenant` reconciler.
Each section matches one alert name and is linked directly from that
alert's `runbook_url` annotation, so start at the section matching the
alert you received.

## TenantScalingEvent

**Severity:** info, no page.

**What it means:** the operator scaled a tenant's replicas up in response
to rising lag and/or latency, using normal partition headroom. This is the
operator working as intended.

**Action:** none required. If you're reviewing this as part of a periodic
check, note the tenant and frequency. A tenant that scales repeatedly over
a week is a candidate for a higher baseline `minReplicas`, adjust that in
its `TradingTenant` spec during your next planned maintenance window rather
than reacting to a single event.

## TenantAtMaxReplicasWithGrowingLag

**Severity:** warning.

**What it means:** a tenant hit `maxReplicas` and lag is still climbing.
Automatic scaling has run out of room.

**Diagnosis:**
1. Check `observedPartitionCount` for this tenant's topic. If
   `maxReplicas` is set below the partition count, that's an easy fix,
   raise `maxReplicas` up to partition count and let the reconciler scale
   further on its own.
2. If `maxReplicas` already equals partition count, this tenant is
   partition-bound. Compare current traffic against the tenant's expected
   baseline. Genuine sustained growth means the topic itself needs more
   partitions.

**Action:**
- If `maxReplicas` was set conservatively below partition count: raise it,
  confirm the tenant resumes scaling on the next reconcile, close out.
- If already partition-bound and traffic growth looks structural: schedule
  a partition increase for this topic. This is a planned, off-peak change,
  since it affects key-to-partition hashing for every consumer on the
  topic, not just this tenant. Coordinate before running it.

## TenantIsolatedNoisyNeighborSuspected

**Severity:** warning, escalate to critical if lag keeps growing after
isolation.

**What it means:** the tenant's Kafka lag is high, but its P99 latency is
normal, and it's already at partition parity (`currentReplicas` equals
`observedPartitionCount`). More replicas can't help at this point, since
Kafka never assigns more than one consumer to a partition within a group.
The operator has isolated the tenant onto a dedicated node pool to stop it
from affecting shared-pool tenants, and stopped there. It can't repartition
Kafka or diagnose root cause on its own, that's why you got paged.

**Diagnosis:**
1. Pull per-partition message rate for this tenant's topic over the
   incident window (Kafka's `BrokerTopicMetrics` or your partition-count
   exporter). Even distribution across partitions with high overall volume
   points to real growth. Concentration on one or two partitions points to
   key skew.
2. Check the producer's partitioning key. If it hashes on `tenantID` alone
   and one tenant has several very active accounts, all of that tenant's
   traffic lands on the same partition regardless of replica count. Look
   for a low-cardinality or hot-value key as the likely cause.
3. Compare current volume against this tenant's baseline from the past
   month. Rule out a step change that might indicate a client-side issue
   (retry storm, duplicate sends) rather than legitimate growth, check the
   DLQ and idempotency-rejection metrics for that tenant over the same
   window.

**Action, by cause found:**
- **Key skew:** schedule a repartition with a better key (for example,
  `tenantID` plus a sub-key like `accountID`), or assign this tenant's hot
  accounts to dedicated partitions. Planned, off-peak change.
- **Genuine volume growth:** raise `maxReplicas` and schedule a partition
  count increase for the topic.
- **Client-side misbehavior:** contact the tenant's team. This is an
  operational conversation, not an infrastructure fix.
- **Isolation is an adequate long-term fit** (for example, this is a large
  tenant that reasonably warrants dedicated resources going forward): leave
  it isolated, close the alert, no further action.

**Reverting isolation:** the operator never reverts `dedicatedNodePool` to
`false` on its own, by design, to avoid thrashing a tenant whose metrics
sit near the threshold. Once the root cause is fixed, or you've decided to
leave the tenant isolated permanently, that decision and any spec change
is manual.

## TenantDegradedDownstreamBottleneck

**Severity:** warning.

**What it means:** a tenant's P99 latency is high but Kafka lag is normal.
Messages are being consumed on schedule, but each one takes too long to
process. This points downstream of Kafka, most likely Postgres. The
operator deliberately takes no scaling action here, since the bottleneck
isn't in anything it controls (replica count, node placement), and scaling
replicas wouldn't fix a slow database.

**Diagnosis:**
1. Check Postgres connection pool utilization for the processor service.
   Exhausted connections show up as increased wait time before a query even
   starts.
2. Check for lock contention or slow queries in the reconciliation write
   path for this tenant's transactions, `pg_stat_activity` and
   `pg_stat_statements` are the usual starting points.
3. Check Aurora/RDS resource metrics (CPU, IOPS) for the instance during
   the incident window, for a sign of the instance itself being saturated
   rather than a single query being the problem.

**Action:**
- Missing index or a slow query plan: add the index or fix the query,
  standard database tuning.
- Connection pool exhaustion: tune pool size, or investigate why
  connections aren't being released promptly.
- Instance-level saturation: this is a capacity decision (larger instance
  class, read replica for reporting load), not something to fix reactively
  mid-incident.

This alert cannot be resolved by adjusting the `TradingTenant` spec. Any
fix happens in the storage layer or its infrastructure.
