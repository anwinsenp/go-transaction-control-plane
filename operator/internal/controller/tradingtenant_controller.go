// Package controller implements the TradingTenant reconcile loop: the
// joint lag/latency decision logic documented in
// docs/DESIGN-operator.md's "Reconcile decision table".
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
	"github.com/anwinsenp/go-transaction-control-plane/operator/internal/promquery"
)

// reasonDedicatedPoolCreated is the Event reason and isolation-transition
// counter label for the isolation.dedicatedNodePool spec flag flipping to
// true — a distinct signal from the TradingTenantState transitions, since
// it's a spec update, not a status.state value (see #58).
const reasonDedicatedPoolCreated = "DedicatedPoolCreated"

// reasonDedicatedPoolProvisioned and reasonDedicatedPoolTornDown are the
// Event reason and isolation-transition counter labels for the dedicated
// Deployment/Service/ConfigMap actually being created or deleted (see
// #55, ensureDedicatedPool/tearDownDedicatedPool) — distinct from
// reasonDedicatedPoolCreated, which fires once on the spec flag flip
// itself, before any resource exists.
const (
	reasonDedicatedPoolProvisioned = "DedicatedPoolProvisioned"
	reasonDedicatedPoolTornDown    = "DedicatedPoolTornDown"
)

// defaultTenantLabelName matches the "tenant" label every per-tenant gauge
// in this repo actually uses (see internal/metrics.KnownTenants and its
// callers in internal/ingestion/kafka and internal/processor/kafka) — not
// "tenant_id".
const defaultTenantLabelName = "tenant"

// TenantObserver reads a tenant's current Kafka lag, P99 processing
// latency, and topic partition count from Prometheus, scoped by a
// low-cardinality tenant label (see promquery.TenantLabel and #25, #42 for
// the label-strategy discussion). Implemented by *promquery.Client.
type TenantObserver interface {
	ObservedKafkaLag(ctx context.Context, label promquery.TenantLabel) (int64, error)
	ObservedP99Ms(ctx context.Context, label promquery.TenantLabel) (int32, error)
	ObservedPartitionCount(ctx context.Context, label promquery.TenantLabel) (int32, error)
	// ObservedPartitionStart returns the start index of the tenant's
	// reserved Kafka partition range, used to configure the dedicated
	// processor's manual partition assignment when the tenant is isolated
	// (see ensureDedicatedPool).
	ObservedPartitionStart(ctx context.Context, label promquery.TenantLabel) (int32, error)
}

// TradingTenantReconciler reconciles a TradingTenant object.
type TradingTenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Observer supplies the observed lag/latency/partition-count signals
	// this reconciler classifies against spec.scaling thresholds.
	Observer TenantObserver

	// TenantLabelName is the Prometheus label key used to scope
	// Observer queries to one tenant. Defaults to "tenant" if unset.
	TenantLabelName string

	// ReconcileTimeout bounds the entire reconcile pass's external calls:
	// the tenant Get, each Observer call, the isolation spec Update, and
	// the status Update all share this same context.Context. Defaults to
	// 5 seconds if unset.
	ReconcileTimeout time.Duration

	// RequeueInterval is the steady-state delay before the next reconcile
	// pass. Defaults to 30 seconds if unset.
	RequeueInterval time.Duration

	// Metrics reports reconcile loop duration and isolation state
	// transitions. Optional: nil skips recording, so tests and callers
	// that don't need metrics can leave it unset.
	Metrics *Metrics

	// Recorder emits Kubernetes Events for isolation state transitions
	// (see #58), the secondary audit trail alongside the Metrics counter.
	// Optional: nil skips emitting Events.
	Recorder record.EventRecorder

	// IngestionImage and ProcessorImage are the container images used for
	// the dedicated ingestion/processor Deployments created once a tenant
	// is isolated (see #55, ensureDedicatedPool). Required if any tenant
	// this reconciler manages can reach the isolation branch.
	IngestionImage string
	ProcessorImage string

	// IngestionEnvFrom and ProcessorEnvFrom are applied to the dedicated
	// Deployments' containers via envFrom, supplying the baseline
	// config/secrets (KAFKA_BROKERS, DATABASE_URL, etc.) every ingestion
	// or processor instance needs — the same ConfigMap/Secret references
	// the shared-pool Deployments use, so isolated tenants stay on
	// identical connection config. The dedicated processor's
	// ProcessorEnvFrom always gets one more source appended at build time:
	// the per-tenant ConfigMap carrying its manual partition assignment
	// (see ensureDedicatedConfigMap).
	IngestionEnvFrom []corev1.EnvFromSource
	ProcessorEnvFrom []corev1.EnvFromSource

	// DedicatedNodeSelector and DedicatedTolerations target the dedicated
	// node pool that isolated tenants' Deployments are scheduled onto (see
	// #55, ensureDedicatedPool). Both are applied verbatim to every
	// dedicated Deployment this reconciler creates; leaving them unset
	// schedules onto any node, which defeats the point of isolation but
	// doesn't break idempotency, so it's not validated here.
	DedicatedNodeSelector map[string]string
	DedicatedTolerations  []corev1.Toleration
}

// reconcileDecision is the outcome of classifying one tenant's observed
// signals against its scaling policy.
type reconcileDecision struct {
	state                tradingv1alpha1.TradingTenantState
	replicas             int32
	setDedicatedNodePool bool
}

// +kubebuilder:rbac:groups=tradingtenant.controlplane.anwinsenp.dev,resources=tradingtenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=tradingtenant.controlplane.anwinsenp.dev,resources=tradingtenants/status,verbs=get;update;patch

// Reconcile implements the joint-signal scale/isolate/degrade decision
// table from docs/DESIGN-operator.md. It is idempotent: given the same
// TradingTenant object state and the same observed signals, it computes
// the same status every time it runs.
func (r *TradingTenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (reconcileResult ctrl.Result, reconcileErr error) {
	start := time.Now()
	defer func() {
		if r.Metrics == nil {
			return
		}
		outcome := resultSuccess
		if reconcileErr != nil {
			outcome = resultError
		}
		r.Metrics.observeDuration(outcome, time.Since(start).Seconds())
	}()

	reconcileTimeout := r.ReconcileTimeout
	if reconcileTimeout <= 0 {
		reconcileTimeout = 5 * time.Second
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	var tenant tradingv1alpha1.TradingTenant
	if err := r.Get(reconcileCtx, req.NamespacedName, &tenant); err != nil {
		// client.IgnoreNotFound deliberately returns nil for a NotFound
		// error (object deleted mid-reconcile), so the defer above records
		// this pass under result=success alongside true successful
		// reconciles rather than a distinct outcome. This is intentional:
		// don't introduce a third "result" label value for it.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	labelName := r.TenantLabelName
	if labelName == "" {
		labelName = defaultTenantLabelName
	}
	label := promquery.TenantLabel{Name: labelName, Value: tenant.Spec.TenantID}

	lag, err := r.Observer.ObservedKafkaLag(reconcileCtx, label)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: observed kafka lag: %w", req.NamespacedName, err)
	}
	p99Ms, err := r.Observer.ObservedP99Ms(reconcileCtx, label)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: observed p99 latency: %w", req.NamespacedName, err)
	}
	partitionCount, err := r.Observer.ObservedPartitionCount(reconcileCtx, label)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: observed partition count: %w", req.NamespacedName, err)
	}

	baseline := tenant.Status.CurrentReplicas
	if baseline < tenant.Spec.MinReplicas {
		baseline = tenant.Spec.MinReplicas
	}

	oldState := tenant.Status.State
	decision := classify(tenant.Spec, baseline, tenant.Spec.Isolation.DedicatedNodePool, lag, p99Ms, partitionCount)

	// isolation.dedicatedNodePool is the only field this reconciler writes
	// to spec rather than status: it's a durable placement decision (#20)
	// that node-pool provisioning depends on, not an ephemeral observed
	// signal, so it must persist as a normal spec field rather than being
	// freely overwritten like status is on every pass.
	if decision.setDedicatedNodePool && !tenant.Spec.Isolation.DedicatedNodePool {
		tenant.Spec.Isolation.DedicatedNodePool = true
		if err := r.Update(reconcileCtx, &tenant); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: updating isolation spec: %w", req.NamespacedName, err)
		}
		r.recordTransition(&tenant, reasonDedicatedPoolCreated, corev1.EventTypeNormal,
			fmt.Sprintf("Tenant %s moved to a dedicated node pool", tenant.Spec.TenantID))
	}

	// Dedicated resource provisioning/teardown is driven off the
	// (possibly just-flipped) spec flag, not decision.state, since state
	// stays Isolated on every subsequent pass (classify's alreadyIsolated
	// branch) while this get-or-create/get-or-delete step must still run
	// every pass to stay idempotent and to catch a manual de-isolation.
	if tenant.Spec.Isolation.DedicatedNodePool {
		partitionStart, err := r.Observer.ObservedPartitionStart(reconcileCtx, label)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: observed partition start: %w", req.NamespacedName, err)
		}
		created, err := r.ensureDedicatedPool(reconcileCtx, &tenant, partitionStart, partitionCount, decision.replicas)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: ensuring dedicated pool: %w", req.NamespacedName, err)
		}
		if created {
			r.recordTransition(&tenant, reasonDedicatedPoolProvisioned, corev1.EventTypeNormal,
				fmt.Sprintf("Dedicated node pool resources provisioned for tenant %s", tenant.Spec.TenantID))
		}
		// Published on every pass, not just when created: partitionStart/
		// partitionCount can drift (topic resize, reservation config change)
		// without the dedicated pool's Deployments/Service/ConfigMap
		// themselves needing to change, and the shared ingestion
		// publisher/dedicated processor's hot reload (ADR 0007 part 3)
		// depends on this map staying current every reconcile.
		if err := r.ensureTenantPartitionMapEntry(reconcileCtx, &tenant, partitionStart, partitionCount); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: publishing tenant partition map entry: %w", req.NamespacedName, err)
		}
	} else {
		tornDown, err := r.tearDownDedicatedPool(reconcileCtx, &tenant)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: tearing down dedicated pool: %w", req.NamespacedName, err)
		}
		if tornDown {
			r.recordTransition(&tenant, reasonDedicatedPoolTornDown, corev1.EventTypeNormal,
				fmt.Sprintf("Dedicated node pool resources torn down for tenant %s", tenant.Spec.TenantID))
		}
		if err := r.removeTenantPartitionMapEntry(reconcileCtx, &tenant); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: removing tenant partition map entry: %w", req.NamespacedName, err)
		}
	}

	tenant.Status.State = decision.state
	tenant.Status.CurrentReplicas = decision.replicas
	tenant.Status.ObservedKafkaLag = lag
	tenant.Status.ObservedP99Ms = p99Ms
	tenant.Status.ObservedPartitionCount = partitionCount
	tenant.Status.LastReconcileTime = metav1.Now()

	if err := r.Status().Update(reconcileCtx, &tenant); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: updating status: %w", req.NamespacedName, err)
	}

	// Recorded only after Status().Update succeeds: if the update fails,
	// the requeued retry re-observes the same oldState and must detect the
	// same transition again, not skip it as already-recorded.
	if decision.state != oldState {
		r.recordTransition(&tenant, string(decision.state), eventTypeForState(decision.state),
			fmt.Sprintf("Tenant %s transitioned from %q to %q", tenant.Spec.TenantID, oldState, decision.state))
	}

	requeueInterval := r.RequeueInterval
	if requeueInterval <= 0 {
		requeueInterval = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// recordTransition emits a Kubernetes Event via r.Recorder and increments
// r.Metrics's isolation-transitions counter for reason. Both are optional
// (nil Recorder/Metrics skip their half), and the Event is the secondary
// audit trail — the counter is the primary Grafana/Alertmanager signal
// (see #58, ADR 0007).
func (r *TradingTenantReconciler) recordTransition(tenant *tradingv1alpha1.TradingTenant, reason, eventType, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(tenant, eventType, reason, message)
	}
	if r.Metrics != nil {
		r.Metrics.observeIsolationTransition(reason)
	}
}

// eventTypeForState reports Isolated and Degraded as Warning events, since
// both indicate a tenant needs attention; Stable and Scaling are routine
// operation and reported as Normal.
func eventTypeForState(state tradingv1alpha1.TradingTenantState) string {
	switch state {
	case tradingv1alpha1.TradingTenantStateIsolated, tradingv1alpha1.TradingTenantStateDegraded:
		return corev1.EventTypeWarning
	default:
		return corev1.EventTypeNormal
	}
}

// classify implements the decision table in docs/DESIGN-operator.md,
// including the state diagram's isolation-persists special case: once a
// tenant is isolated, "both normal" keeps state Isolated rather than
// reverting to Stable, since the dedicatedNodePool flag never auto-reverts.
func classify(
	spec tradingv1alpha1.TradingTenantSpec,
	baseline int32,
	alreadyIsolated bool,
	lag int64,
	p99Ms int32,
	partitionCount int32,
) reconcileDecision {
	lagHigh := lag > spec.Scaling.KafkaLagThreshold
	latencyHigh := p99Ms > spec.Scaling.P99LatencyThresholdMs

	switch {
	case lagHigh && latencyHigh:
		return reconcileDecision{
			state:    tradingv1alpha1.TradingTenantStateScaling,
			replicas: spec.MaxReplicas,
		}
	case lagHigh && !latencyHigh && baseline < partitionCount:
		target := spec.MaxReplicas
		if partitionCount < target {
			target = partitionCount
		}
		return reconcileDecision{
			state:    tradingv1alpha1.TradingTenantStateScaling,
			replicas: clampReplicas(target, spec.MinReplicas, spec.MaxReplicas),
		}
	case lagHigh && !latencyHigh:
		return reconcileDecision{
			state:                tradingv1alpha1.TradingTenantStateIsolated,
			replicas:             baseline,
			setDedicatedNodePool: true,
		}
	case !lagHigh && latencyHigh:
		return reconcileDecision{
			state:    tradingv1alpha1.TradingTenantStateDegraded,
			replicas: baseline,
		}
	default:
		state := tradingv1alpha1.TradingTenantStateStable
		if alreadyIsolated {
			state = tradingv1alpha1.TradingTenantStateIsolated
		}
		return reconcileDecision{state: state, replicas: baseline}
	}
}

func clampReplicas(value, minimum, maximum int32) int32 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

// SetupWithManager registers this reconciler with the controller manager.
func (r *TradingTenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tradingv1alpha1.TradingTenant{}).
		Complete(r)
}
