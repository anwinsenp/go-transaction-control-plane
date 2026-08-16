// Package controller implements the TradingTenant reconcile loop: the
// joint lag/latency decision logic documented in
// docs/DESIGN-operator.md's "Reconcile decision table".
package controller

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
	"github.com/anwinsenp/go-transaction-control-plane/operator/internal/promquery"
)

const defaultTenantLabelName = "tenant_id"

// TenantObserver reads a tenant's current Kafka lag, P99 processing
// latency, and topic partition count from Prometheus, scoped by a
// low-cardinality tenant label (see promquery.TenantLabel and #25, #42 for
// the label-strategy discussion). Implemented by *promquery.Client.
type TenantObserver interface {
	ObservedKafkaLag(ctx context.Context, label promquery.TenantLabel) (int64, error)
	ObservedP99Ms(ctx context.Context, label promquery.TenantLabel) (int32, error)
	ObservedPartitionCount(ctx context.Context, label promquery.TenantLabel) (int32, error)
}

// TradingTenantReconciler reconciles a TradingTenant object.
type TradingTenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Observer supplies the observed lag/latency/partition-count signals
	// this reconciler classifies against spec.scaling thresholds.
	Observer TenantObserver

	// TenantLabelName is the Prometheus label key used to scope
	// Observer queries to one tenant. Defaults to "tenant_id" if unset.
	TenantLabelName string

	// ReconcileTimeout bounds the entire reconcile pass's external calls:
	// the tenant Get, each Observer call, the isolation spec Update, and
	// the status Update all share this same context.Context. Defaults to
	// 5 seconds if unset.
	ReconcileTimeout time.Duration

	// RequeueInterval is the steady-state delay before the next reconcile
	// pass. Defaults to 30 seconds if unset.
	RequeueInterval time.Duration
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
func (r *TradingTenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reconcileTimeout := r.ReconcileTimeout
	if reconcileTimeout <= 0 {
		reconcileTimeout = 5 * time.Second
	}
	reconcileCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	var tenant tradingv1alpha1.TradingTenant
	if err := r.Get(reconcileCtx, req.NamespacedName, &tenant); err != nil {
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

	decision := classify(tenant.Spec, baseline, tenant.Spec.Isolation.DedicatedNodePool, lag, p99Ms, partitionCount)

	if decision.setDedicatedNodePool && !tenant.Spec.Isolation.DedicatedNodePool {
		tenant.Spec.Isolation.DedicatedNodePool = true
		if err := r.Update(reconcileCtx, &tenant); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling tradingtenant %s: updating isolation spec: %w", req.NamespacedName, err)
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

	requeueInterval := r.RequeueInterval
	if requeueInterval <= 0 {
		requeueInterval = 30 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
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
