// Command operator is the entrypoint for the TradingTenant Kubernetes
// controller. This is wiring only: manager/config setup and
// SetupWithManager registration. Reconcile logic lives in
// operator/internal/controller; the Prometheus query client lives in
// operator/internal/promquery.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
	"github.com/anwinsenp/go-transaction-control-plane/operator/internal/controller"
	"github.com/anwinsenp/go-transaction-control-plane/operator/internal/promquery"
)

// scheme is shared package state rather than a local in run(), since
// client-go's AddToScheme functions require it at var-init time via
// utilruntime.Must; this mirrors the standard kubebuilder scaffold.
var scheme = runtime.NewScheme() //nolint:gochecknoglobals // controller-runtime scheme registration convention

func init() { //nolint:gochecknoinits // required to populate the package-level scheme before main runs
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(tradingv1alpha1.AddToScheme(scheme))
}

// defaultReconcileTimeout and defaultRequeueInterval mirror
// controller.TradingTenantReconciler's own zero-value defaults (5s reconcile
// timeout, 30s requeue interval), applied here so OPERATOR_RECONCILE_TIMEOUT
// / OPERATOR_REQUEUE_INTERVAL only need to be set to override them.
const (
	defaultReconcileTimeout = 5 * time.Second
	defaultRequeueInterval  = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var metricsAddr string
	var healthProbeAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election so only one operator replica reconciles at a time.")
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthProbeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "tradingtenant-operator.controlplane.anwinsenp.dev",
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	operatorMetrics, err := controller.NewMetrics(ctrlmetrics.Registry)
	if err != nil {
		return fmt.Errorf("register operator metrics: %w", err)
	}

	promClient, err := promquery.NewClient(promquery.NewConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create prometheus query client: %w", err)
	}

	reconcileTimeout, err := durationFromEnv("OPERATOR_RECONCILE_TIMEOUT", defaultReconcileTimeout)
	if err != nil {
		return fmt.Errorf("resolve reconcile timeout: %w", err)
	}
	requeueInterval, err := durationFromEnv("OPERATOR_REQUEUE_INTERVAL", defaultRequeueInterval)
	if err != nil {
		return fmt.Errorf("resolve requeue interval: %w", err)
	}

	reconciler := &controller.TradingTenantReconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Observer:              promClient,
		TenantLabelName:       os.Getenv("OPERATOR_TENANT_LABEL_NAME"),
		ReconcileTimeout:      reconcileTimeout,
		RequeueInterval:       requeueInterval,
		Metrics:               operatorMetrics,
		Recorder:              mgr.GetEventRecorderFor("tradingtenant-controller"),
		IngestionImage:        os.Getenv("OPERATOR_INGESTION_IMAGE"),
		ProcessorImage:        os.Getenv("OPERATOR_PROCESSOR_IMAGE"),
		IngestionEnvFrom:      configMapEnvFromSourcesFromEnv("OPERATOR_INGESTION_CONFIGMAP_ENV_FROM"),
		ProcessorEnvFrom:      configMapEnvFromSourcesFromEnv("OPERATOR_PROCESSOR_CONFIGMAP_ENV_FROM"),
		DedicatedNodeSelector: keyValueMapFromEnv("OPERATOR_DEDICATED_NODE_SELECTOR"),
		DedicatedTolerations:  tolerationsFromEnv("OPERATOR_DEDICATED_TOLERATIONS"),
	}

	if reconciler.IngestionImage == "" || reconciler.ProcessorImage == "" {
		return fmt.Errorf("OPERATOR_INGESTION_IMAGE and OPERATOR_PROCESSOR_IMAGE must both be set")
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup TradingTenant controller: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add healthz check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readyz check: %w", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}

// durationFromEnv reads name as a time.ParseDuration-formatted value (e.g.
// "30s"), falling back to fallback when name is unset.
func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

// configMapEnvFromSourcesFromEnv reads name as a comma-separated list of
// ConfigMap names and returns each as an EnvFromSource, in the order given.
// An unset or empty value returns nil, matching
// TradingTenantReconciler.IngestionEnvFrom/ProcessorEnvFrom's documented
// "optional" default.
func configMapEnvFromSourcesFromEnv(name string) []corev1.EnvFromSource {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}

	var envFrom []corev1.EnvFromSource
	for _, configMapName := range strings.Split(raw, ",") {
		configMapName = strings.TrimSpace(configMapName)
		if configMapName == "" {
			continue
		}
		envFrom = append(envFrom, corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
			},
		})
	}
	return envFrom
}

// keyValueMapFromEnv reads name as a comma-separated list of key=value
// pairs (e.g. "pool=dedicated,zone=us-east-1a") into a map. Malformed pairs
// (missing "=") are skipped rather than failing startup, since a node
// selector is an optional scheduling hint, not a required config value.
func keyValueMapFromEnv(name string) map[string]string {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}

	pairs := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found || key == "" {
			continue
		}
		pairs[key] = strings.TrimSpace(value)
	}
	if len(pairs) == 0 {
		return nil
	}
	return pairs
}

// tolerationsFromEnv reads name as a comma-separated list of
// "key=value:Effect" entries (or "key:Effect" for an Exists-operator
// toleration with no value, e.g. "pool:NoSchedule") into
// []corev1.Toleration. Effect is required per entry and must be one of
// NoSchedule, PreferNoSchedule, or NoExecute. Malformed entries (missing
// Effect, invalid Effect, or empty key) are skipped rather than failing
// startup, since dedicated tolerations are optional scheduling config,
// matching keyValueMapFromEnv's tolerance for bad input.
func tolerationsFromEnv(name string) []corev1.Toleration {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}

	var tolerations []corev1.Toleration
	for _, entry := range strings.Split(raw, ",") {
		keyValue, effect, hasEffect := strings.Cut(strings.TrimSpace(entry), ":")
		if !hasEffect {
			continue
		}
		effect = strings.TrimSpace(effect)
		switch corev1.TaintEffect(effect) {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
		default:
			continue
		}

		key, value, hasValue := strings.Cut(keyValue, "=")
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		operator := corev1.TolerationOpExists
		if hasValue {
			operator = corev1.TolerationOpEqual
			value = strings.TrimSpace(value)
		} else {
			value = ""
		}

		tolerations = append(tolerations, corev1.Toleration{
			Key:      key,
			Operator: operator,
			Value:    value,
			Effect:   corev1.TaintEffect(effect),
		})
	}
	if len(tolerations) == 0 {
		return nil
	}
	return tolerations
}
