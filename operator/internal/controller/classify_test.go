package controller

import (
	"testing"

	tradingv1alpha1 "github.com/anwinsenp/go-transaction-control-plane/operator/api/v1alpha1"
)

func TestClassify(t *testing.T) {
	baseSpec := tradingv1alpha1.TradingTenantSpec{
		MinReplicas: 2,
		MaxReplicas: 10,
		Scaling: tradingv1alpha1.ScalingPolicy{
			KafkaLagThreshold:     1000,
			P99LatencyThresholdMs: 50,
		},
	}

	testCases := []struct {
		name            string
		spec            tradingv1alpha1.TradingTenantSpec
		baseline        int32
		alreadyIsolated bool
		lag             int64
		p99Ms           int32
		partitionCount  int32
		want            reconcileDecision
	}{
		{
			name:           "lag high and latency high scales up to max replicas",
			spec:           baseSpec,
			baseline:       3,
			lag:            2000,
			p99Ms:          100,
			partitionCount: 12,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateScaling,
				replicas: 10,
			},
		},
		{
			name:           "lag high latency normal with partition headroom scales toward partition count",
			spec:           baseSpec,
			baseline:       3,
			lag:            2000,
			p99Ms:          10,
			partitionCount: 6,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateScaling,
				replicas: 6,
			},
		},
		{
			name:           "lag high latency normal at partition parity isolates",
			spec:           baseSpec,
			baseline:       6,
			lag:            2000,
			p99Ms:          10,
			partitionCount: 6,
			want: reconcileDecision{
				state:                tradingv1alpha1.TradingTenantStateIsolated,
				replicas:             6,
				setDedicatedNodePool: true,
			},
		},
		{
			name:           "lag normal latency high is degraded",
			spec:           baseSpec,
			baseline:       4,
			lag:            100,
			p99Ms:          100,
			partitionCount: 12,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateDegraded,
				replicas: 4,
			},
		},
		{
			name:           "lag normal latency normal is stable when not already isolated",
			spec:           baseSpec,
			baseline:       4,
			lag:            100,
			p99Ms:          10,
			partitionCount: 12,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateStable,
				replicas: 4,
			},
		},
		{
			name:            "lag normal latency normal keeps isolated state once already isolated",
			spec:            baseSpec,
			baseline:        4,
			alreadyIsolated: true,
			lag:             100,
			p99Ms:           10,
			partitionCount:  12,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateIsolated,
				replicas: 4,
			},
		},
		{
			name:            "baseline below min replicas passes through unclamped outside the scaling branches",
			spec:            baseSpec,
			baseline:        1,
			alreadyIsolated: false,
			lag:             100,
			p99Ms:           100,
			partitionCount:  12,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateDegraded,
				replicas: 1,
			},
		},
		{
			name:           "lag and latency high target above max replicas clamps down to max",
			spec:           baseSpec,
			baseline:       9,
			lag:            2000,
			p99Ms:          100,
			partitionCount: 20,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateScaling,
				replicas: 10,
			},
		},
		{
			name:           "headroom target above max replicas clamps down to max via partition count",
			spec:           baseSpec,
			baseline:       2,
			lag:            2000,
			p99Ms:          10,
			partitionCount: 50,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateScaling,
				replicas: 10,
			},
		},
		{
			name:           "headroom target below min replicas clamps up to min",
			spec:           baseSpec,
			baseline:       0,
			lag:            2000,
			p99Ms:          10,
			partitionCount: 1,
			want: reconcileDecision{
				state:    tradingv1alpha1.TradingTenantStateScaling,
				replicas: 2,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := classify(testCase.spec, testCase.baseline, testCase.alreadyIsolated, testCase.lag, testCase.p99Ms, testCase.partitionCount)
			if got != testCase.want {
				t.Errorf("classify() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}
