package main

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func TestDurationFromEnv(t *testing.T) {
	testCases := []struct {
		name      string
		envValue  string
		setEnv    bool
		fallback  time.Duration
		wantValue time.Duration
		wantErr   bool
	}{
		{
			name:      "unset returns fallback",
			setEnv:    false,
			fallback:  5 * time.Second,
			wantValue: 5 * time.Second,
		},
		{
			name:      "empty returns fallback",
			setEnv:    true,
			envValue:  "",
			fallback:  30 * time.Second,
			wantValue: 30 * time.Second,
		},
		{
			name:      "valid duration overrides fallback",
			setEnv:    true,
			envValue:  "45s",
			fallback:  5 * time.Second,
			wantValue: 45 * time.Second,
		},
		{
			name:     "malformed duration returns error",
			setEnv:   true,
			envValue: "not-a-duration",
			fallback: 5 * time.Second,
			wantErr:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			const envName = "TEST_OPERATOR_DURATION"
			if testCase.setEnv {
				t.Setenv(envName, testCase.envValue)
			}

			got, err := durationFromEnv(envName, testCase.fallback)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("durationFromEnv(%q) = nil error, want error", testCase.envValue)
				}
				return
			}
			if err != nil {
				t.Fatalf("durationFromEnv(%q) unexpected error: %v", testCase.envValue, err)
			}
			if got != testCase.wantValue {
				t.Errorf("durationFromEnv(%q) = %v, want %v", testCase.envValue, got, testCase.wantValue)
			}
		})
	}
}

func TestConfigMapEnvFromSourcesFromEnv(t *testing.T) {
	testCases := []struct {
		name     string
		envValue string
		setEnv   bool
		want     []corev1.EnvFromSource
	}{
		{
			name:   "unset returns nil",
			setEnv: false,
			want:   nil,
		},
		{
			name:     "empty returns nil",
			setEnv:   true,
			envValue: "",
			want:     nil,
		},
		{
			name:     "single configmap name",
			setEnv:   true,
			envValue: "ingestion-config",
			want: []corev1.EnvFromSource{
				envFromSourceFor("ingestion-config"),
			},
		},
		{
			name:     "multiple configmap names preserve order",
			setEnv:   true,
			envValue: "ingestion-config,shared-config",
			want: []corev1.EnvFromSource{
				envFromSourceFor("ingestion-config"),
				envFromSourceFor("shared-config"),
			},
		},
		{
			name:     "blank entries and surrounding whitespace are skipped",
			setEnv:   true,
			envValue: " ingestion-config ,, shared-config ,   ",
			want: []corev1.EnvFromSource{
				envFromSourceFor("ingestion-config"),
				envFromSourceFor("shared-config"),
			},
		},
		{
			name:     "only blank entries returns nil",
			setEnv:   true,
			envValue: " , , ",
			want:     nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			const envName = "TEST_OPERATOR_CONFIGMAP_ENV_FROM"
			if testCase.setEnv {
				t.Setenv(envName, testCase.envValue)
			}

			got := configMapEnvFromSourcesFromEnv(envName)
			assertEnvFromSourcesEqual(t, got, testCase.want)
		})
	}
}

func TestKeyValueMapFromEnv(t *testing.T) {
	testCases := []struct {
		name     string
		envValue string
		setEnv   bool
		want     map[string]string
	}{
		{
			name:   "unset returns nil",
			setEnv: false,
			want:   nil,
		},
		{
			name:     "empty returns nil",
			setEnv:   true,
			envValue: "",
			want:     nil,
		},
		{
			name:     "single pair",
			setEnv:   true,
			envValue: "pool=dedicated",
			want:     map[string]string{"pool": "dedicated"},
		},
		{
			name:     "multiple pairs",
			setEnv:   true,
			envValue: "pool=dedicated,zone=us-east-1a",
			want:     map[string]string{"pool": "dedicated", "zone": "us-east-1a"},
		},
		{
			name:     "malformed pairs missing equals are skipped",
			setEnv:   true,
			envValue: "pool=dedicated,malformed,zone=us-east-1a",
			want:     map[string]string{"pool": "dedicated", "zone": "us-east-1a"},
		},
		{
			name:     "pair with empty key is skipped",
			setEnv:   true,
			envValue: "=novalue,pool=dedicated",
			want:     map[string]string{"pool": "dedicated"},
		},
		{
			name:     "pair with empty value is kept",
			setEnv:   true,
			envValue: "pool=",
			want:     map[string]string{"pool": ""},
		},
		{
			name:     "only malformed pairs returns nil",
			setEnv:   true,
			envValue: "malformed,alsomalformed",
			want:     nil,
		},
		{
			name:     "whitespace around pairs is trimmed",
			setEnv:   true,
			envValue: " pool=dedicated , zone=us-east-1a ",
			want:     map[string]string{"pool": "dedicated", "zone": "us-east-1a"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			const envName = "TEST_OPERATOR_KEY_VALUE_MAP"
			if testCase.setEnv {
				t.Setenv(envName, testCase.envValue)
			}

			got := keyValueMapFromEnv(envName)
			if len(got) != len(testCase.want) {
				t.Fatalf("keyValueMapFromEnv(%q) = %v, want %v", testCase.envValue, got, testCase.want)
			}
			for key, value := range testCase.want {
				if got[key] != value {
					t.Errorf("keyValueMapFromEnv(%q)[%q] = %q, want %q", testCase.envValue, key, got[key], value)
				}
			}
		})
	}
}

func TestTolerationsFromEnv(t *testing.T) {
	testCases := []struct {
		name     string
		envValue string
		setEnv   bool
		want     []corev1.Toleration
	}{
		{
			name:   "unset returns nil",
			setEnv: false,
			want:   nil,
		},
		{
			name:     "empty returns nil",
			setEnv:   true,
			envValue: "",
			want:     nil,
		},
		{
			name:     "single key=value:Effect entry",
			setEnv:   true,
			envValue: "pool=dedicated:NoSchedule",
			want: []corev1.Toleration{
				{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "dedicated", Effect: corev1.TaintEffectNoSchedule},
			},
		},
		{
			name:     "multiple entries preserve order",
			setEnv:   true,
			envValue: "pool=dedicated:NoSchedule,zone=us-east-1a:NoExecute",
			want: []corev1.Toleration{
				{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "dedicated", Effect: corev1.TaintEffectNoSchedule},
				{Key: "zone", Operator: corev1.TolerationOpEqual, Value: "us-east-1a", Effect: corev1.TaintEffectNoExecute},
			},
		},
		{
			name:     "key:Effect with no value defaults to Exists operator",
			setEnv:   true,
			envValue: "pool:PreferNoSchedule",
			want: []corev1.Toleration{
				{Key: "pool", Operator: corev1.TolerationOpExists, Value: "", Effect: corev1.TaintEffectPreferNoSchedule},
			},
		},
		{
			name:     "entry missing effect is skipped",
			setEnv:   true,
			envValue: "pool=dedicated,zone=us-east-1a:NoExecute",
			want: []corev1.Toleration{
				{Key: "zone", Operator: corev1.TolerationOpEqual, Value: "us-east-1a", Effect: corev1.TaintEffectNoExecute},
			},
		},
		{
			name:     "entry with invalid effect is skipped",
			setEnv:   true,
			envValue: "pool=dedicated:NotARealEffect,zone=us-east-1a:NoExecute",
			want: []corev1.Toleration{
				{Key: "zone", Operator: corev1.TolerationOpEqual, Value: "us-east-1a", Effect: corev1.TaintEffectNoExecute},
			},
		},
		{
			name:     "entry with empty key is skipped",
			setEnv:   true,
			envValue: "=dedicated:NoSchedule,zone=us-east-1a:NoExecute",
			want: []corev1.Toleration{
				{Key: "zone", Operator: corev1.TolerationOpEqual, Value: "us-east-1a", Effect: corev1.TaintEffectNoExecute},
			},
		},
		{
			name:     "only malformed entries returns nil",
			setEnv:   true,
			envValue: "malformed,alsomalformed",
			want:     nil,
		},
		{
			name:     "whitespace around entries is trimmed",
			setEnv:   true,
			envValue: " pool=dedicated:NoSchedule , zone=us-east-1a:NoExecute ",
			want: []corev1.Toleration{
				{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "dedicated", Effect: corev1.TaintEffectNoSchedule},
				{Key: "zone", Operator: corev1.TolerationOpEqual, Value: "us-east-1a", Effect: corev1.TaintEffectNoExecute},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			const envName = "TEST_OPERATOR_TOLERATIONS"
			if testCase.setEnv {
				t.Setenv(envName, testCase.envValue)
			}

			got := tolerationsFromEnv(envName)
			assertTolerationsEqual(t, got, testCase.want)
		})
	}
}

func assertTolerationsEqual(t *testing.T, got, want []corev1.Toleration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d tolerations, want %d (%v vs %v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func envFromSourceFor(configMapName string) corev1.EnvFromSource {
	return corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
		},
	}
}

func assertEnvFromSourcesEqual(t *testing.T, got, want []corev1.EnvFromSource) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d EnvFromSource entries, want %d (%v vs %v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].ConfigMapRef == nil || want[i].ConfigMapRef == nil {
			t.Fatalf("entry %d: ConfigMapRef nil mismatch: got %v want %v", i, got[i], want[i])
		}
		if got[i].ConfigMapRef.Name != want[i].ConfigMapRef.Name {
			t.Errorf("entry %d: got name %q, want %q", i, got[i].ConfigMapRef.Name, want[i].ConfigMapRef.Name)
		}
	}
}
