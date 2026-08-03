package storage

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// poolEnvVars lists every environment variable PoolConfigFromEnv reads.
var poolEnvVars = []string{
	"APP_ENV",
	"PGPOOL_MAX_CONNS",
	"PGPOOL_MIN_CONNS",
	"PGPOOL_MAX_CONN_LIFETIME",
	"PGPOOL_MAX_CONN_IDLE_TIME",
}

// setPoolEnv sets exactly the env vars in overrides and clears every other
// var PoolConfigFromEnv reads, so each subtest observes only the env it
// asked for regardless of what an earlier subtest set. t.Setenv registers
// its own cleanup on the *testing.T it's called against, so values are
// restored the moment this subtest returns — subtests don't leak into one
// another even without this helper, but clearing explicitly also covers
// the case where APP_ENV or PGPOOL_* happen to be set in the environment
// the test binary itself was launched from.
func setPoolEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for _, name := range poolEnvVars {
		value, ok := overrides[name]
		if !ok {
			value = ""
		}
		t.Setenv(name, value)
	}
}

func TestPoolConfigFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want PoolConfig
	}{
		{
			name: "no env vars set defaults to local",
			env:  nil,
			want: LocalPoolConfig(),
		},
		{
			name: "APP_ENV=local explicit",
			env:  map[string]string{"APP_ENV": "local"},
			want: LocalPoolConfig(),
		},
		{
			name: "APP_ENV=sandbox",
			env:  map[string]string{"APP_ENV": "sandbox"},
			want: SandboxPoolConfig(),
		},
		{
			name: "APP_ENV set to empty string behaves like unset",
			env:  map[string]string{"APP_ENV": ""},
			want: LocalPoolConfig(),
		},
		{
			name: "PGPOOL_MAX_CONNS override only",
			env:  map[string]string{"PGPOOL_MAX_CONNS": "42"},
			want: PoolConfig{
				MaxConns:        42,
				MinConns:        LocalPoolConfig().MinConns,
				MaxConnLifetime: LocalPoolConfig().MaxConnLifetime,
				MaxConnIdleTime: LocalPoolConfig().MaxConnIdleTime,
			},
		},
		{
			name: "PGPOOL_MIN_CONNS override only",
			env:  map[string]string{"PGPOOL_MIN_CONNS": "3"},
			want: PoolConfig{
				MaxConns:        LocalPoolConfig().MaxConns,
				MinConns:        3,
				MaxConnLifetime: LocalPoolConfig().MaxConnLifetime,
				MaxConnIdleTime: LocalPoolConfig().MaxConnIdleTime,
			},
		},
		{
			name: "PGPOOL_MAX_CONN_LIFETIME override only",
			env:  map[string]string{"PGPOOL_MAX_CONN_LIFETIME": "10m"},
			want: PoolConfig{
				MaxConns:        LocalPoolConfig().MaxConns,
				MinConns:        LocalPoolConfig().MinConns,
				MaxConnLifetime: 10 * time.Minute,
				MaxConnIdleTime: LocalPoolConfig().MaxConnIdleTime,
			},
		},
		{
			name: "PGPOOL_MAX_CONN_IDLE_TIME override only",
			env:  map[string]string{"PGPOOL_MAX_CONN_IDLE_TIME": "90s"},
			want: PoolConfig{
				MaxConns:        LocalPoolConfig().MaxConns,
				MinConns:        LocalPoolConfig().MinConns,
				MaxConnLifetime: LocalPoolConfig().MaxConnLifetime,
				MaxConnIdleTime: 90 * time.Second,
			},
		},
		{
			name: "all four overrides combined on top of sandbox",
			env: map[string]string{
				"APP_ENV":                   "sandbox",
				"PGPOOL_MAX_CONNS":          "42",
				"PGPOOL_MIN_CONNS":          "7",
				"PGPOOL_MAX_CONN_LIFETIME":  "10m",
				"PGPOOL_MAX_CONN_IDLE_TIME": "90s",
			},
			want: PoolConfig{
				MaxConns:        42,
				MinConns:        7,
				MaxConnLifetime: 10 * time.Minute,
				MaxConnIdleTime: 90 * time.Second,
			},
		},
		{
			name: "PGPOOL_MAX_CONNS set to empty string behaves like unset",
			env:  map[string]string{"PGPOOL_MAX_CONNS": ""},
			want: LocalPoolConfig(),
		},
		{
			// MinConns is meaningful at zero (a deliberate "don't
			// pre-warm" choice), unlike MaxConns=0 which breaks the pool
			// entirely and is rejected — see TestPoolConfigFromEnvErrors.
			name: "PGPOOL_MIN_CONNS=0 override only",
			env:  map[string]string{"PGPOOL_MIN_CONNS": "0"},
			want: PoolConfig{
				MaxConns:        LocalPoolConfig().MaxConns,
				MinConns:        0,
				MaxConnLifetime: LocalPoolConfig().MaxConnLifetime,
				MaxConnIdleTime: LocalPoolConfig().MaxConnIdleTime,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPoolEnv(t, tt.env)

			config, err := PoolConfigFromEnv()
			if err != nil {
				t.Fatalf("PoolConfigFromEnv() error = %v", err)
			}
			if config != tt.want {
				t.Fatalf("PoolConfigFromEnv() = %+v, want %+v", config, tt.want)
			}
		})
	}
}

func TestPoolConfigFromEnvErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "unknown APP_ENV",
			env:  map[string]string{"APP_ENV": "staging"},
		},
		{
			name: "PGPOOL_MAX_CONNS not a number",
			env:  map[string]string{"PGPOOL_MAX_CONNS": "not-a-number"},
		},
		{
			name: "PGPOOL_MAX_CONNS overflows int32",
			env:  map[string]string{"PGPOOL_MAX_CONNS": "99999999999"},
		},
		{
			name: "PGPOOL_MIN_CONNS not a number",
			env:  map[string]string{"PGPOOL_MIN_CONNS": "not-a-number"},
		},
		{
			name: "PGPOOL_MAX_CONN_LIFETIME malformed duration",
			env:  map[string]string{"PGPOOL_MAX_CONN_LIFETIME": "not-a-duration"},
		},
		{
			name: "PGPOOL_MAX_CONN_IDLE_TIME malformed duration",
			env:  map[string]string{"PGPOOL_MAX_CONN_IDLE_TIME": "not-a-duration"},
		},
		{
			name: "PGPOOL_MAX_CONNS=0",
			env:  map[string]string{"PGPOOL_MAX_CONNS": "0"},
		},
		{
			name: "PGPOOL_MIN_CONNS negative",
			env:  map[string]string{"PGPOOL_MIN_CONNS": "-1"},
		},
		{
			name: "PGPOOL_MAX_CONN_LIFETIME=0s",
			env:  map[string]string{"PGPOOL_MAX_CONN_LIFETIME": "0s"},
		},
		{
			name: "PGPOOL_MAX_CONN_IDLE_TIME negative",
			env:  map[string]string{"PGPOOL_MAX_CONN_IDLE_TIME": "-5m"},
		},
		{
			name: "PGPOOL_MIN_CONNS greater than PGPOOL_MAX_CONNS",
			env: map[string]string{
				"PGPOOL_MAX_CONNS": "5",
				"PGPOOL_MIN_CONNS": "10",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setPoolEnv(t, tt.env)

			if _, err := PoolConfigFromEnv(); err == nil {
				t.Fatal("PoolConfigFromEnv() error = nil, want an error")
			}
		})
	}
}

func TestNewPoolAppliesConfig(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed test")
	}

	cfg := PoolConfig{
		MaxConns:        3,
		MinConns:        1,
		MaxConnLifetime: 15 * time.Minute,
		MaxConnIdleTime: 90 * time.Second,
	}

	ctx := context.Background()
	pool, err := NewPool(ctx, databaseURL, cfg)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	defer pool.Close()

	poolConfig := pool.Config()
	if poolConfig.MaxConns != cfg.MaxConns {
		t.Errorf("MaxConns = %d, want %d", poolConfig.MaxConns, cfg.MaxConns)
	}
	if poolConfig.MinConns != cfg.MinConns {
		t.Errorf("MinConns = %d, want %d", poolConfig.MinConns, cfg.MinConns)
	}
	if poolConfig.MaxConnLifetime != cfg.MaxConnLifetime {
		t.Errorf("MaxConnLifetime = %v, want %v", poolConfig.MaxConnLifetime, cfg.MaxConnLifetime)
	}
	if poolConfig.MaxConnIdleTime != cfg.MaxConnIdleTime {
		t.Errorf("MaxConnIdleTime = %v, want %v", poolConfig.MaxConnIdleTime, cfg.MaxConnIdleTime)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
}

func TestPoolConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PoolConfig
		wantErr bool
	}{
		{
			name: "LocalPoolConfig is valid",
			cfg:  LocalPoolConfig(),
		},
		{
			name: "SandboxPoolConfig is valid",
			cfg:  SandboxPoolConfig(),
		},
		{
			name: "MaxConns zero rejected",
			cfg: PoolConfig{
				MaxConns:        0,
				MinConns:        0,
				MaxConnLifetime: 30 * time.Minute,
				MaxConnIdleTime: 5 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "MinConns negative rejected",
			cfg: PoolConfig{
				MaxConns:        5,
				MinConns:        -1,
				MaxConnLifetime: 30 * time.Minute,
				MaxConnIdleTime: 5 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "MinConns greater than MaxConns rejected",
			cfg: PoolConfig{
				MaxConns:        5,
				MinConns:        10,
				MaxConnLifetime: 30 * time.Minute,
				MaxConnIdleTime: 5 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "MaxConnLifetime zero rejected",
			cfg: PoolConfig{
				MaxConns:        5,
				MinConns:        1,
				MaxConnLifetime: 0,
				MaxConnIdleTime: 5 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "MaxConnIdleTime zero rejected",
			cfg: PoolConfig{
				MaxConns:        5,
				MinConns:        1,
				MaxConnLifetime: 30 * time.Minute,
				MaxConnIdleTime: 0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr && err == nil {
				t.Fatal("validate() error = nil, want an error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validate() error = %v, want nil", err)
			}
		})
	}
}

func TestNewPoolRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  PoolConfig
	}{
		{
			name: "zero-value PoolConfig",
			cfg:  PoolConfig{},
		},
		{
			name: "MinConns greater than MaxConns",
			cfg: PoolConfig{
				MaxConns:        5,
				MinConns:        10,
				MaxConnLifetime: 30 * time.Minute,
				MaxConnIdleTime: 5 * time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPool(context.Background(), "postgres://ignored", tt.cfg)
			if err == nil {
				t.Fatal("NewPool() error = nil, want an error")
			}
			if !errors.Is(err, ErrInvalidPoolConfig) {
				t.Fatalf("NewPool() error = %v, want it to wrap ErrInvalidPoolConfig", err)
			}
		})
	}
}
