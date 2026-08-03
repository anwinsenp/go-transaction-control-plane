package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidPoolConfig indicates a PoolConfig failed validation. Since
// PoolConfig's fields are exported, callers can construct one directly
// instead of going through PoolConfigFromEnv, so both NewPool and
// PoolConfigFromEnv validate before use rather than trusting the env path
// alone.
var ErrInvalidPoolConfig = errors.New("invalid pool config")

// PoolConfig controls the sizing of a Postgres connection pool. Left at
// pgxpool's defaults (100 max conns, no idle/lifetime caps), a pool doesn't
// account for how many replicas of a service run concurrently against the
// same Postgres instance, or for a managed database's own connection
// ceiling — so every pool built for this project goes through PoolConfig
// rather than pgxpool.New's defaults.
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// LocalPoolConfig returns pool settings sized for the docker-compose local
// stack: a single Postgres instance with no other tenants, so the pool can
// stay small without risking exhaustion and doesn't need aggressive idle
// reclamation.
func LocalPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConns:        5,
		MinConns:        1,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
}

// SandboxPoolConfig returns pool settings sized for the public sandbox
// deployment: a modestly-sized managed Postgres instance shared by every
// replica of both the ingestion and processor services. MaxConns is kept
// conservative per-service so a handful of replicas can't collectively
// exhaust the database's own connection ceiling, and MaxConnIdleTime is
// short so idle connections are returned quickly, leaving headroom for
// bursts from the public endpoint's traffic. MaxConnLifetime rotates
// connections periodically so long-lived ones don't pin a stale route
// behind a load balancer or pooler.
func SandboxPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConns:        20,
		MinConns:        5,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 2 * time.Minute,
	}
}

// PoolConfigFromEnv resolves a PoolConfig for the deployment named by the
// APP_ENV environment variable ("local" or "sandbox"; defaults to "local"
// if unset), then applies any per-field overrides found in
// PGPOOL_MAX_CONNS, PGPOOL_MIN_CONNS, PGPOOL_MAX_CONN_LIFETIME, and
// PGPOOL_MAX_CONN_IDLE_TIME. The two duration variables accept any format
// understood by time.ParseDuration (e.g. "30m").
func PoolConfigFromEnv() (PoolConfig, error) {
	appEnv := os.Getenv("APP_ENV")
	if appEnv == "" {
		appEnv = "local"
	}

	var config PoolConfig
	switch appEnv {
	case "local":
		config = LocalPoolConfig()
	case "sandbox":
		config = SandboxPoolConfig()
	default:
		return PoolConfig{}, fmt.Errorf("resolve pool config: unknown APP_ENV %q (want \"local\" or \"sandbox\")", appEnv)
	}

	if err := overrideInt32FromEnv("PGPOOL_MAX_CONNS", &config.MaxConns, 1); err != nil {
		return PoolConfig{}, err
	}
	if err := overrideInt32FromEnv("PGPOOL_MIN_CONNS", &config.MinConns, 0); err != nil {
		return PoolConfig{}, err
	}
	if err := overrideDurationFromEnv("PGPOOL_MAX_CONN_LIFETIME", &config.MaxConnLifetime); err != nil {
		return PoolConfig{}, err
	}
	if err := overrideDurationFromEnv("PGPOOL_MAX_CONN_IDLE_TIME", &config.MaxConnIdleTime); err != nil {
		return PoolConfig{}, err
	}

	if err := config.validate(); err != nil {
		return PoolConfig{}, err
	}

	return config, nil
}

// validate checks that cfg is safe to hand to pgxpool. MaxConns must be
// positive (a zero-sized pool can never hand out a connection), MinConns
// must not exceed MaxConns (pgxpool otherwise silently fails to pre-warm
// idle connections beyond MaxConns), and both duration fields must be
// strictly positive so a direct PoolConfig{} construction can't silently
// bypass the same checks overrideDurationFromEnv already enforces on the
// env path.
func (cfg PoolConfig) validate() error {
	if cfg.MaxConns < 1 {
		return fmt.Errorf("%w: MaxConns %d must be at least 1", ErrInvalidPoolConfig, cfg.MaxConns)
	}
	if cfg.MinConns < 0 {
		return fmt.Errorf("%w: MinConns %d must not be negative", ErrInvalidPoolConfig, cfg.MinConns)
	}
	if cfg.MinConns > cfg.MaxConns {
		return fmt.Errorf("%w: MinConns %d must not exceed MaxConns %d", ErrInvalidPoolConfig, cfg.MinConns, cfg.MaxConns)
	}
	if cfg.MaxConnLifetime <= 0 {
		return fmt.Errorf("%w: MaxConnLifetime %s must be a positive duration", ErrInvalidPoolConfig, cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime <= 0 {
		return fmt.Errorf("%w: MaxConnIdleTime %s must be a positive duration", ErrInvalidPoolConfig, cfg.MaxConnIdleTime)
	}
	return nil
}

// overrideInt32FromEnv parses the env var name as an int32 into dest, if
// set. minAllowed enforces the lower bound each field needs: MaxConns must
// be positive (a zero-sized pool can never hand out a connection), while
// MinConns is meaningful at zero (a deliberate "don't pre-warm" choice).
func overrideInt32FromEnv(name string, dest *int32, minAllowed int32) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed < int64(minAllowed) {
		return fmt.Errorf("parse %s: value %d is below the minimum allowed %d", name, parsed, minAllowed)
	}
	*dest = int32(parsed)
	return nil
}

// overrideDurationFromEnv parses the env var name as a duration into dest,
// if set. Both MaxConnLifetime and MaxConnIdleTime must be strictly
// positive when explicitly overridden — a zero or negative value isn't a
// meaningful pool setting here, unlike the fields' own zero value which
// pgxpool interprets as "no cap."
func overrideDurationFromEnv(name string, dest *time.Duration) error {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed <= 0 {
		return fmt.Errorf("parse %s: value %s must be a positive duration", name, parsed)
	}
	*dest = parsed
	return nil
}

// NewPool builds a pgxpool.Pool for databaseURL, configured with cfg's pool
// sizing settings.
func NewPool(ctx context.Context, databaseURL string, cfg PoolConfig) (*pgxpool.Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime
	poolConfig.MaxConnIdleTime = cfg.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}
