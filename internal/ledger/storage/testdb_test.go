package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// onceErr runs fn at most once and remembers whether it failed, so every
// caller after the first observes the same outcome instead of silently
// treating a failed first run as success. Plain sync.Once can't be used
// here directly: fn is expected to be safe to call from a test-helper
// context where the caller may abort via t.Fatalf (runtime.Goexit), and
// sync.Once marks itself done even when f only partially completes via
// Goexit, so we capture the result ourselves rather than relying on
// whether Do's closure ran to completion.
type onceErr struct {
	once sync.Once
	err  error
}

// Do runs fn exactly once across all calls to this onceErr and returns
// the error from that single run (nil or otherwise) on every call.
func (o *onceErr) Do(fn func() error) error {
	o.once.Do(func() { o.err = fn() })
	return o.err
}

// migrationsOnce ensures the migrations are applied at most once per test
// binary run: every table they create already exists on the second and
// later calls, so re-running them would fail.
var migrationsOnce onceErr

// testPool connects to the Postgres instance at DATABASE_URL, applies the
// migrations from ./migrations (once per test run), and truncates the
// ledger tables before returning so each test starts from a clean slate.
// Tests using this helper are skipped when DATABASE_URL isn't set (see #29
// for the local docker-compose stack that will set it by default).
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres-backed test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping test database: %v", err)
	}

	if err := migrationsOnce.Do(func() error { return applyMigrations(ctx, pool) }); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE TABLE reconciled_state, transactions RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate ledger tables: %v", err)
	}

	return pool
}

// applyMigrations runs every *.up.sql file in ./migrations in order.
// It's a minimal, dependency-free stand-in for the migrate CLI, sufficient
// for tests: schema drift here is caught by comparing against what
// `make migrate-up` produces, which is exercised manually/in CI, not
// re-implemented as a full migration runner here.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("resolve migrations directory: runtime.Caller failed")
	}
	migrationsDir := filepath.Join(filepath.Dir(thisFile), "migrations")

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations directory: %w", err)
	}

	var upFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sql" && filepath.Ext(strippedExt(entry.Name())) == ".up" {
			upFiles = append(upFiles, entry.Name())
		}
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

func strippedExt(name string) string {
	return name[:len(name)-len(filepath.Ext(name))]
}

// TestOnceErrPropagatesFailure guards against the bug this type replaced a
// bare sync.Once for: a failed first run must be visible to every later
// caller, not just the one that hit it. This mirrors what testPool needs
// from migrationsOnce, but exercises onceErr directly rather than a real
// Postgres connection.
func TestOnceErrPropagatesFailure(t *testing.T) {
	var subject onceErr
	failure := fmt.Errorf("boom")

	calls := 0
	fn := func() error {
		calls++
		return failure
	}

	firstErr := subject.Do(fn)
	if !errors.Is(firstErr, failure) {
		t.Fatalf("first Do() = %v, want %v", firstErr, failure)
	}

	secondErr := subject.Do(fn)
	if !errors.Is(secondErr, failure) {
		t.Fatalf("second Do() = %v, want %v (failure must propagate to later callers)", secondErr, failure)
	}

	if calls != 1 {
		t.Fatalf("fn called %d times, want 1 (Do must run fn at most once)", calls)
	}
}
