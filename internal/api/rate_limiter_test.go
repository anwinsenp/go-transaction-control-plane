package api

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRateLimiterRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  RateLimitConfig
	}{
		{name: "zero rate", cfg: RateLimitConfig{RequestsPerSecond: 0, Burst: 10}},
		{name: "negative rate", cfg: RateLimitConfig{RequestsPerSecond: -1, Burst: 10}},
		{name: "zero burst", cfg: RateLimitConfig{RequestsPerSecond: 10, Burst: 0}},
		{name: "negative burst", cfg: RateLimitConfig{RequestsPerSecond: 10, Burst: -1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := newRateLimiter(test.cfg)
			if err == nil {
				t.Fatalf("newRateLimiter(%+v) error = nil, want error", test.cfg)
			}
			if limiter != nil {
				t.Errorf("newRateLimiter(%+v) limiter = %v, want nil", test.cfg, limiter)
			}
		})
	}
}

func TestTokenBucketAllowsUpToBurstThenRejects(t *testing.T) {
	bucket := &tokenBucket{tokens: 3, lastRefill: time.Unix(0, 0)}
	now := time.Unix(0, 0)

	for i := 0; i < 3; i++ {
		if !bucket.allow(1, 3, now) {
			t.Fatalf("allow() call %d = false, want true", i)
		}
	}
	if bucket.allow(1, 3, now) {
		t.Fatal("allow() after burst exhausted = true, want false")
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	start := time.Unix(0, 0)
	bucket := &tokenBucket{tokens: 0, lastRefill: start}

	if bucket.allow(1, 1, start) {
		t.Fatal("allow() with empty bucket = true, want false")
	}

	if !bucket.allow(1, 1, start.Add(time.Second)) {
		t.Fatal("allow() one second later = false, want true")
	}
}

func TestTokenBucketDoesNotExceedBurstCapacity(t *testing.T) {
	start := time.Unix(0, 0)
	bucket := &tokenBucket{tokens: 2, lastRefill: start}

	// A long idle period should cap refill at burst, not accumulate
	// unbounded credit.
	later := start.Add(time.Hour)
	for i := 0; i < 2; i++ {
		if !bucket.allow(1, 2, later) {
			t.Fatalf("allow() call %d after long idle = false, want true", i)
		}
	}
	if bucket.allow(1, 2, later) {
		t.Fatal("allow() beyond burst capacity after long idle = true, want false")
	}
}

func TestTokenBucketRefillBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		startTokens float64
		rate        float64
		burst       float64
		elapsed     time.Duration
		wantAllow   bool
		wantTokens  float64
		checkTokens bool
	}{
		{
			name:        "fractional refill insufficient for another token",
			startTokens: 0,
			rate:        0.5,
			burst:       1,
			elapsed:     time.Second,
			wantAllow:   false,
			wantTokens:  0.5,
			checkTokens: true,
		},
		{
			name:        "fractional refill accumulates across calls to reach a full token",
			startTokens: 0.5,
			rate:        0.5,
			burst:       1,
			elapsed:     time.Second,
			wantAllow:   true,
			wantTokens:  0,
			checkTokens: true,
		},
		{
			name:        "burst of one at exact rate boundary allows precisely one request",
			startTokens: 0,
			rate:        1,
			burst:       1,
			elapsed:     time.Second,
			wantAllow:   true,
			wantTokens:  0,
			checkTokens: true,
		},
		{
			name:        "no elapsed time with no starting tokens rejects",
			startTokens: 0,
			rate:        1,
			burst:       1,
			elapsed:     0,
			wantAllow:   false,
			wantTokens:  0,
			checkTokens: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := time.Unix(0, 0)
			bucket := &tokenBucket{tokens: test.startTokens, lastRefill: start}

			got := bucket.allow(test.rate, test.burst, start.Add(test.elapsed))

			if got != test.wantAllow {
				t.Errorf("allow() = %v, want %v", got, test.wantAllow)
			}
			if test.checkTokens && bucket.tokens != test.wantTokens {
				t.Errorf("bucket.tokens after allow() = %v, want %v", bucket.tokens, test.wantTokens)
			}
		})
	}
}

func TestFnv32a(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "empty string", key: ""},
		{name: "short key", key: "a"},
		{name: "longer key", key: "shared-key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := fnv32a(test.key)
			second := fnv32a(test.key)
			if first != second {
				t.Errorf("fnv32a(%q) = %d then %d, want deterministic result", test.key, first, second)
			}
		})
	}

	// The FNV-1a loop never executes for an empty key, so the hash should
	// come back as the untouched offset basis.
	const offsetBasis32 = 2166136261
	if got := fnv32a(""); got != offsetBasis32 {
		t.Errorf(`fnv32a("") = %d, want offset basis %d`, got, offsetBasis32)
	}

	if fnv32a("key-a") == fnv32a("key-b") {
		t.Error(`fnv32a("key-a") == fnv32a("key-b"), want distinct hashes for distinct keys (spot check)`)
	}
}

// findKeyForShard deterministically searches for a key whose fnv32a hash
// masks to wantShard, other than exclude, so shard-boundary tests can target
// a specific shard instead of hoping an arbitrary key lands where needed.
func findKeyForShard(t *testing.T, wantShard int, exclude string) string {
	t.Helper()

	for i := 0; i < 1_000_000; i++ {
		key := "shard-probe-" + strconv.Itoa(i)
		if key == exclude {
			continue
		}
		if int(fnv32a(key)&(rateLimiterShardCount-1)) == wantShard {
			return key
		}
	}
	t.Fatalf("could not find a key hashing to shard %d", wantShard)
	return ""
}

func TestRateLimiterAllowIsolatesKeysAcrossDifferentShards(t *testing.T) {
	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1})
	if err != nil {
		t.Fatalf("newRateLimiter() error = %v", err)
	}

	keyA := findKeyForShard(t, 0, "")
	keyB := findKeyForShard(t, 1, keyA)

	if !limiter.allow(keyA) {
		t.Fatalf("allow(%q) first call = false, want true", keyA)
	}
	if limiter.allow(keyA) {
		t.Fatalf("allow(%q) second call = true, want false", keyA)
	}
	if !limiter.allow(keyB) {
		t.Fatalf("allow(%q) in a different shard = false, want true (should not share %q's exhausted bucket)", keyB, keyA)
	}
}

func TestRateLimiterAllowIsolatesKeysWithinSameShard(t *testing.T) {
	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1})
	if err != nil {
		t.Fatalf("newRateLimiter() error = %v", err)
	}

	keyA := findKeyForShard(t, 0, "")
	keyB := findKeyForShard(t, 0, keyA)

	if !limiter.allow(keyA) {
		t.Fatalf("allow(%q) first call = false, want true", keyA)
	}
	if limiter.allow(keyA) {
		t.Fatalf("allow(%q) second call = true, want false", keyA)
	}
	if !limiter.allow(keyB) {
		t.Fatalf("allow(%q) sharing %q's shard = false, want true (buckets in one shard must stay independent per key)", keyB, keyA)
	}
}

func TestRateLimiterAllowIsolatesKeys(t *testing.T) {
	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1})
	if err != nil {
		t.Fatalf("newRateLimiter() error = %v", err)
	}

	if !limiter.allow("key-a") {
		t.Fatal(`allow("key-a") first call = false, want true`)
	}
	if limiter.allow("key-a") {
		t.Fatal(`allow("key-a") second call = true, want false`)
	}
	if !limiter.allow("key-b") {
		t.Fatal(`allow("key-b") = false, want true (should not share key-a's exhausted bucket)`)
	}
}

// TestRateLimiterAllowConcurrentSameKey exercises allow() for a single key
// under contention from many goroutines at once, run with -race to catch
// data races in the lazily-created bucket and its mutex-guarded state. The
// refill rate is low enough (1/s) that the sub-millisecond test duration
// can't add a whole extra token, so exactly burst calls should succeed
// regardless of how the goroutines interleave.
func TestRateLimiterAllowConcurrentSameKey(t *testing.T) {
	const burst = 50
	const callers = 200

	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: burst})
	if err != nil {
		t.Fatalf("newRateLimiter() error = %v", err)
	}

	var allowedCount int64
	var waitGroup sync.WaitGroup
	waitGroup.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer waitGroup.Done()
			if limiter.allow("shared-key") {
				atomic.AddInt64(&allowedCount, 1)
			}
		}()
	}
	waitGroup.Wait()

	if allowedCount != burst {
		t.Errorf("allowedCount = %d, want exactly %d (burst capacity)", allowedCount, burst)
	}
}

// TestRateLimiterAllowConcurrentManyKeys exercises the double-checked bucket
// creation path for many distinct, never-seen keys hit concurrently at
// once, spreading contention across shards rather than piling every
// goroutine onto a single shard's lock the way
// TestRateLimiterAllowConcurrentSameKey does. Run with -race to catch data
// races in bucket creation.
func TestRateLimiterAllowConcurrentManyKeys(t *testing.T) {
	const keyCount = 500

	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1})
	if err != nil {
		t.Fatalf("newRateLimiter() error = %v", err)
	}

	results := make([]bool, keyCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(keyCount)
	for i := 0; i < keyCount; i++ {
		go func(index int) {
			defer waitGroup.Done()
			results[index] = limiter.allow("many-key-" + strconv.Itoa(index))
		}(i)
	}
	waitGroup.Wait()

	for index, allowed := range results {
		if !allowed {
			t.Errorf("allow(%q) = false, want true (first request for a brand-new key)", "many-key-"+strconv.Itoa(index))
		}
	}

	var totalBuckets int
	for shardIndex := range limiter.shards {
		limiter.shards[shardIndex].mu.RLock()
		totalBuckets += len(limiter.shards[shardIndex].buckets)
		limiter.shards[shardIndex].mu.RUnlock()
	}
	if totalBuckets != keyCount {
		t.Errorf("total buckets across shards = %d, want %d (one per key, no lost writes)", totalBuckets, keyCount)
	}
}

// BenchmarkRateLimiterAllow measures allocation overhead of the steady-state
// path (bucket already exists), which is what every request after a key's
// first hits. Run with -benchmem: the token-bucket check itself should not
// allocate, since sync.Map.Load and the mutex-guarded arithmetic in
// tokenBucket.allow don't touch the heap on a hit.
func BenchmarkRateLimiterAllow(b *testing.B) {
	limiter, err := newRateLimiter(RateLimitConfig{RequestsPerSecond: 1e9, Burst: 1e9})
	if err != nil {
		b.Fatalf("newRateLimiter() error = %v", err)
	}

	const keyCount = 64
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = "bench-key-" + strconv.Itoa(i)
		limiter.allow(keys[i]) // warm each bucket before measuring
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.allow(keys[i%keyCount])
	}
}
