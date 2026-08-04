package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RateLimitConfig configures a token-bucket rate limiter applied per API
// key: RequestsPerSecond is the steady-state refill rate, Burst is the
// maximum number of requests a single key may make instantaneously before
// it starts being throttled.
type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
}

// tokenBucket is a single API key's token-bucket state. Tokens refill
// continuously at a fixed rate up to burst capacity; each allowed request
// consumes one token.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

// allow reports whether a request arriving at now may proceed, refilling
// tokens for the elapsed time since the last call before checking.
func (bucket *tokenBucket) allow(rate, burst float64, now time.Time) bool {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.lastRefill = now
	bucket.tokens = min(burst, bucket.tokens+elapsed*rate)

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// rateLimiterShardCount is the number of independently-locked shards a
// rateLimiter's bucket map is split across. It must be a power of two so
// shard selection can mask fnv32a's hash instead of using a modulo.
const rateLimiterShardCount = 64

// rateLimiterShard holds one slice of a rateLimiter's per-key buckets,
// guarded by its own lock so unrelated keys never contend with each other.
type rateLimiterShard struct {
	mu      sync.RWMutex
	buckets map[string]*tokenBucket
}

// rateLimiter enforces a per-API-key token-bucket rate limit across both
// the REST and gRPC ingestion transports. Buckets are sharded by key hash
// so concurrent requests for different keys don't serialize on a single
// lock, and the read path only takes a shard's read lock, not a write lock,
// once a key's bucket exists.
type rateLimiter struct {
	rate   float64
	burst  float64
	shards [rateLimiterShardCount]rateLimiterShard
}

// newRateLimiter builds a rateLimiter from cfg. Both RequestsPerSecond and
// Burst must be positive, since a zero or negative limit would reject every
// request and that's almost certainly a configuration mistake rather than
// an intentional deployment.
func newRateLimiter(cfg RateLimitConfig) (*rateLimiter, error) {
	if cfg.RequestsPerSecond <= 0 {
		return nil, fmt.Errorf("rate limit requests per second must be positive, got %v", cfg.RequestsPerSecond)
	}
	if cfg.Burst <= 0 {
		return nil, fmt.Errorf("rate limit burst must be positive, got %v", cfg.Burst)
	}

	limiter := &rateLimiter{rate: cfg.RequestsPerSecond, burst: float64(cfg.Burst)}
	for i := range limiter.shards {
		limiter.shards[i].buckets = make(map[string]*tokenBucket)
	}
	return limiter, nil
}

// allow reports whether a request authenticated with key may proceed,
// consuming a token from key's bucket if so. Buckets are created lazily and
// start full so a key's first burst worth of requests isn't throttled. The
// steady-state path only takes the owning shard's read lock, so concurrent
// requests for existing keys don't allocate or block each other; a bucket
// is only ever constructed once per key, under the shard's write lock with
// a double-checked lookup, so a race between two first requests for the
// same key doesn't waste an allocation on the loser.
func (limiter *rateLimiter) allow(key string) bool {
	now := time.Now()
	shard := &limiter.shards[fnv32a(key)&(rateLimiterShardCount-1)]

	shard.mu.RLock()
	bucket, ok := shard.buckets[key]
	shard.mu.RUnlock()
	if ok {
		return bucket.allow(limiter.rate, limiter.burst, now)
	}

	shard.mu.Lock()
	if bucket, ok = shard.buckets[key]; ok {
		shard.mu.Unlock()
		return bucket.allow(limiter.rate, limiter.burst, now)
	}
	// The bucket starts with its first token already spent, since this
	// call itself counts as the request being allowed.
	bucket = &tokenBucket{tokens: limiter.burst - 1, lastRefill: now}
	shard.buckets[key] = bucket
	shard.mu.Unlock()
	return true
}

// fnv32a hashes key for shard selection using the FNV-1a algorithm,
// implemented directly rather than via hash/fnv to avoid that package's
// heap-allocated hash.Hash32 on every call.
func fnv32a(key string) uint32 {
	const (
		offsetBasis32 = 2166136261
		prime32       = 16777619
	)

	hash := uint32(offsetBasis32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

// middleware returns an http.Handler that rejects requests over the rate
// limit for the caller's API key with 429, before next is invoked. It must
// run after apiKeyAuthenticator.middleware, so only already-authenticated
// requests reach it; it derives the same key straight from the
// Authorization header rather than depending on auth to pass it along.
func (limiter *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		key, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok || !limiter.allow(key) {
			writeJSONError(responseWriter, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		next.ServeHTTP(responseWriter, request)
	})
}

// unaryInterceptor returns a grpc.UnaryServerInterceptor that rejects
// requests over the rate limit for the caller's API key with
// codes.ResourceExhausted, before req reaches handler. It must run after
// apiKeyAuthenticator.unaryInterceptor, so only already-authenticated
// requests reach it; it derives the same key straight from ctx's incoming
// metadata rather than depending on auth to pass it along. Health checking
// requests are exempt, matching the auth interceptor.
func (limiter *rateLimiter) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, healthCheckServicePrefix) {
			return handler(ctx, req)
		}

		key, ok := bearerTokenFromContext(ctx)
		if !ok || !limiter.allow(key) {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}
