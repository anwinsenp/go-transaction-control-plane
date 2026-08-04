package api

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// healthCheckServicePrefix is the gRPC method prefix for the standard health
// checking protocol, which must stay reachable without credentials so
// Kubernetes liveness/readiness probes don't need an API key.
const healthCheckServicePrefix = "/grpc.health.v1.Health/"

// bearerToken extracts the token from a "Bearer <token>" Authorization
// header value. It's shared by the REST authenticator and rate limiter so
// both derive the same request key from the header without duplicating the
// parse, and doesn't allocate: CutPrefix returns a re-slice of header, not
// a copy.
func bearerToken(header string) (string, bool) {
	return strings.CutPrefix(header, "Bearer ")
}

// bearerTokenFromContext extracts the token from ctx's incoming gRPC
// "authorization" metadata. It's shared by the gRPC authenticator and rate
// limiter interceptors so both derive the same request key independently,
// straight from the metadata gRPC-Go already attaches to ctx.
func bearerTokenFromContext(ctx context.Context) (string, bool) {
	metaData, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	values := metaData.Get("authorization")
	if len(values) == 0 {
		return "", false
	}

	// gRPC-Go delivers a single-value metadata header as one entry, so only
	// the first value needs checking.
	return bearerToken(values[0])
}

// apiKeyAuthenticator validates incoming gRPC requests against a fixed set
// of accepted API keys, presented as a bearer token.
type apiKeyAuthenticator struct {
	validKeys []string
}

// newAPIKeyAuthenticator builds an apiKeyAuthenticator that accepts any of
// keys. It returns an error if keys is empty, since a server that accepts no
// keys would reject every request and that's almost certainly a
// configuration mistake rather than an intentional deployment.
func newAPIKeyAuthenticator(keys []string) (*apiKeyAuthenticator, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("at least one API key is required")
	}

	validKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("API key must not be empty")
		}
		validKeys = append(validKeys, key)
	}

	return &apiKeyAuthenticator{validKeys: validKeys}, nil
}

// unaryInterceptor returns a grpc.UnaryServerInterceptor that rejects
// requests without a valid bearer token, before req reaches handler. Health
// checking requests are exempt so orchestrator probes don't need a key.
func (auth *apiKeyAuthenticator) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, healthCheckServicePrefix) {
			return handler(ctx, req)
		}

		if !auth.authenticate(ctx) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid bearer token")
		}

		return handler(ctx, req)
	}
}

// authenticate reports whether ctx carries a valid bearer token in its
// "authorization" metadata.
func (auth *apiKeyAuthenticator) authenticate(ctx context.Context) bool {
	token, ok := bearerTokenFromContext(ctx)
	if !ok {
		return false
	}
	return auth.validate(token)
}

// middleware wraps next with a bearer-token check equivalent to
// unaryInterceptor's, for the REST transport: it requires the same
// "Authorization: Bearer <key>" header and rejects with 401 instead of
// codes.Unauthenticated.
func (auth *apiKeyAuthenticator) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		token, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok || !auth.validate(token) {
			writeJSONError(responseWriter, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}

		next.ServeHTTP(responseWriter, request)
	})
}

// validate reports whether token matches one of auth's configured keys. It
// runs a fixed-time comparison against every key rather than returning as
// soon as one matches, so total comparison time doesn't depend on which key
// matched or whether any did.
func (auth *apiKeyAuthenticator) validate(token string) bool {
	var match int
	for _, key := range auth.validKeys {
		match |= subtle.ConstantTimeCompare([]byte(token), []byte(key))
	}
	return match == 1
}
