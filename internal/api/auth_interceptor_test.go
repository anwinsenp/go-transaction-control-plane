package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		wantToken string
		wantOK    bool
	}{
		{name: "valid bearer token", header: "Bearer valid-key", wantToken: "valid-key", wantOK: true},
		{name: "empty header", header: "", wantOK: false},
		{name: "missing bearer scheme", header: "valid-key", wantOK: false},
		{name: "lowercase bearer scheme is rejected", header: "bearer valid-key", wantOK: false},
		{name: "extra space after scheme is kept as part of the token", header: "Bearer  valid-key", wantToken: " valid-key", wantOK: true},
		{name: "empty token after scheme", header: "Bearer ", wantToken: "", wantOK: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token, ok := bearerToken(test.header)
			if ok != test.wantOK {
				t.Fatalf("bearerToken(%q) ok = %v, want %v", test.header, ok, test.wantOK)
			}
			if ok && token != test.wantToken {
				t.Errorf("bearerToken(%q) token = %q, want %q", test.header, token, test.wantToken)
			}
		})
	}
}

func TestBearerTokenFromContext(t *testing.T) {
	tests := []struct {
		name      string
		ctx       context.Context
		wantToken string
		wantOK    bool
	}{
		{
			name:      "valid bearer token in metadata",
			ctx:       metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-key")),
			wantToken: "valid-key",
			wantOK:    true,
		},
		{
			name:   "no incoming metadata",
			ctx:    context.Background(),
			wantOK: false,
		},
		{
			name:   "metadata present but no authorization key",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-other", "value")),
			wantOK: false,
		},
		{
			name:   "authorization value missing bearer scheme",
			ctx:    metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "valid-key")),
			wantOK: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token, ok := bearerTokenFromContext(test.ctx)
			if ok != test.wantOK {
				t.Fatalf("bearerTokenFromContext() ok = %v, want %v", ok, test.wantOK)
			}
			if ok && token != test.wantToken {
				t.Errorf("bearerTokenFromContext() token = %q, want %q", token, test.wantToken)
			}
		})
	}
}

func TestNewAPIKeyAuthenticatorRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{name: "no keys", keys: nil},
		{name: "empty key", keys: []string{"valid-key", ""}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newAPIKeyAuthenticator(test.keys); err == nil {
				t.Fatalf("newAPIKeyAuthenticator(%v) error = nil, want error", test.keys)
			}
		})
	}
}

// handlerCalled is a grpc.UnaryHandler that records whether it ran, so tests
// can assert the interceptor stops unauthenticated requests before they
// reach the handler.
func handlerCalled(called *bool) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		*called = true
		return "ok", nil
	}
}

func TestAPIKeyAuthenticatorUnaryInterceptor(t *testing.T) {
	authenticator, err := newAPIKeyAuthenticator([]string{"valid-key"})
	if err != nil {
		t.Fatalf("newAPIKeyAuthenticator() error = %v", err)
	}
	interceptor := authenticator.unaryInterceptor()

	tests := []struct {
		name        string
		ctx         context.Context
		wantHandled bool
		wantCode    codes.Code
	}{
		{
			name:        "valid bearer token",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-key")),
			wantHandled: true,
		},
		{
			name:        "missing metadata",
			ctx:         context.Background(),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "missing authorization header",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-other", "value")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "not a bearer token",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "valid-key")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "wrong key",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-key")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "lowercase bearer scheme is rejected",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bearer valid-key")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "extra space after bearer scheme is rejected",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer  valid-key")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
		{
			name:        "trailing whitespace on token is rejected",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-key ")),
			wantHandled: false,
			wantCode:    codes.Unauthenticated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var handled bool
			info := &grpc.UnaryServerInfo{FullMethod: "/ingestion.v1.TransactionIngestionService/IngestTransaction"}

			_, err := interceptor(test.ctx, "req", info, handlerCalled(&handled))

			if handled != test.wantHandled {
				t.Errorf("handler called = %v, want %v", handled, test.wantHandled)
			}

			if test.wantHandled {
				if err != nil {
					t.Errorf("interceptor error = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatal("interceptor error = nil, want Unauthenticated")
			}
			if code := status.Code(err); code != test.wantCode {
				t.Errorf("interceptor error code = %v, want %v", code, test.wantCode)
			}
		})
	}
}

func TestAPIKeyAuthenticatorUnaryInterceptorAcceptsAnyConfiguredKey(t *testing.T) {
	authenticator, err := newAPIKeyAuthenticator([]string{"key-one", "key-two"})
	if err != nil {
		t.Fatalf("newAPIKeyAuthenticator() error = %v", err)
	}
	interceptor := authenticator.unaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/ingestion.v1.TransactionIngestionService/IngestTransaction"}

	tests := []struct {
		name  string
		token string
	}{
		{name: "first configured key", token: "key-one"},
		{name: "second configured key", token: "key-two"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var handled bool
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+test.token))

			if _, err := interceptor(ctx, "req", info, handlerCalled(&handled)); err != nil {
				t.Errorf("interceptor error = %v, want nil", err)
			}
			if !handled {
				t.Errorf("handler called = false, want true for token %q", test.token)
			}
		})
	}
}

func TestAPIKeyAuthenticatorUnaryInterceptorExemptsHealthCheck(t *testing.T) {
	authenticator, err := newAPIKeyAuthenticator([]string{"valid-key"})
	if err != nil {
		t.Fatalf("newAPIKeyAuthenticator() error = %v", err)
	}
	interceptor := authenticator.unaryInterceptor()

	var handled bool
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}

	if _, err := interceptor(context.Background(), "req", info, handlerCalled(&handled)); err != nil {
		t.Errorf("interceptor error = %v, want nil", err)
	}
	if !handled {
		t.Error("handler called = false, want true (health check should bypass auth)")
	}
}

// recordingHandler is an http.Handler that records whether it ran, so tests
// can assert middleware stops unauthenticated requests before they reach
// the wrapped handler.
func recordingHandler(called *bool) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		*called = true
		responseWriter.WriteHeader(http.StatusOK)
	})
}

func TestAPIKeyAuthenticatorMiddleware(t *testing.T) {
	authenticator, err := newAPIKeyAuthenticator([]string{"valid-key"})
	if err != nil {
		t.Fatalf("newAPIKeyAuthenticator() error = %v", err)
	}

	tests := []struct {
		name        string
		header      string
		wantHandled bool
	}{
		{
			name:        "valid bearer token",
			header:      "Bearer valid-key",
			wantHandled: true,
		},
		{
			name:        "missing authorization header",
			header:      "",
			wantHandled: false,
		},
		{
			name:        "not a bearer token",
			header:      "valid-key",
			wantHandled: false,
		},
		{
			name:        "wrong key",
			header:      "Bearer wrong-key",
			wantHandled: false,
		},
		{
			name:        "lowercase bearer scheme is rejected",
			header:      "bearer valid-key",
			wantHandled: false,
		},
		{
			name:        "extra space after bearer scheme is rejected",
			header:      "Bearer  valid-key",
			wantHandled: false,
		},
		{
			name:        "trailing whitespace on token is rejected",
			header:      "Bearer valid-key ",
			wantHandled: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var handled bool
			handler := authenticator.middleware(recordingHandler(&handled))

			request := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
			if test.header != "" {
				request.Header.Set("Authorization", test.header)
			}
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if handled != test.wantHandled {
				t.Errorf("handler called = %v, want %v", handled, test.wantHandled)
			}

			if test.wantHandled {
				if recorder.Code != http.StatusOK {
					t.Errorf("status = %d, want %d", recorder.Code, http.StatusOK)
				}
				return
			}

			if recorder.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
			}

			var response errorResponse
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Error == "" {
				t.Error("error message is empty, want a clear description")
			}
		})
	}
}

func TestAPIKeyAuthenticatorMiddlewareAcceptsAnyConfiguredKey(t *testing.T) {
	authenticator, err := newAPIKeyAuthenticator([]string{"key-one", "key-two"})
	if err != nil {
		t.Fatalf("newAPIKeyAuthenticator() error = %v", err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{name: "first configured key", token: "key-one"},
		{name: "second configured key", token: "key-two"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var handled bool
			handler := authenticator.middleware(recordingHandler(&handled))

			request := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
			request.Header.Set("Authorization", "Bearer "+test.token)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if !handled {
				t.Errorf("handler called = false, want true for token %q", test.token)
			}
			if recorder.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", recorder.Code, http.StatusOK)
			}
		})
	}
}
