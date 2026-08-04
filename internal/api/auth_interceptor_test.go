package api

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
