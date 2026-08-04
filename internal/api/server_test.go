package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anwinsenp/go-transaction-control-plane/internal/ingestion"
)

// testHTTPAPIKey is the bearer token every test Server accepts.
const testHTTPAPIKey = "test-api-key"

// newTestServer builds a Server configured with testHTTPAPIKey, failing the
// test if construction fails.
func newTestServer(t *testing.T, addr string, publisher ingestion.Publisher) *Server {
	t.Helper()
	server, err := NewServer(addr, publisher, []string{testHTTPAPIKey})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

// setAuthHeader sets request's Authorization header to testHTTPAPIKey as a
// bearer token, for requests against routes that require authentication.
func setAuthHeader(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+testHTTPAPIKey)
}

func TestNewServerRejectsInvalidAPIKeys(t *testing.T) {
	tests := []struct {
		name    string
		apiKeys []string
	}{
		{name: "no keys", apiKeys: nil},
		{name: "empty key", apiKeys: []string{"valid-key", ""}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server, err := NewServer("127.0.0.1:0", &fakePublisher{}, testCase.apiKeys)
			if err == nil {
				t.Fatalf("NewServer(%v) error = nil, want error", testCase.apiKeys)
			}
			if server != nil {
				t.Errorf("NewServer(%v) server = %v, want nil", testCase.apiKeys, server)
			}
			if !strings.Contains(err.Error(), "configure API key authenticator") {
				t.Errorf("NewServer(%v) error = %v, want it to wrap %q", testCase.apiKeys, err, "configure API key authenticator")
			}
		})
	}
}

func TestServerRoutes(t *testing.T) {
	server := newTestServer(t, "127.0.0.1:0", &fakePublisher{})
	testServer := httptest.NewServer(server.httpServer.Handler)
	defer testServer.Close()

	validBody := mustMarshal(t, TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	})

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "GET healthz returns 200",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST transactions with valid body returns 202",
			method:     http.MethodPost,
			path:       "/v1/transactions",
			body:       validBody,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "GET transactions returns 405",
			method:     http.MethodGet,
			path:       "/v1/transactions",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "unregistered route returns 404",
			method:     http.MethodGet,
			path:       "/unknown",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := http.NewRequest(testCase.method, testServer.URL+testCase.path, strings.NewReader(testCase.body))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			setAuthHeader(request)

			response, err := testServer.Client().Do(request)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer response.Body.Close()

			if response.StatusCode != testCase.wantStatus {
				t.Errorf("status = %d, want %d", response.StatusCode, testCase.wantStatus)
			}
		})
	}
}

// TestServerRoutePublishFailureReturns503 exercises a publish failure
// through the actual mux-registered route (rather than calling the handler
// function directly, as transaction_handler_test.go does), so a mismatch
// between server.go's route registration and the handler's error behavior
// would be caught here too.
func TestServerRoutePublishFailureReturns503(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka broker unreachable")}
	server := newTestServer(t, "127.0.0.1:0", publisher)
	testServer := httptest.NewServer(server.httpServer.Handler)
	defer testServer.Close()

	validBody := mustMarshal(t, TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	})

	request, err := http.NewRequest(http.MethodPost, testServer.URL+"/v1/transactions", strings.NewReader(validBody))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	setAuthHeader(request)

	response, err := testServer.Client().Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestServerRejectsTransactionsWithoutValidAPIKey(t *testing.T) {
	server := newTestServer(t, "127.0.0.1:0", &fakePublisher{})
	testServer := httptest.NewServer(server.httpServer.Handler)
	defer testServer.Close()

	validBody := mustMarshal(t, TransactionEventRequest{
		EventID:    "11111111-1111-1111-1111-111111111111",
		TenantID:   "tenant-a",
		Instrument: "AAPL",
		Side:       "BUY",
		Quantity:   "10",
		Price:      "150.25",
		Currency:   "USD",
		OccurredAt: "2026-08-03T12:00:00Z",
	})

	tests := []struct {
		name   string
		header string
	}{
		{name: "no credentials"},
		{name: "wrong key", header: "Bearer wrong-key"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, testServer.URL+"/v1/transactions", strings.NewReader(validBody))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			if testCase.header != "" {
				request.Header.Set("Authorization", testCase.header)
			}

			response, err := testServer.Client().Do(request)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer response.Body.Close()

			if response.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", response.StatusCode, http.StatusUnauthorized)
			}

			var errResponse errorResponse
			if err := json.NewDecoder(response.Body).Decode(&errResponse); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if errResponse.Error == "" {
				t.Error("error message is empty, want a clear description")
			}
		})
	}
}

// TestServerHealthzDoesNotRequireAuth confirms /healthz stays reachable
// without any Authorization header, mirroring the gRPC side's health-check
// exemption. Unlike TestServerRoutes's healthz case, which always sends an
// auth header via its shared per-case loop, this test omits it entirely.
func TestServerHealthzDoesNotRequireAuth(t *testing.T) {
	server := newTestServer(t, "127.0.0.1:0", &fakePublisher{})
	testServer := httptest.NewServer(server.httpServer.Handler)
	defer testServer.Close()

	request, err := http.NewRequest(http.MethodGet, testServer.URL+"/healthz", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	response, err := testServer.Client().Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestServerStartAndShutdown(t *testing.T) {
	server := newTestServer(t, "127.0.0.1:0", &fakePublisher{})

	startErrors := make(chan error, 1)
	go func() {
		startErrors <- server.Start()
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	select {
	case err := <-startErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Start() error = %v, want errors.Is(err, http.ErrServerClosed)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not return after Shutdown()")
	}
}
