package api

import "net/http"

// healthHandler reports that the ingestion service is up and able to serve
// requests.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
