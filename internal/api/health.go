package api

import "net/http"

// healthHandler reports that the ingestion service is up and able to serve
// requests.
func healthHandler(rsp http.ResponseWriter, req *http.Request) {
	rsp.WriteHeader(http.StatusOK)
}
