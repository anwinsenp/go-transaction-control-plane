# Build context is the repo root (module github.com/anwinsenp/go-transaction-control-plane):
#   docker build -f cmd/ingestion/Dockerfile -t ingestion:local .
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ingestion ./cmd/ingestion

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/ingestion /usr/local/bin/ingestion
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/ingestion"]
