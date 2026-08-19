# Build context is the repo root (module github.com/anwinsenp/go-transaction-control-plane):
#   docker build -f cmd/processor/Dockerfile -t processor:local .
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/processor ./cmd/processor

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/processor /usr/local/bin/processor
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/processor"]
