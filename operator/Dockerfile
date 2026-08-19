# Build context is /operator (module github.com/anwinsenp/go-transaction-control-plane/operator):
#   docker build -t operator:local operator
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/operator ./cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/operator /usr/local/bin/operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/operator"]
