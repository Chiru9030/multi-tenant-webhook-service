FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /relay-server ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget \
    && adduser -D -u 10001 appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app
WORKDIR /app
COPY --from=builder /relay-server /app/relay-server
ENV PORT=8080 DB_PATH=/app/data/webhooks.db
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD /bin/sh -c 'wget -qO- "http://127.0.0.1:${PORT:-8080}/healthz" || exit 1'
ENTRYPOINT ["/app/relay-server"]
CMD ["-workers=15"]
