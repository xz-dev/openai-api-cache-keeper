# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.26.7-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /openai-api-cache-keeper .
# Image-owned STORE_DIR so a fresh named volume is initialized writable for 65534.
RUN mkdir -p /out/var/tmp/openai-api-cache-keeper && chmod 700 /out/var/tmp/openai-api-cache-keeper

FROM scratch
COPY --from=builder /openai-api-cache-keeper /openai-api-cache-keeper
COPY --from=builder --chown=65534:65534 /out/var/tmp/openai-api-cache-keeper /var/tmp/openai-api-cache-keeper
USER 65534:65534
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["/openai-api-cache-keeper", "healthcheck"]
ENTRYPOINT ["/openai-api-cache-keeper"]
