# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.26.7-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /response-keeper .
# Image-owned STORE_DIR so a fresh named volume is initialized writable for 65534.
RUN mkdir -p /out/var/tmp/response-keeper && chmod 700 /out/var/tmp/response-keeper

FROM scratch
COPY --from=builder /response-keeper /response-keeper
COPY --from=builder --chown=65534:65534 /out/var/tmp/response-keeper /var/tmp/response-keeper
USER 65534:65534
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["/response-keeper", "healthcheck"]
ENTRYPOINT ["/response-keeper"]
