# syntax=docker/dockerfile:1
#
# logdbd container image — multi-stage build.
#
#   docker build -t logdbd .
#   docker run -p 50051:50051 -e LOGDBD_ALLOW_INSECURE=1 -v logdbd-data:/var/lib/logdbd logdbd
#
# The default config binds 0.0.0.0:50051 with TLS+auth disabled. Starting it
# on a non-loopback address without TLS+auth is refused unless
# LOGDBD_ALLOW_INSECURE=1 (dev only). For production, mount a real config +
# TLS certs + auth token — see deploy/helm/logdbd/ and deploy/docker/README.md.

ARG RUST_VERSION=1.85
ARG DEBIAN_VERSION=bookworm

# ── builder ──────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

# prost-build (logdbd-proto) invokes protoc at build time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
# Build the logdbd package: produces both `logdbd` (server) and
# `logdbd-admin` (management CLI). Release profile for a small, fast binary.
RUN cargo build --release -p logdbd

# ── runtime ──────────────────────────────────────────────────────────────
FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    # Fixed UID/GID so k8s securityContext (runAsUser/fsGroup) and PVC
    # ownership line up regardless of the host.
    && groupadd --system --gid 65532 logdbd \
    && useradd --system --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin logdbd

COPY --from=builder /build/target/release/logdbd       /usr/local/bin/logdbd
COPY --from=builder /build/target/release/logdbd-admin /usr/local/bin/logdbd-admin

# Container default config (binds 0.0.0.0). Override by mounting a config
# at /etc/logdbd/logdbd.yaml.
COPY deploy/docker/logdbd.yaml /etc/logdbd/logdbd.yaml

RUN mkdir -p /var/lib/logdbd \
    && chown -R 65532:65532 /var/lib/logdbd /etc/logdbd

USER logdbd
VOLUME ["/var/lib/logdbd"]
EXPOSE 50051

# Optional gRPC health probe; logdbd serves tonic_health on the main port.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD logdbd-admin ping 127.0.0.1:50051 || exit 1

ENTRYPOINT ["logdbd"]
CMD ["--config", "/etc/logdbd/logdbd.yaml"]
