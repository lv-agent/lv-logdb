# syntax=docker/dockerfile:1
#
# logdb-broker container image — multi-stage build.
#
#   docker build -t logdb-broker -f logdb-broker/Dockerfile .
#   docker run -p 9091:9091 -p 9100:9100 logdb-broker
#
# The broker is stateless (offsets are event-sourced into the logdbd meta
# stream), so it needs NO local volume — point it at a reachable logdbd via
# `logdbd_addr` (override the mounted config). Pair with the logdbd image.

ARG RUST_VERSION=1.85
ARG DEBIAN_VERSION=bookworm

# ── builder ──────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

# prost-build (logdb-broker-proto, logdbd-proto) invokes protoc at build time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release -p logdb-broker

# ── runtime ──────────────────────────────────────────────────────────────
FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    # Fixed UID/GID so k8s securityContext lines up regardless of host.
    && groupadd --system --gid 65532 logdbbroker \
    && useradd --system --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin logdbbroker

COPY --from=builder /build/target/release/logdb-broker /usr/local/bin/logdb-broker

# Default config (binds 0.0.0.0). Override by mounting a config here.
COPY logdb-broker/config.yaml /etc/logdb-broker/config.yaml

RUN mkdir -p /etc/logdb-broker \
    && chown -R 65532:65532 /etc/logdb-broker

USER logdbbroker

# 9091 = broker gRPC (producers + consumers); 9100 = Prometheus /metrics.
EXPOSE 9091 9100

ENV LOGDB_BROKER_CONFIG=/etc/logdb-broker/config.yaml \
    RUST_LOG=info

ENTRYPOINT ["logdb-broker"]
