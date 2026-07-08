# Container deployment

logdbd ships as a multi-stage Docker image and a Helm chart for Kubernetes.

## Quick start (local, dev)

```bash
# Build the image and run a single dev node (plaintext, no auth — dev only)
docker compose -f deploy/docker/docker-compose.yml up --build
```

logdbd listens on `127.0.0.1:50051`. Connect a client (Rust or TS SDK) — no
auth token needed in dev mode.

The dev compose sets `LOGDBD_ALLOW_INSECURE=1` so the server will bind
`0.0.0.0` without TLS+auth. **Never set this in production.**

## Building the image

```bash
docker build -t logdbd .
```

The image contains two binaries:

- `logdbd` — the server (entrypoint).
- `logdbd-admin` — the management CLI (`status`, `checkpoint`, `backup`,
  `restore`, …), also used for container health probes.

It runs as a non-root user (UID/GID `65532`) and persists data under
`/var/lib/logdbd` (a `VOLUME`).

## Production checklist

A non-loopback bind without TLS+auth is **refused** at startup (unless
`LOGDBD_ALLOW_INSECURE=1`). For production:

1. **TLS** — provide `server.tls` (`tls` or `mtls`) and mount certs. With
   Helm, set `server.tls.secretName` to a Secret with `tls.crt` / `tls.key`
   (and `ca.crt` for mTLS).
2. **Auth** — set `server.auth` to a token file. With Helm, either reference
   an existing Secret (`server.auth.secretName`, key `token`) or set
   `server.auth.token` (the chart generates a Secret — prefer the former).
3. **Persistence** — a PVC for `/var/lib/logdbd` (Helm: `persistence.enabled`).
4. **Replication** — standbys for durability (Helm: `standby.replicaCount`;
   horizontal write scaling is cr-026, not yet shipped).

## Kubernetes (Helm)

```bash
# TLS + auth from pre-provisioned secrets, 10Gi PVC
helm install logdbd deploy/helm/logdbd \
    --set image.repository=ghcr.io/lv-agent/logdbd \
    --set server.tls.mode=tls \
    --set server.tls.secretName=logdbd-tls \
    --set server.auth.secretName=logdbd-token \
    --set persistence.size=50Gi
```

See `deploy/helm/logdbd/values.yaml` for the full set of knobs (resources,
nodeSelector, affinity, tolerations, durability mode, compression, hash
algorithm, …).

## Backup & restore (cr-029)

Run the file-level backup tool against a **stopped** node's data dir (it holds
the primary `active.lock` and refuses if the server is running):

```bash
# In a pod (scale the primary to 0 first, or exec into a stopped pod):
logdbd-admin backup  --data-dir /var/lib/logdbd --out /backups/snap.logdbbak
logdbd-admin restore --backup  /backups/snap.logdbbak --data-dir /var/lib/logdbd --verify
```

The `.logdbbak` archive + its `.sha256` sidecar are portable across hosts.
`--verify` re-opens the logdb so recovery re-checks CRC + the BLAKE3 hash chain
+ torn-write truncation.
