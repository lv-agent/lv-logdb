# Upgrading to logdbd v0.4

## From v0.2

v0.4 is a major release with breaking proto and storage changes. There is **no
automatic migration** from v0.2 data directories — the data model has changed
fundamentally.

### Migration Strategy: Start Fresh

1. Deploy a v0.4 **standby** node alongside your existing v0.2 primary.
2. Configure the v0.4 standby to receive **snapshot** from the v0.2 primary:
   - v0.4 has a `PullSnapshot` RPC, but v0.2 doesn't serve it.
   - **Workaround**: use v0.4 exporter pointing at v0.2 to export all data,
     then import into v0.4 via `Append`.
3. Once v0.4 has caught up, switch traffic to v0.4 primary.
4. Decommission v0.2 nodes.

### Proto Changes

All RPCs now require `namespace` and `stream`:

```protobuf
// v0.2
message AppendRequest { bytes content = 1; }
message ReadRequest   { uint64 sequence = 1; }

// v0.4
message AppendRequest {
  string namespace = 1;    // NEW, required
  string stream    = 2;    // NEW, required
  string event_type = 3;   // NEW, required
  bytes  content   = 7;
}
message ReadRequest {
  string namespace = 1;    // NEW
  string stream    = 2;    // NEW
  uint64 seq       = 3;    // was "sequence"
}
```

Client code must be updated to include `namespace` and `stream` on every request.

### Config Changes

```bash
# v0.2: environment variables
LOGDBD_LISTEN=0.0.0.0:50051
LOGDBD_DATA_DIR=/var/lib/logdbd
LOGDBD_ROLE=primary

# v0.4: YAML config file
# logdbd --config /etc/logdbd/logdbd.yaml
node:
  id: "primary-1"
  role: primary
  cluster_id: "my-cluster"
  epoch: 1
server:
  bind: "0.0.0.0:50051"
logdb:
  data_dir: /var/lib/logdbd
```

Legacy `LOGDBD_*` env vars still work as overrides but are deprecated.

### Storage Changes

v0.4 stores records with a new binary format including namespace_id, stream_id,
and per-stream seq. Old v0.2 data directories are incompatible and cannot be
mounted as a v0.4 data directory.

### Replication Changes

v0.4 replication uses `ReplicationRecord` (raw bytes) instead of the public
`Record` proto type. Mixing v0.2 and v0.4 in a replication group will cause
errors. All nodes in a cluster must be on the same version.

### Catalog

v0.4 introduces a `catalog.dat` file in the data directory. This file is
managed automatically. Do not modify it. It survives process restarts and
persists namespace/stream name→ID mappings.

### Recommended Deployment Order

```
1. Deploy v0.4 standby (fresh data dir)
2. Export v0.2 data → import to v0.4 via Append
3. Verify v0.4 has all records
4. Deploy additional v0.4 standbys
5. Switch Agent traffic to v0.4 primary
6. Decommission v0.2
```

## From v0.4.x to v0.4.y (patch)

Patch versions within v0.4 are backward-compatible:
- Catalog format is versioned (magic + version fields), auto-upgraded on load.
- Proto is wire-compatible within v0.4.
- Data directories are directly mountable.
- Rolling restart is safe: stop one node, upgrade binary, start; repeat.

## v0.5+ Planning

Expected breaking changes in v0.5:
- Shard-level stream assignment (currently all streams share shard 0).
- Sparse index for per-stream point reads (currently O(n) scan).
- Per-namespace retention configuration.
- External anchor for hash chain (blockchain/timestamp service).

No data migration will be provided from v0.4 to v0.5 — plan for fresh deployment
or export/import.
