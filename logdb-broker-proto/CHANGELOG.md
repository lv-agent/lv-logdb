# Changelog — logdb-broker-proto

## [0.1.0] — 2026-07-09

### Added

- **`broker.proto`** — standalone protobuf definitions for the logdb-broker
  gRPC service (`BrokerService`). Does not import `logdbd.proto`; the broker
  defines its own forwarded `Record` (field-compatible, decoupled from logdbd's
  internal format).
- **Membership RPCs**: `JoinGroup`, `LeaveGroup`, `ListMembers` (Phase 2).
- **Data-path RPCs**: `Consume` (server-streaming, Phase 3) and `Produce`
  (Phase 3.A, symmetric gateway).
- **Offset RPC**: `CommitShardOffset` (Phase 6).
- **Rebalance frames**: `RebalanceSignal` / `Assignment` in `ConsumeResponse`
  oneof (Phase 5).
- `Record.shard_id` stamp (Phase 6) — filled by the broker's per-shard Tail.
