# Features

logdb ships four optional Cargo features — all **off by default**. Each feature pulls in optional dependencies and unlocks a corresponding field on [`Config`](configuration.md#field-reference). This page is the complete feature matrix, what each feature does, and the operational consequences of turning it on.

## Contents

- [Feature matrix](#feature-matrix)
- [Enabling features](#enabling-features)
- [hash-chain](#hash-chain)
- [compression](#compression)
- [encryption](#encryption)
- [remote-push](#remote-push)
- [See also](#see-also)

## Feature matrix

The feature table (`Cargo.toml`, `default = []`):

| Feature | Optional dependencies | Enables | Notes |
|---------|----------------------|---------|-------|
| `hash-chain` | `sha2`, `blake3` | `Config.hash_enabled` | BLAKE3 keyed hash chain for tamper-evidence. **Single-shard only** (`shards == 1`); rejected at `open` otherwise. |
| `compression` | `zstd` (no default features) | `Config.compression_enabled` | Streaming, per-frame zstd compression. Transparent on read. |
| `encryption` | `aes-gcm` (with `aes`, `alloc`), `getrandom` | `Config.encryption_key: Option<[u8;32]>` | AES-256-GCM per frame with a random nonce. **Key loss is unrecoverable.** |
| `remote-push` | — (flag only) | Standby write-in via `LogDb::replicate` | Flag-gated module; see [remote-push](#remote-push). |

All features are independent and can be combined, except that `hash-chain` implies `shards == 1` (see [hash-chain](#hash-chain)).

## Enabling features

Enable features in your `Cargo.toml`:

```toml
[dependencies]
logdb = { version = "0.2.0", features = ["hash-chain", "compression"] }

# Or all of them:
# logdb = { version = "0.2.0", features = ["hash-chain", "compression", "encryption", "remote-push"] }
```

Then turn the corresponding field on in `Config`:

```rust
use logdb::Config;

let config = Config {
    hash_enabled: true,           // requires feature "hash-chain"
    compression_enabled: true,    // requires feature "compression"
    encryption_key: Some(/* [u8; 32] */), // requires feature "encryption"
    ..Config::default()
};
let db = logdb::LogDb::open(config)?;
```

A `Config` field set without its feature gate fails at **compile time**, not at runtime — `validate()` does not check feature gates. The four knobs above are the only feature-gated `Config` fields.

## hash-chain

`hash-chain` (`Config.hash_enabled`) appends a tamper-evident hash chain over the log so that any after-the-fact mutation of a sealed segment is detectable on read. It uses BLAKE3 in **keyed mode**: the chain is seeded with a secret `hash_init` and each record's hash chains the previous hash with the record body, so a modified byte anywhere in the chain breaks verification of every subsequent record.

**`hash_init` is generated from entropy and never written to disk.** It is produced by `generate_hash_init` (`src/lib.rs:685-699`) at `open` time when a fresh database is created, and held only in memory for the Sealer thread. Consequences:

- The chain is **verifiable on read** without the key — readers recompute the chain to detect tampering.
- The key is **not a recoverable secret**. Losing the in-memory `hash_init` across a process restart does not break verification (the key is only used to initialize the first chain link); the chain remains self-checking as long as the on-disk hashes are intact. The point of keeping it off disk is that an attacker who reads the file cannot forge a consistent chain without also re-running BLAKE3 keyed with the original key.
- The hash chain is built by the **Sealer** background thread, which runs only when `hash_enabled` and `shards == 1`.

**Single-shard constraint.** The Sealer seals one shard at a time, and a global hash chain across shards requires a global merge ordering that v1.1 does not provide. With `hash-chain` enabled and `shards > 1`, `LogDb::open` returns this exact error (`src/lib.rs:176-181`):

> hash-chain is not supported with shards > 1 in v1.1. Use shards=1 with hash-chain, or shards>1 without hash.

Multi-shard hash chaining is deferred to v1.2. See [Sharding](sharding.md) for the trade-off.

## compression

`compression` (`Config.compression_enabled`) applies streaming **zstd** compression to segment frames. Each frame is compressed independently, so the reader can decompress on the fly without seeking to a global dictionary. The dependency is pulled in with `default-features = false` to keep the build lean.

Compression is **transparent on read**: the same `LogDb::read` / scan APIs decode compressed segments without any caller-side change. There is no separate "compressed read" path.

Operational notes:

- Compression interacts with the sparse index: `index_stride` only affects **raw** segments — compressed segments are frame-based and have no per-record sparse index, so the knob is a no-op there (see [Configuration: index_stride](configuration.md#lower-index_stride-for-latency-sensitive-point-reads)).
- There is no per-record compression toggle; the choice is per-database at `Config` time.

## encryption

`encryption` (`Config.encryption_key: Option<[u8;32]>`) encrypts segment frames with **AES-256-GCM** authenticated encryption:

- **Per-frame random nonce.** Each frame gets a fresh nonce sourced from `getrandom`, so identical plaintext records encrypt to different ciphertext.
- **256-bit key.** The key is the 32-byte array you supply via `Config.encryption_key`. `None` means plaintext (no encryption).
- **Authenticated.** GCM carries an authentication tag per frame, so tampering is detected on read just like a CRC failure.

```rust
// 32-byte key — generate and manage this out of band.
let key: [u8; 32] = /* your key, e.g. from a KMS / vault */;
let config = Config {
    encryption_key: Some(key),
    ..Config::default()
};
```

**Key management is your responsibility, and key loss is unrecoverable.** Records are encrypted with the key that was active when they were written; if that key is lost, those records cannot be decrypted. logdb does **not** store the key, rotate keys automatically, or wrap keys at rest — `Config.encryption_key` is exactly the bytes you pass in. Treat it like any other root secret: source it from a KMS, a sealed vault, or an envelope-encryption scheme, and never log it.

## remote-push

`remote-push` is a **flag-only** feature: it gates the `pusher` module and the `LogDb::replicate` API but pulls in **no** extra dependencies. The remote story in v1.1 is intentionally split into two halves:

**Public API — `LogDb::replicate(sequence, timestamp_ns, content)`.** This is the **only** remote-related method on `LogDb`. It is the standby write-in path used by `logdbd` standby nodes to ingest records received from the primary at the primary's own sequence, preserving the global offset space so consumers can fail over primary → standby without re-mapping offsets. The standby contract (`src/lib.rs:305-391`):

- **Single-shard.** Replication is a linear stream onto shard 0, so `shards` must be `1`.
- **In-order.** `sequence` must equal the current producer cursor; gaps return an error so the caller retries.
- **Idempotent.** A `sequence` already replicated (below the cursor) is a no-op, so duplicate or replayed Sync RPCs are safe.
- **Backpressured.** Refuses to overwrite a live (uncommitted) slot, returning `QueueFull` via the same watermark gate as `claim`.

**Daemon-level plumbing — the Pusher / `RemoteSink` trait / `run_pusher`.** These are **not exposed via `LogDb`**. The `pusher` module is private (`mod pusher;` at `src/lib.rs:37` — note: not `pub mod`), and the Pusher is meant to be driven by an embedding daemon (e.g. `logdbd`) that owns its own thread, progress file, and backoff policy. There is **no one-line `db.push(...)` API**.

This is a **known gap** in v1.1: the library exposes the standby write-in (`replicate`) but not the primary-side push driver. A public push API would need its own design change record. For the daemon-level integration pattern, see [Extending logdb](../dev/extending.md) (the `RemoteSink` trait and how a host daemon threads records to a remote endpoint).

## See also

- [Usage guide](README.md)
- [Configuration](configuration.md) — the `Config` fields each feature unlocks.
- [Sharding](sharding.md) — why `hash-chain` is single-shard, and the throughput/latency trade-off.
- [Durability](durability.md) — orthogonal to all four features.
- [Recovery](recovery.md) — how hash-chain verification, compression, and decryption behave during recovery.

> logdb 0.2.0
