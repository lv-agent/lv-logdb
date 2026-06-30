# Threat Model

This document describes what logdb's security features **protect against** and,
just as important, what they **do not**. It is written for engineers integrating
logdb into a product who need to reason about its security boundary.

See also: [key-management.md](key-management.md) (how to handle encryption keys)
and the root [`SECURITY.md`](../../../../SECURITY.md) (vulnerability disclosure).

---

## Features in scope

| Feature flag | Primitive | Purpose |
|--------------|-----------|---------|
| `encryption` | AES-256-GCM per frame | Confidentiality + integrity of record **contents** at rest |
| `hash-chain` | BLAKE3 keyed forward-linking chain | Tamper-**evidence** of the record sequence (shards == 1) |
| (always on) | CRC-32C per record + segment header | Accidental-corruption / torn-write detection |

These are **distinct, layered** mechanisms: CRC catches *accidental* damage;
AES-GCM hides and authenticates *contents*; the hash chain detects
*structural tampering* (reorder/delete/insert/modify) when verified.

---

## Encryption (`encryption` feature)

### What it does

Each commit batch is serialized, optionally compressed, then encrypted with
**AES-256-GCM** under the configured 32-byte key. The frame stored on disk is
`{ nonce(12 bytes) | ciphertext + 16-byte tag }`.

- The 96-bit **nonce is freshly random per frame** via the OS CSPRNG
  (`getrandom`), never reused deterministically.
- GCM is an **AEAD**: it provides both confidentiality (ciphertext is
  unintelligible without the key) and authenticity (any modification to the
  ciphertext or tag fails decryption).

### What it protects against

- **Offline disclosure of segment files** — stolen disk, lost backup, a
  filesystem snapshot leaving your trust boundary. Without the key, record
  contents cannot be read.
- **Tampering with record contents** on disk — modification is detected on read
  (decryption fails).

### What it does NOT protect against (read carefully)

- **Metadata leakage.** AES-GCM is not length-hiding. An attacker with the
  segment file learns: the number of records (frame/record boundaries), their
  approximate sizes, timestamps (these are structural, not encrypted), and the
  segment roll/retention pattern. *That traffic patterns exist is visible; what
  the records say is not.*
- **Process / memory compromise.** The key lives in process memory (in the
  `Config`). A core dump, a swap file, a debugger, or any code-execution bug in
  the host process exposes the key. logdb does **not** `mlock` the key or
  zeroize it on drop.
- **No forward secrecy.** One key for the life of the data. Compromise of the
  key decrypts the **entire history**, not just future records.
- **No access control.** Encryption supplements, not replaces, OS file
  permissions. It defends against *offline* media theft, not against an
  attacker who can run code in your process or read its memory.
- **Structural metadata is not encrypted.** `record_id`, sequence numbers, ring
  cursors, and the segment/checkpoint/tailer metadata files are plaintext
  (CRC-protected, not encrypted).
- **No anti-replay at the file level.** An attacker with *write* access to the
  data directory can replace a segment file with an older (still-valid)
  ciphertext. Encryption detects *edits*, not *replacement of a whole file by a
  valid older copy*. (Pair with the hash chain + operational controls for this.)
- **Nonce birthday bound (operational).** With random 96-bit nonces, the
  probability of a repeated nonce becomes non-negligible after ~2⁴⁸ frames
  under a **single key**. A repeated (key, nonce) pair is catastrophic for
  GCM confidentiality. For any realistic log volume this is astronomically far
  off, but it is the trigger for **key rotation** — see
  [key-management.md](key-management.md).

---

## Hash chain (`hash-chain` feature, requires `shards == 1`)

### What it does

When enabled, a background sealer computes a forward-linking BLAKE3 chain:

```
hash_n = BLAKE3_keyed(hash_init,  hash_{n-1} || record_content)
```

Each record's hash incorporates the previous record's hash and its own content.
Verification replays the chain and rejects any divergence.

### What it protects against

- **Tamper-evidence.** Any modification, deletion, insertion, or reordering of
  records breaks the chain and is detected at verify time. This catches both
  accidental corruption *and* unsophisticated in-place edits.

### What it does NOT protect against

- **It does not prevent tampering** — it *detects* it (on verify). An attacker
  with write access can still corrupt data; you find out when you read.
- **Single-shard only.** A global chain needs total order; `shards > 1` is
  rejected at `Config::validate`.
- **⚠️ Not cryptographic authenticity against a sophisticated forger.** This is
  the most important caveat. The chain is BLAKE3 in *keyed* mode — but the key
  (`hash_init`) is:
  1. derived by logdb from **non-secret inputs** (wall-clock time + a domain
     constant), and
  2. **persisted in cleartext** in the segment header.

  Therefore an attacker who can read the segment header can recover `hash_init`,
  recompute the chain, and forge a self-consistent tail. The chain is a strong
  integrity check against accidental damage and read-only or naive attackers,
  but **not a MAC against an active forger with write access**. If you need
  authenticity against such an attacker, derive the chain key from your own
  secret (see [key-management.md](key-management.md#hash-chain-key)) and/or keep
  the data directory on trusted storage.

> **Roadmap:** a future hardening may derive `hash_init` from the user's secret
> and avoid persisting it in cleartext, turning the chain into a true MAC. Until
> then, treat `hash-chain` as tamper-**evidence**, not tamper-**resistance**.

---

## CRC-32C (always on)

Per-record and per-segment-header CRC-32C detects **accidental** corruption:
torn writes (a record half-written before a crash), bit rot, and truncated
files. On reopen, recovery truncates at the last CRC-valid record. CRC is
**not** a security control — it has no key and is trivially recomputable by an
attacker.

---

## Summary matrix

| Threat | Encryption | Hash chain | CRC |
|--------|:----------:|:----------:|:---:|
| Stolen disk / offline file disclosure | ✅ | — | — |
| Accidental corruption / torn write | (detected) | ✅ | ✅ |
| In-place edit of a record | ✅ detected | ✅ detected | ✅ detected |
| Reorder / delete / insert records | — | ✅ detected | — |
| Whole-file replacement by valid older copy (replay) | ❌ | partial | — |
| Forged chain by an active attacker with write access | ❌ | ❌ | ❌ |
| Metadata (count, size, timing) exposure | ❌ | — | — |
| Process/memory compromise (key theft) | ❌ | ❌ | — |

✅ = defends; (detected) = flags on read; ❌ = not defended; — = N/A.

## Trust boundary

logdb's security model assumes the **data directory and the host process** are
within your trust boundary for *confidentiality of the key*, while the **storage
media** (disks, backups, snapshots that leave the host) is not. If the host
itself is untrusted, logdb's encryption cannot help — use a KMS / envelope
encryption layer above it, or run on trusted infrastructure.
