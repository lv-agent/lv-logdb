# Key Management

logdb's `encryption` feature is only as strong as how you generate, store, and
rotate the 32-byte AES-256-GCM key. **Cryptography is never the weak link; key
handling is.** This guide covers the minimum a production deployment must do.

See [threat-model.md](threat-model.md) for what encryption does and does not
protect against, and the root [`SECURITY.md`](../../../../SECURITY.md).

---

## 1. Generate the key with a CSPRNG

Never derive the key from a password, a constant, `Math::random`, the system
clock, or anything predictable. Use the OS CSPRNG:

```rust
let mut key = [0u8; 32];
getrandom::getrandom(&mut key).expect("CSPRNG available");
let mut config = logdb::Config::default();
config.encryption_key = Some(key);   // requires the "encryption" feature
```

For a passphrase-derived key, use a real KDF (Argon2id / scrypt / HKDF from a
master secret) — never a raw hash of a password.

## 2. Store the key outside the data directory

The single most important rule:

> **The encryption key must not live where the ciphertext lives.**

If the key sits next to the segment files, anyone who can steal the disk gets
both — and encryption buys nothing. Store the key in a separate trust domain:

| Environment | Where to keep the key |
|-------------|----------------------|
| Cloud / server | KMS (AWS KMS, GCP KMS, Azure Key Vault), HashiCorp Vault, or a secrets manager. At startup, fetch the key over TLS into process memory; never write it to disk. |
| Edge / appliance | TPM, secure element, or an OS keychain. |
| Single-user / embedded | An OS keychain (macOS Keychain, Windows DPAPI, Linux secret-service), or a passphrase fed through Argon2id. |

### Envelope encryption (recommended for server deployments)

Hold a **data-encryption key (DEK)** per logdb instance in memory, wrapped by a
**key-encryption key (KEK)** that lives only in the KMS. Persist the *wrapped*
DEK (not the raw key) next to your app config. The raw DEK never touches disk.
logdb itself takes a raw 32-byte key; the wrapping is your responsibility.

## 3. Restrict the data directory

```sh
chmod 700 /var/lib/myapp/logdb_data   # owner-only; group/world cannot read
```

Encryption supplements filesystem permissions; it does not replace them. The
`data_dir` must not be world-readable, must not be on a world-readable backup,
and must not be logged.

## 4. Plan for rotation

Two reasons to rotate:

- **Compromise / suspicion** — rotate immediately. Old data stays under the old
  key; new data under the new key.
- **Volume bound** — AES-256-GCM with random 96-bit nonces has a birthday bound
  at ~2⁴⁸ frames per key. Rotate well before that (a non-issue for any normal
  log volume, but it is the theoretical ceiling).

### How to rotate today

logdb uses **one key per instance for the life of the data**; it does not yet
support in-place key rotation or key-versioned segments. To rotate:

1. Open the log with the **old** key, scan everything out.
2. Open a fresh `data_dir` with the **new** key, re-append.
3. Delete the old data directory and retire the old key.

> **Roadmap:** an on-disk key-version header would let segments under different
  keys coexist, enabling rolling rotation without a full rewrite. The format
  reserves header space for this.

## 5. Hash-chain key

The `hash-chain` feature computes a BLAKE3 keyed chain. As noted in
[threat-model.md](threat-model.md), logdb currently derives the chain key
(`hash_init`) from non-secret inputs and persists it in the segment header — so
the chain is tamper-**evidence**, not authenticity against a forger.

If you require authenticity against an attacker with write access, do **not**
rely on the built-in chain key as a secret. Instead, treat the data directory as
trusted storage, or layer your own authenticated log (e.g. sign segment roots
with a key you control in a KMS).

## 6. Don'ts

- ❌ Don't hard-code the key in source, config files, container images, or
  environment variables that get logged.
- ❌ Don't reuse one key across unrelated datasets (a compromise then spans all of
  them).
- ❌ Don't ship a backup of `data_dir` without confirming the key is *not* in the
  same backup.
- ❌ Don't keep the key in swap — on Linux, consider `mlock`-ing the region that
  holds it (logdb does not do this for you).

## Quick checklist

- [ ] Key generated from a CSPRNG (or a proper KDF).
- [ ] Key stored in a KMS / secrets manager / OS keychain — never in `data_dir`.
- [ ] `data_dir` is `chmod 700` and not on shared/world-readable storage.
- [ ] You know how you would rotate (re-write path) if the key were compromised.
- [ ] Backups of `data_dir` do not include the key.
