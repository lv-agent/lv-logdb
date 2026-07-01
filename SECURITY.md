# Security Policy

## Reporting a Vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

Please report suspected vulnerabilities **privately**, preferably via GitHub's
Private Vulnerability Reporting:

> **[Report a vulnerability](https://github.com/lv-agent/lv-logdb/security/advisories/new)**

(GitHub → *Security* tab → *Report a vulnerability*.)

Alternatively, create a private security advisory via the GitHub Security tab.

Please include:

- The logdb version and feature set (e.g. `logdb 0.3.0, features: encryption`).
- The platform (OS, architecture, filesystem).
- A minimal reproduction or the exact steps that trigger the issue.
- The impact you observe or suspect.

## Response

We will acknowledge receipt and respond as soon as practical. Response
timelines are **best-effort** and depend on severity and availability — there
is no committed SLA. We prefer coordinated disclosure and will credit reporters
unless they request anonymity.

## Supported Versions

logdb is pre-1.0 and under active development. Security fixes are applied to
the **latest released version**. Older versions are not maintained.

## Scope

This policy covers the **logdb** embedded library crate and its on-disk format
(record/segment layout, encryption, hash chain). It also covers the **logdbd**
gRPC daemon included in this repository.

Out of scope:

- Vulnerabilities in a downstream application that misuses the logdb API
  (e.g. hard-coding keys, disabling durability, world-readable `data_dir`).
- Issues whose impact requires an attacker who already has the encryption key
  or full host/compromise — see
  [`logdb/docs/en/security/threat-model.md`](logdb/docs/en/security/threat-model.md)
  for the threat model and explicit non-goals.
- Theoretical crypto "attacks" faster than the documented bounds (e.g. AES-GCM
  nonce collision requires ~2⁴⁸ frames under one key — see
  [`key-management.md`](logdb/docs/en/security/key-management.md)).

## Cryptography Notice

logdb uses well-vetted primitives (AES-256-GCM, BLAKE3, CRC-32C) via the
RustCrypto / `blake3` crates. **Cryptography is only as strong as key
management** — read
[`logdb/docs/en/security/key-management.md`](logdb/docs/en/security/key-management.md)
before deploying the `encryption` feature.
