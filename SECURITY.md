# Security Policy

## Reporting a Vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

Please report suspected vulnerabilities **privately**, preferably via GitHub's
Private Vulnerability Reporting:

> **[Report a vulnerability](https://github.com/lv-agent/lv-logdb/security/advisories/new)**

(GitHub → *Security* tab → *Report a vulnerability*.)

Alternatively, email the maintainer at **security@lv-agent.example**
(replace with the maintained address before publishing) with a description and,
if possible, a proof of concept.

Please include:

- The logdb version and feature set (e.g. `logdb 0.3.0, features: encryption`).
- The platform (OS, architecture, filesystem).
- A minimal reproduction or the exact steps that trigger the issue.
- The impact you observe or suspect.

## Response SLA

| Stage | Target |
|-------|--------|
| Acknowledge receipt | within 3 business days |
| Initial assessment / triage | within 14 days |
| Fix or mitigation coordinated disclosure | best-effort, typically within 90 days |

We will keep the reporter informed of progress and coordinate a public
disclosure date. Coordinated disclosure is preferred; we will credit reporters
unless they prefer to remain anonymous.

## Supported Versions

logdb is pre-1.0. Security fixes are applied to the **latest released minor**
only; older versions are not maintained. Backports are at the maintainer's
discretion.

| Version | Supported |
|---------|-----------|
| latest 0.x | ✅ |
| older 0.x | ❌ |

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
