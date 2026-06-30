# Security

This section covers the security properties of logdb's optional `encryption` and
`hash-chain` features, and how to operate them safely.

- [**Threat model**](threat-model.md) — what the security features protect
  against, and (importantly) what they do **not**.
- [**Key management**](key-management.md) — how to generate, store, and rotate
  the AES-256-GCM key. **Read this before deploying the `encryption` feature.**

For reporting a vulnerability, see the repository root
[`SECURITY.md`](../../../../SECURITY.md).

For third-party license attributions, see [`THIRDPARTY.md`](../../../THIRDPARTY.md).
