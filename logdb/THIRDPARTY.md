# Third-Party Licenses

This is the license inventory for the **runtime** dependencies of the `logdb`
crate (built with `--all-features`; dev-only dependencies such as `criterion`
and `proptest` are excluded — they are not distributed with the library).

logdb is `Apache-2.0`. Every transitive runtime dependency below is
permissively licensed (MIT / Apache-2.0 / BSD-3-Clause). **There is no copyleft
(GPL/AGPL/LGPL/MPL/CDDL/EPL) anywhere in the graph.** This is enforced in CI by
`cargo-deny` (see [`../deny.toml`](../deny.toml)).

| Crate | Version | License | Source |
|-------|---------|---------|--------|
| aead | 0.5.2 | MIT OR Apache-2.0 | https://github.com/RustCrypto/traits |
| aes | 0.8.4 | MIT OR Apache-2.0 | https://github.com/RustCrypto/block-ciphers |
| aes-gcm | 0.10.3 | Apache-2.0 OR MIT | https://github.com/RustCrypto/AEADs |
| cfg-if | 1.0.4 | MIT OR Apache-2.0 | https://github.com/rust-lang/cfg-if |
| cipher | 0.4.4 | MIT OR Apache-2.0 | https://github.com/RustCrypto/traits |
| cpufeatures | 0.2.17 | MIT OR Apache-2.0 | https://github.com/RustCrypto/utils |
| crc32c | 0.6.8 | Apache-2.0/MIT | https://github.com/zowens/crc32c |
| crypto-common | 0.1.7 | MIT OR Apache-2.0 | https://github.com/RustCrypto/traits |
| ctr | 0.9.2 | MIT OR Apache-2.0 | https://github.com/RustCrypto/block-modes |
| generic-array | 0.14.7 | MIT | https://github.com/fizyk20/generic-array.git |
| getrandom | 0.2.17 | MIT OR Apache-2.0 | https://github.com/rust-random/getrandom |
| ghash | 0.5.1 | Apache-2.0 OR MIT | https://github.com/RustCrypto/universal-hashes |
| inout | 0.1.4 | MIT OR Apache-2.0 | https://github.com/RustCrypto/utils |
| libc | 0.2.186 | MIT OR Apache-2.0 | https://github.com/rust-lang/libc |
| opaque-debug | 0.3.1 | MIT OR Apache-2.0 | https://github.com/RustCrypto/utils |
| polyval | 0.6.2 | Apache-2.0 OR MIT | https://github.com/RustCrypto/universal-hashes |
| proc-macro2 | 1.0.106 | MIT OR Apache-2.0 | https://github.com/dtolnay/proc-macro2 |
| quote | 1.0.46 | MIT OR Apache-2.0 | https://github.com/dtolnay/quote |
| scopeguard | 1.2.0 | MIT OR Apache-2.0 | https://github.com/bluss/scopeguard |
| subtle | 2.6.1 | BSD-3-Clause | https://github.com/dalek-cryptography/subtle |
| syn | 2.0.118 | MIT OR Apache-2.0 | https://github.com/dtolnay/syn |
| thiserror | 2.0.18 | MIT OR Apache-2.0 | https://github.com/dtolnay/thiserror |
| thiserror-impl | 2.0.18 | MIT OR Apache-2.0 | https://github.com/dtolnay/thiserror |
| typenum | 1.20.1 | MIT OR Apache-2.0 | https://github.com/paholg/typenum |
| unicode-ident | 1.0.24 | (MIT OR Apache-2.0) AND Unicode-3.0 | https://github.com/dtolnay/unicode-ident |
| universal-hash | 0.5.1 | MIT OR Apache-2.0 | https://github.com/RustCrypto/traits |
| zstd | 0.13.3 | MIT | https://github.com/gyscos/zstd-rs |
| zstd-safe | 7.2.4 | MIT OR Apache-2.0 | https://github.com/gyscos/zstd-rs |
| zstd-sys | 2.0.16+zstd.1.5.7 | MIT/Apache-2.0 | https://github.com/gyscos/zstd-rs |

### Bundled C library

`zstd-sys` (under the `compression` feature) builds and statically links
**libzstd** by Facebook/Meta, licensed under **BSD-3-Clause** (permissive). The
full libzstd license is bundled with the source and available at
https://github.com/facebook/zstd/blob/dev/LICENSE.

## Regenerating

```sh
cargo install cargo-deny --locked
cargo deny check licenses         # enforces the allow-list (fails on copyleft)
cargo deny list                   # enumerates crates + detected licenses
```
