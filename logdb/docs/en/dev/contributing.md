# Contributing

How to contribute to logdb: the test-driven workflow, the design-doc convention, commit and code style, and the pull-request process.

> Authoritative for **logdb 0.2.0**. These are the project's working conventions — follow them faithfully.

## Workflow: test-driven, design-first

logdb is developed **test-first**. For any feature or bugfix:

1. **Write a design doc first** (before feature code) at `veps/cr-NNN-<english-topic>.md`.
   - The **filename is English** (e.g. `cr-002-push-api.md`); the **body is Chinese** — discussion and specs are written in Chinese, the project's working language, while all *artifacts* (code, comments, commits, filenames) stay English.
   - The `veps/` directory is **git-ignored and never committed**. It holds working design notes only.
2. **Write the failing test first.** Red → green. Add or extend a unit test in the relevant `src/` module's `#[cfg(test)]` block, or an integration test under `tests/`, and confirm it fails for the right reason.
3. **Make it pass** with the minimum change. Refactor after green, not before.
4. **Keep both doc languages in sync** (see Docs maintenance below).

For the test taxonomy and commands, see [Testing](testing.md). For where new modules belong, see [Project layout](project-layout.md).

## Commit conventions

- **English only.** Commit subjects and bodies are English, even though discussion and design docs are Chinese.
- **No `Co-Authored-By`.** Do not add `Co-Authored-By:` trailers — to anyone, ever. This is a hard project rule.
- **Focused commits.** One logical change per commit; write a concise subject (imperative mood) and, where useful, a body explaining *why*.
- **Commit `docs/` only on doc work, never `veps/`.** `veps/` is git-ignored by design — verify it stays out of your commits.

## Code style

- **`rustfmt`.** Run `cargo fmt` before every commit; CI expects formatted code.
- **English code and comments.** Identifiers, doc-comments, and inline comments are English. Chinese is reserved for conversation and the `veps/` design-doc bodies.
- **Additive features only.** A new feature must not change default-build behavior; gate it with `#[cfg(feature = "...")]`. See [Extending / Adding a feature flag](extending.md#adding-a-feature-flag).

## Pull-request process

1. **Open a focused PR** — one feature or one fix, not a bundle. Small PRs review faster and land safer.
2. **Pass `cargo test`.** On a default build at minimum:
   ```sh
   cargo test
   cargo fmt -- --check
   cargo clippy --all-targets -- -D warnings
   ```
3. **Run the feature matrix** if your change is feature-gated:
   ```sh
   cargo test --features compression
   cargo test --features encryption
   cargo test --features hash-chain
   cargo test --all-features
   ```
4. **Fuzz or bench where relevant.** If you touch serialization, segments, or the append path, run the relevant fuzz target or `cargo bench` and report the delta. See [Testing](testing.md).
5. **Update both doc languages.** If your change is user-visible, update `docs/en/` **and** `docs/zh/` in the same PR (see below).

## Docs maintenance

English and Chinese docs must **stay in sync**. The two trees (`docs/en/`, `docs/zh/`) mirror each other file-for-file.

- **Add a page to both sides.** A new `docs/en/<area>/<page>.md` implies a new `docs/zh/<area>/<page>.md`, and vice versa.
- **Edit both sides in the same change.** When you fix a factual error or add a section on the English side, mirror it on the Chinese side (and the reverse).
- **Verify parity before pushing:**
  ```sh
  diff <(cd docs/en && find . -name '*.md' | sort) \
       <(cd docs/zh && find . -name '*.md' | sort)
  # empty output ⇒ both trees have the same set of files
  ```
- **Keep prose idiomatic in each language.** Code, symbols, paths, and identifiers stay English in both trees; only the surrounding prose changes language.

## See also

- [Development guide home](README.md)
- [Testing](testing.md) — the test taxonomy and feature-matrix commands referenced above.
- [Extending](extending.md) — the additive-feature and durability-guardrails rules every change must respect.
- [Building](building.md) — toolchain, features, and the qualification scripts.

> logdb 0.2.0
