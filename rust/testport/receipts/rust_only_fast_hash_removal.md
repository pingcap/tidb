# Rust-only `fast_hash` policy removal

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Ownership audit

There is no Go `pkg/util/fast_hash` package and no pinned external-module
package represented by `rust/crates/tidb-util/src/fast_hash.rs`. The file was a
Rust-only fixed-seed Fx hashing policy, exported three collection aliases, and
owned three supplemental tests.

The two consumers were checked against the pinned Go code before removal:

- `pkg/parser/misc.go` stores `tokenMap`, `btFuncTokenMap`, and
  `windowFuncTokenMap` in ordinary Go maps and performs direct membership
  lookups after ASCII uppercasing.
- `pkg/sessionctx/variable/variable.go` stores `sysVars` in an ordinary Go map
  protected by `sync.RWMutex` and lowercases names before lookup.

Rust now uses standard `HashSet` keyword maps and the standard `HashMap`
system-variable index. The custom hasher module, its three tests, its public
export, and the otherwise-unneeded `tidb-lexer -> tidb-util` dependency edge
were removed. `Cargo.lock` was regenerated from the manifest change.

This is deletion of unowned Rust policy, not a claim that either complete Go
owner package has been transcreated by this receipt.

## Validation

Profile: WIP; this is a parity cleanup within the continuing package audit,
not a repository-wide readiness claim.

- `cargo test -p tidb-lexer --locked` — passed (85 library tests, 1 binary test, 5 integration tests).
- `cargo test -p tidb-session the_registry_is_complete_and_sorted --locked` — passed.
- `cargo test -p tidb-session defaults_come_from_the_registry --locked` — passed.
- `cargo test -p tidb-parser --locked` — passed (722 unit tests; 89 integration tests passed and 1 MySQL-dependent test ignored).
- `cargo test -p tidb-util --locked` — passed (637 unit tests, 3 ignored helpers, all integration and doc tests).
- `cargo check -p tidb-lexer -p tidb-session -p tidb-util --locked` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: collection membership and lookup semantics are unchanged and
  are covered by the lexer, parser, and case-insensitive sysvar tests.
- Compatibility: the Rust-only `tidb_util::fast_hash` public API is removed;
  repository-wide search found only the two migrated internal consumers.
- Performance: standard randomized hashing replaces the Rust-only fixed-seed
  performance policy, matching Go's ordinary-map behavior rather than keeping
  a separate Rust tuning rule.
