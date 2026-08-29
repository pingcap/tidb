# `pkg/config/deploymode` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `doc.go` — package contract for process-wide TiDB X deployment mode and its
  fixed-resource and cross-instance consistency constraints;
- `mode.go` — the integer-backed `Mode`, process-wide atomic state, predicates,
  parsing, validation, enumeration, display, and JSON/TOML behavior;
- `mode_test.go` — `TestModeJSON`, `TestModeTOML`, and `TestCurrentMode`;
- `BUILD.bazel` — one library target and one test target.

There is no ownership file, generated/platform source, fixture, benchmark, or
additional test harness in this package. The checkout is byte-identical to the
pin.

## Rust ownership and audit result

`rust/crates/tidb-config/src/deploymode.rs` owns the complete package behavior,
and root configuration consumes its public `Mode`. The three owner tests cover
every assertion in the three Go tests. The deleted
`tests/deploymode_source.rs` duplicated those assertions and maintained a
second behavioral carrier without a Go counterpart.

Go defines `Mode` as an `int32`; the previous Rust enum added an `Unknown(i32)`
variant and therefore gave invalid values enum-discriminant ordering rather
than Go's numeric ordering. Rust now uses a transparent integer-backed type,
preserving arbitrary source values, equality, hashing, and ordering while
retaining named constants for the three valid modes. The package documentation
now carries the pinned Go contract for SYSTEM-keyspace execution and the
intentional component-config consistency tradeoff.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/config/deploymode` — passed (3 tests).
- `cargo test -p tidb-config --locked` — 81 unit and 18 integration tests
  passed; no ignored tests.
- `cargo test -p tidb-config --features nextgen --locked` — 82 unit and 18
  integration tests passed; no ignored tests.
- `cargo check -p tidb-domain -p tidb-server -p tidb-session --lib --locked` —
  passed with pre-existing warnings.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; invalid modes now retain Go's integer semantics.
- Compatibility: the Rust-only enum variant was replaced by the explicit raw
  integer constructor already used by configuration consumers and tests.
- Performance: unchanged; the process-wide value remains an atomic `i32`.
