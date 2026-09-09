# `pkg/config/configtypes` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `types.go` — `ByteSize` and `Duration`, including their JSON and text
  marshal/unmarshal methods;
- `types_test.go` — `TestByteSize` and `TestDuration`, each with JSON and TOML
  subtests;
- `BUILD.bazel` — one library target and one two-shard test target.

There is no `doc.go`, ownership file, generated/platform source, fixture, test
harness, or benchmark in this package. The checkout is byte-identical to the
pin. This package is independent from the root `pkg/config` claim in
`b001.md`.

## Rust ownership and audit result

`rust/crates/tidb-config/src/configtypes.rs` owns both wrapper types and their
Serde JSON/TOML behavior. Its two owner tests reproduce every assertion from
the Go tests; the deleted `tests/configtypes_source.rs` duplicated those same
assertions without adding behavior.

The audit removed four public Rust helpers and two conversion conveniences
that do not exist in Go `pkg/config/configtypes`. Formatting/parsing remains
private implementation machinery for the source methods. Downstream log code
now uses the public `Duration` wrapper's promoted-string equivalent, and the
server configuration consumer deserializes that wrapper instead of calling a
Rust-only parser API.

Reading pinned `docker/go-units` v0.5.0 exposed a real mismatch in the private
`RAMInBytes` implementation: Go delegates its numeric prefix to
`strconv.ParseFloat`, accepting forms such as `.3kB`, `32.KiB`, `+32MiB`, and
`1e2KiB`. Rust previously imposed a narrower invented grammar. The parser now
follows the pinned dependency's split, suffix, nonnegative, and conversion
behavior. Duration parsing now follows Go's checked integer/fraction algorithm
and overflow limits rather than unchecked `i64` arithmetic and a broad `f64`
approximation.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/config/configtypes` — passed (2 tests, 4 subtests).
- `cargo test -p tidb-config --locked` — 81 unit and 21 integration tests
  passed; no ignored tests.
- `cargo test -p tidb-log --locked` — 22 tests passed.
- `cargo check -p tidb-log -p tidb-server --lib --locked` — passed with
  pre-existing warnings.
- `cargo fmt --all` and `git diff --check` — passed.

An additional broad server command,
`cargo test -p tidb-server --test all --locked node_config_source`, ran 33
tests: 32 passed and the unrelated
`configured_read_tables_are_atomic_ordered_and_globally_unique` failed. The
failure reproduces alone and at the unmodified `HEAD`: production already sets
`MAX_CONFIGURED_READ_TABLES` to 4096 while the stale test expects two tables to
exceed the limit. The configtypes change does not touch that policy, and this
package unit does not modify the unrelated test.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; accepted byte-size syntax and duration overflow now
  follow the exact upstream authorities.
- Compatibility: public Rust APIs absent from Go were removed; all workspace
  consumers were migrated and checked.
- Performance: parsing remains startup/configuration-only; no execution-path
  policy changed.
