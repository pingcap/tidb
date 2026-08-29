# `pkg/util/nocopy` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `nocopy.go` and
`BUILD.bazel`. There is no test, benchmark, package doc, README, fixture,
generated or platform variant, or ownership file. The local Go package is
byte-identical to the pin.

Production behavior is one zero-sized `NoCopy` marker with no-op `Lock` and
`Unlock` methods. Go's vet analyzer recognizes that method pair and prevents
copying an embedding owner after use.

## Rust ownership and audit result

`rust/crates/tidb-util/src/nocopy/mod.rs` owns the complete package. Rust
preserves the zero-sized marker and two no-op methods, while implementing
neither `Copy` nor `Clone`; this is the native enforcement of the Go vet
contract. The unit struct itself represents Go's directly constructible zero
value.

The audit removed the Rust-only public constructor and `Debug` behavior, one
supplemental unit test, one compile-fail doctest, and the separate semantic
test manifest. It also removed the legacy nocopy ExecPlan because that document
required those non-Go artifacts and pinned a different source revision. The Go
package has none of those artifacts. A later strict-surface re-audit also
removed the redundant `Default` trait and compile-time `const` calls; Go has
only ordinary no-op methods on the zero value.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `cargo check --offline --locked -p tidb-util --all-targets` — passed.
- `cargo test --offline --locked -p tidb-util --lib nocopy -- --test-threads=1` —
  passed; zero tests ran, matching the source package's test inventory.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production marker size, ownership semantics, and methods are
  unchanged.
- Compatibility: repository-unused Rust-only constructor and formatting
  behavior are intentionally removed.
- Performance: unchanged.
