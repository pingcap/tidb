# Align `pkg/util/channel` with the pinned Go package

This ExecPlan follows `PLANS.md` and uses Go commit
`e2788410d8d696605e8cb002585877a063ccc909` as authority.

## Goal

Treat `BUILD.bazel` and `channel.go` as the complete package. Rust must expose
one channel-drain function, block until disconnect, and add no acceptance of
non-channel iterables. The source package has no tests, benchmarks, fixtures,
generated variants, or `TestMain`.

## Progress

- [x] Read both pinned Go artifacts and confirmed the zero-test inventory.
- [x] Read the Rust owner, synthetic contracts, semantic manifest, old audit,
  and all workspace consumers; there are no production consumers.
- [x] Narrowed `clear` from arbitrary `IntoIterator` values to the native
  standard receive-channel type.
- [x] Removed both synthetic tests and the retired semantic manifest.
- [x] Validate the Go zero-test package, Rust owner crate checks, formatting,
  scoped Clippy, and diff quality.
- [x] Prepare the validated package snapshot for a normal commit and push.

## Validation

Use the WIP profile because package-by-package parity work continues. No Go or
Bazel file changes are made, so `make bazel_prepare` is not required.

`go test ./pkg/util/channel` reports `[no test files]` as expected.
`cargo check -p tidb-util --all-targets --locked`, scoped owner Clippy,
`cargo fmt --all --check`, and `git diff --check` pass.
