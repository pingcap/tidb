# Align `pkg/util/channel` with the pinned Go package

This ExecPlan follows `PLANS.md` and uses Go `origin/master` commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` as authority. The package is
unchanged from the earlier pinned audit; this refresh records the rolling
master comparison.

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
- [x] Validate the Go zero-test package, all `tidb-util` library tests,
  formatting, and diff quality.
- [x] Refresh the receipt and ExecPlan for the current Go-master authority.
- [ ] Commit, push, pull, and verify the target branch synchronization.

## Validation

Use the Ready profile for this documentation-only authority refresh. No Go or
Bazel file changes are made, so `make bazel_prepare` is not required. No new
regression test is added because the complete Go package has no source tests
and this batch changes no behavior.

`go test ./pkg/util/channel` reports `[no test files]` as expected.
The full `tidb-util` library test suite passes (523 passed, 2 ignored),
`cargo fmt --all --check`, and `git diff --check` pass.
