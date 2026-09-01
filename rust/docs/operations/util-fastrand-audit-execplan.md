# `pkg/util/fastrand` parity audit ExecPlan

## Objective

Keep the complete Go-master fast-random package aligned with its native Rust
runtime owner, source test, and benchmark translations while removing Rust-only
API diagnostics.

## Progress

- [x] Read all five Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `main_test.go`,
  `random.go`, `random_test.go`, and `runtime.go` (227 lines; three private
  random declarations, four public helpers, `TestMain`, one source test, and
  four benchmarks).
- [x] Confirm there are no package docs, fixtures, generated inputs/outputs,
  platform files, or nested packages; separately reviewed the Go 1.25.10
  runtime.cheaprand source linked by `runtime.go`.
- [x] Remove Rust-only `must_use` annotations from `buf`, `uint32_n`,
  `uint64_n`, and `uint32`; add `TestReturnValuesMayBeIgnoredLikeGo`, which
  failed before the fix and passes after it. Preserve wyrand arithmetic,
  bounded reductions, 32-bit xorshift branch, thread-local runtime state, and
  all four benchmark identities.
- [x] Revalidate current and exact detached Go-master tests, focused Rust
  regressions, all-target and benchmark compilation, formatting, diff quality,
  and the pinned detached `make lint` Ready gate.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a focused Ready parity fix. No Go or Bazel file changed, so
`make bazel_prepare` is not required. Exact commands, pre-fix failure,
detached lint result, and remaining 32-bit cross-compilation boundary are
recorded in
`rust/testport/receipts/util_fastrand.md`.

## Next boundary

Any future random helper must preserve the Go runtime source, unsigned edge
cases (including zero bounds), excluded-byte policy, and platform-specific
cheaprand branch. Avoid `must_use` or deterministic seed APIs absent from Go.
