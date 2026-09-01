# `pkg/util/mathutil` parity audit ExecPlan

## Objective

Maintain the complete Go math utility package as one Rust owner, including its
exponential average, integer helpers, batch division, synchronized MySQL RNG,
source tests, and build target.

## Progress

- Read all eight Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: three production files, three
  source test files, `main_test.go`, and `BUILD.bazel` (431 lines total).
- Enumerated all production declarations and the eight source test functions;
  `TestMain` only installs common setup and goleak options. No docs, fixtures,
  generated/platform variants, benchmarks, fuzzers, examples, or nested
  packages exist.
- Confirmed the Go package is unchanged from the earlier extraction pin.
- Verified the Rust `tidb-util::mathutil` owner preserves source algorithms and
  all eight Go test identities. The owner removes Rust-only public cloning,
  debug, and `must_use` behavior; the focused regression for ignored return
  values was already added and passed. The duplicate executor batch helper now
  delegates to this owner.

## Validation

- Active and detached Go-master `go test ./pkg/util/mathutil -count=1` — passed.
- Rust mathutil owner suite and focused downstream DDL consumer suite — passed.
- Rust all-target/benchmark compilation, pinned fmt, diff checks, and detached
  pinned `make lint` — passed.
- `git diff --stat c6054025ed4c32ab3672a2a24ea46892714d21ec --
  pkg/util/mathutil` — empty.

## Completion and risks

The complete owner fix is already landed and this plan refreshes its authority
to current Go master. No Go or Bazel files changed, so `make bazel_prepare` is
not required. The 32-bit runtime branch remains source-reviewed rather than
cross-compiled locally because no 32-bit target is installed.
