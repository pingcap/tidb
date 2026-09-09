# `pkg/util/gctuner` parity audit ExecPlan

## Objective

Inventory the complete Go GC tuner package at current Go master and establish
whether Rust can own its runtime finalizer, GOGC tuner, and memory-limit policy
without a detached scheduler or allocator policy.

## Progress

- Read all nine Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five production files, three
  source test files, and `BUILD.bazel` (993 lines total; 30 production
  declarations and seven source tests). No docs, fixtures, generated/platform
  variants, benchmarks, fuzzers, examples, or nested packages exist.
- Re-read the detached Go-master `memory_limit_tuner.go` and confirmed its
  source contract: self-reinstalling finalizer, GOGC threshold arithmetic,
  runtime/debug memory-limit adjustment, disable/enable nesting, and the two
  failpoint races. The current worktree has an existing branch-only
  memory-arbitration delta; it is preserved and excluded from Go-master scope.
- Searched Rust memory, alarm, and server-limit crates. They provide supporting
  fragments but no dependency-closed owner for Go's process-global finalizer,
  GOGC, SetMemoryLimit, and lifecycle policy.

## Decision

Keep `pkg/util/gctuner` explicitly unclaimed. A Rust finalizer, synthetic GC
percentage, or independent memory-limit thread would create Rust-only
scheduling and runtime policy with no complete consumer graph.

## Validation

- Active and detached Go-master failpoint-runner suites for the focused tuner,
  finalizer, GC-percent, and memory-limit tests — passed.
- Rust ownership search completed; no dependency-closed owner suite exists.
- Rust fmt, pinned detached `make lint`, and diff checks — passed.

## Risks and follow-up

Go runtime finalizers, GOGC, and `runtime/debug.SetMemoryLimit` have no direct
portable Rust equivalent. A future owner must move domain, resource-manager,
BR, session, and server lifecycle consumers together and preserve process-global
state and cadence. Race-enabled Bazel and live server/BR behavior remain
unverified here.
