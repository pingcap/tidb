# `pkg/sessionctx/variable/tests/slowlog` parity audit ExecPlan

## Objective

Audit the nested Go slow-log test package independently from the variable root,
map every test to its Rust owner, and land only dependency-closed behavior.

## Completed

- Read all three Go-master artifacts (766 lines): the ten-shard flaky BUILD
  target, goleak/TestMain harness, and all 10 slow-log tests.
- Ran the exact Go-master failpoint-managed suite successfully.
- Confirmed Rust's `slow_log_parse`, `slow_log_match`, and
  `slow_log_threshold` owners execute the parser, grouping, precedence, and
  typed-threshold leaves. Existing aggregate tests pass.
- Found no safe package-local implementation for the remaining live field
  accessors; those depend on unported `SessionVars`, `StmtContext`, execution
  details, and slow-log output. No Rust-only behavior was removed.

## Validation gate

- [x] Complete three-artifact inventory, including the absence of fixtures,
      generated/platform variants, fuzz/benchmark inputs, and extra targets.
- [x] Exact Go-master failpoint suite passes and failpoints are disabled.
- [x] Rust aggregate slow-log tests, formatting, `make lint`, and diff checks
      pass (Ready profile).
- [x] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote SHAs, and fast-forward pull the explicit branch ref.

## Remaining boundaries

`TestSlowLogFieldAccessor` and the six live match/accessor cases still need a
dependency-closed owner for Go's `SlowLogRuleFieldAccessors`,
`SlowQueryLogItems`, session-variable state, and execution-detail snapshots.
The package remains an explicit test-boundary claim rather than a completed
slow-log transcreation.
