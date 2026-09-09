# `pkg/session/nontransactional.go` parity audit ExecPlan

## Objective

Audit the complete nontransactional DML production owner and its focused Go
tests, compare every behavior with the Rust workspace, and implement only a
dependency-closed parity fix. Keep the larger root `pkg/session` package
boundary explicit rather than claiming a single-file audit is package
completion.

## Completed this batch

1. Read all 873 lines and 21 functions in
   `pkg/session/nontransactional.go`, its root BUILD registration, and all 614
   lines of the associated test-only package (harness, six SQL tests, helper,
   and flaky six-shard target). No target fixture, generated output, or
   platform variant was omitted.
2. Ran the exact Go-master failpoint-managed suite: all seven tests passed in
   16.845s and failpoints were disabled during teardown.
3. Ran both Rust parser source regressions; each passed. The Rust
   `tidb-exec` admission target could not build because the environment lacks
   pkg-config/OpenSSL (`openssl-sys`).
4. Compared the full target behavior with Rust. Rust has parser and
   dependency-free admission pieces plus ignored source carriers, but no
   dependency-closed planner/worker/catalog/storage/metrics owner for the Go
   implementation.
5. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded hashes, validation evidence, and the explicit SEED
   boundary in `rust/testport/receipts/session_nontransactional.md`.

## Validation gate

- [x] Complete target production/test/Bazel inventory and Rust comparison.
- [x] Exact Go-master failpoint-managed behavior suite passed.
- [x] Rust parser regressions passed; admission build blocker recorded.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Sharding, SQL restoration, worker execution, constraints/foreign keys,
metrics, max-execution-time behavior, and aggregated job errors remain
cross-owner work in the session, planner, executor, catalog, table, storage,
and result-set crates. The root `pkg/session` package still has 25 direct
artifacts outside this slice; the repository package loop continues there.
