# Preserve Go PD retry values before adding transport interception

This living ExecPlan follows TiDB PLANS.md. The acceptance unit remains the
complete pinned PD pkg/retry package, including both production files, original
test file, support/platform/build inputs and all existing native callers.

## Purpose and context


Tracing pkg/utils/grpcutil revealed that UnaryBackofferInterceptor copies the
caller backoffer by value for every RPC (`newBo := *bo`). Rust's boxed callback
makes that impossible; sharing its mutable execution state under a lock would
serialize unrelated RPCs. The existing retry port also explicitly substitutes
unsigned Duration and saturating arithmetic for Go signed nanoseconds and wrapping
arithmetic. These are dependency gaps, not complete transport ownership. Repair
the shared package before integrating either interceptor; add no partial grpcutil
production owner.

Both branches were refreshed without incoming changes. Go master is
93a01d31f6da205ae4bf376825293903a6899fdb; PD remains
v0.0.0-20260805103528-afa43111d149. Native baseline is
44afb53ffadfb5a711fa9c459d95fd197a3adb1a, TiDB baseline is
dac222a94dabccae88c98324395fbd2c352f600c. Existing package inventory/receipts and
all original cases are retained and rechecked, including source failpoints.

## Progress


- [x] Refresh branches; inspect complete retry/grpcutil packages and native consumers.
- [x] Reproduce valid-Go duration overflow against unchanged native production.
- [x] Restore complete signed timing/count domain, reusable constructor options and value-copy ownership; migrate all callers.
- [x] Re-run original Go race/goleak with scoped failpoints, source arithmetic/copy oracle, all native cases and static gates.
- [ ] Publish native, sync TiDB, validate adapters/lint, commit with actual locked server hook and freshly rebuild before push.

## Implementation milestones


First add an executed regression for the next-interval sequence with base/max
MaxInt64 nanoseconds and unbounded total. Go yields MaxInt64, -2, -4, while the
unsigned saturation policy yields MaxInt64 repeatedly. Keep the red log. Remove
the old Duration::MAX saturation assertion, which tests a Go-absent duration and
policy; preserve all original cap tests and all remaining regression assertions.

Represent all retry durations as signed i64 nanoseconds and retry attempt counts
as isize. Preserve constructor conditions, signed wrapping additions/subtractions/
doubling, positive-total-only budgeting, positive-log-interval-only logging and
negative/zero timers becoming immediately eligible. Fixed retry rejects a
non-positive ticker interval even when maxTimes is zero/negative. Positive ticker
phase and dropped-tick semantics remain unchanged. Convert only at the Tokio timer
boundary; no new sleep policy or retry worker is added. Default initialization
keeps 100 attempts at one second, directly passing its signed source count.

Derive Clone for Backoffer, sharing only the retryable callback handle through Arc
and copying every scalar, including an in-progress snapshot. Keep the existing
Box-accepting setter by converting its owned function into Arc. Overwriting or
resetting one copy must not change another copy; captured callback state remains
shared like Go closure identity. Clone a context-attached backoffer under its
existing short ownership guard, then execute copies independently. No callback
or wait holds the shared context mutex. Panic/drop reset and error identity remain
unchanged. Tests prove exact snapshots, concurrent independent execution, callback
identity, replacement isolation, signed budgets, log counters and source reset.

Update complete source inventory mappings and qualify historical unsigned-domain
acceptance. Neither grpcutil nor root/discovery/TSO ownership is accepted by this
repair. Their callers are inventoried and the complete transport implementation
remains the next unit after this prerequisite.

## Validation and acceptance


Use the exact source copy of all pkg/retry files, testutil, go.mod and go.sum.
Enable failpoints only inside that disposable scratch module and always disable
in a trap; compare original bytes after cleanup. The repository wrapper assumes
a TiDB-owned package, so use its underlying failpoint controller on the isolated
external module instead. No shared worktree instrumentation is changed.

From the scratch module:

    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable pkg/retry
    go test -mod=readonly -race ./pkg/retry -count=1
    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable pkg/retry

From native root:

    cargo test --locked --lib source_retry_duration_doubling_matches_signed_go -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Publish native master only after these pass. From TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-pd-client --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: preserve Go PD retry value ownership'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go source/import/module/Bazel change requires bazel_prepare. Oracle .go.txt
inputs must be gofmt -s formatted. Preserve incoming commits, recheck their scopes
if either branch advances, and do not bypass hooks. Source files are not generated.
Retry tests are repeatable; preserve original source copies and disable failpoints
even on failure. No live-cluster/performance claim follows from deterministic tests.

## Surprises & Discoveries


The prior receipt documented unsigned duration restrictions but still described
complete package acceptance. This re-review explicitly closes that domain gap and
the value-copy gap. Copying the owner is essential to Go's RPC design; a shared
mutable execution lock is not equivalent. Signed retry doubling intentionally
wraps into negative waits; preserve Go rather than introducing saturation.

## Decision Log


2026-10-01: repair the complete retry dependency before grpcutil. Reusing the old
uncloneable owner would require a second retry implementation or serialize RPCs.
Native signed nanoseconds and Arc function identity preserve the source model.
Future transport work must copy the attached owner, not its Arc<Mutex> wrapper.

## Outcomes & Retrospective


All 136 focused PD cases pass. Original Go race/goleak and four source-oracle
extensions pass with failpoint cleanup and byte-identical source restoration.
The original saturation regression failed before replacement and passes now.
Final native library validation passes 1,512 tests with two existing ignored.
Strict Clippy, all-target compilation, formatting and all source/inventory/oracle
checks pass. Native publication and TiDB integration use the recorded gates. Complete transport/grpcutil, PD discovery,
root/TSO and broad routing still remain open. Existing finding counts do not change.

Reusable BackofferOption callbacks apply after normalization in source order.
The private logging option remains available to original tests; its old public
Rust-only builder is removed. To reproduce the additional oracle, copy
`doc/pd-retry-value-oracle/oracle_test.go.txt` into the scratch module as
`pkg/retry/native_value_test.go`. Set PD_RETRY_VALUE_ORACLE to an existing output
directory for the Go test, then compare intervals.json byte-for-byte with the
committed fixture. This fixture is generated from Go, never hand-edited.
