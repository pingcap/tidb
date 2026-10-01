# Complete PD retry ownership and initialization integration

## Source, inventory and acceptance


Fresh TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb` selects PD client
`v0.0.0-20260805103528-afa43111d149`. This unit implements its entire `pkg/retry`
package: `backoff.go`, `interval_retry.go`, and `backoff_test.go`. No doc.go,
generated source/input, platform/build-tag production variant, fixture or
package build artifact exists. The
[native inventory](../../../third_party/tikv-client-rs/doc/pd-retry-package.json)
hashes every package artifact, all five test-support artifacts (including
Linux/non-Linux support), module/build/license inputs and all source consumers.
It records every mapping and Rust adapter decision. The
[native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-retry-owner.md)
retains the implementation and validation procedure.

Native starts from `bcf74b7282b01372f93fb814ba601eb4c22b5d12`, TiDB from
`5a01a45d918aaae59fa9bac1f43521460d3a83cd`. Both were clean and refreshed before
implementation. Native master now publishes
`2fd0ecebadf0e8274a2b10ebfed729b03284a52b`. This accepts the retry dependency,
not PD root, `servicediscovery`, `clients/tso`, `opt` or all client-go packages.

## Root causes and changes


Native initialization gave up on its first failed membership probe. Go
`serviceDiscovery.initRetry` retries with `MaxRetryTimes` (default 100) and a
one-second ticker. Native `Connection::connect` also ignored its timeout, so a
stalled GetMembers request could prevent any retry from progressing.

`src/pd/backoff.rs` supplies the complete shared policy: exponential base/max/
total normalization, checker preservation/replacement, warning cadence and
function/attempt/error fields, one lazily initialized resettable timer, final partial wait,
unbounded zero-total mode, deferred state reset, context attachment and the
source failpoint probe. The fixed retry helper and its default ten/500ms wrapper
retain ticker phase, skip missed ticks, wait after the final failed attempt and
handle zero attempts/invalid intervals like Go's representable inputs.

The two helpers intentionally differ on cancellation. Exponential Exec returns
the owning context's error; fixed retry returns the last operation error. Both
invoke the operation first. Rust takes the context completion as a future that
returns its exact native error; it does not introduce another context owner.
Metadata attachment reuses TraceContext and retains Arc identity. RAII resets
state on a dropped execution future, extending Go's deferred cleanup to Rust
cancellation. Function names follow Rust's type naming; Go runtime reflection
and stack-wrapper spelling are not copied into native errors. Unsigned native
durations/counts cannot express negative Go inputs; saturating arithmetic avoids
Rust overflow outside meaningful PD timing values.

`RetryClient::connect` now uses the shared fixed policy for default initialization,
retaining the successful Cluster for publication. `Connection::connect` gives
dialing and GetMembers one absolute deadline and sends only its remaining time
in the request timeout. These are the only production construction/probe callers;
existing loopback construction uses the same bounded probe. No new dependencies,
protocol schema or generated outputs were added. The root per-RPC reconnect loop,
configurable initialization options, service modes and public close propagation
remain obligations of their full parent owners; they are not silently certified.

Changed native files are `src/pd/backoff.rs`, `backoff_tests.rs`, `mod.rs`,
`retry.rs`, `cluster.rs`, `timestamp_tests.rs`, and the two linked documents.
TiDB receives those exact sources through its maintained sync script, plus the
sync log and current plan/audit receipts.

## Failing evidence and validation


Before production edits, two new loopback regressions fail:
`source_retry_initialization_recovers_from_a_failed_member_probe` receives the
first unavailable result instead of recovering; and
`source_retry_member_probe_honors_its_timeout` outlives a 500ms test deadline
despite a 50ms PD timeout. `/private/tmp/pd-retry-red.log` preserves both failures
and the passing existing retry-configuration control.

Self-review adds two red/green timer cases. Immediate success without a Tokio
runtime initially panicked because exponential Exec constructed its timer before
any failure; the timer now lives in pinned optional storage and is created only
when needed. A 1ms fixed interval with a 3ms operation initially completed three
failed attempts at 3ms instead of 5ms: Tokio Interval's Skip mode permits bursts
below its five-millisecond lateness threshold. Fixed retry now resets one Sleep
using Go runtime's original-phase/missed-tick arithmetic. Both original long-
interval and small-interval cases pass. Logs are
`/private/tmp/pd-retry-lazy-timer-red.log` and `/private/tmp/pd-retry-ticker-red.log`.
The source Go package is unchanged; final native gates were repeated. Native
initial package commit 043e71e and timer follow-up 2fd0ece are both preserved.

From native client-rust:

    cargo test --locked --lib source_retry_ -- --test-threads=1
    cargo test --locked --lib immediate_completion_does_not_construct_a_retry_timer -- --test-threads=1
    cargo test --locked --lib fixed_interval_keeps_ticker_phase_after_slow_operations -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check
    python3 /private/tmp/check-pd-retry.py

All **95 focused PD cases** and **1,470 native library tests** pass, with two
pre-existing ignored library cases. Strict Clippy, all-target compilation,
formatting and complete artifact/hash checks pass. Original TestBackoffer and
TestBackofferWithLog assertions are retained; extra cases cover reset/drop,
context identity/deadlines, elapsed operation time, ticker phase, final waits,
default policy and failpoint observation.

Original source tests run in `/private/tmp/tidb-pd-retry-go-source`:

    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable pkg/retry
    go test -mod=readonly -race ./pkg/retry -count=1
    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable pkg/retry

The command wrapper installs a cleanup trap. The original race/goleak suite
passes; all source bytes match the pinned module after instrumentation removal.
The first attempt found copied directories read-only; only this disposable copy
was made writable before the successful rerun. No module-cache source changed.

After native publication, from TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets

Then from TiDB root:

    make lint
    python3 /private/tmp/check-pd-retry.py --synced
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared Go PD retries'
    TERM=xterm git -c core.hooksPath=hooks commit --amend --no-edit
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/module/import/Bazel/test-target change triggers bazel_prepare. The sync
fetched exactly 2fd0ece, applied all four maintained patches and regenerated
protocol outputs without generated-file or Cargo.lock changes. All 10 bridge and 26 PD tests pass, with one pre-existing ignored live-PD
case. The first PD-suite attempt was denied a loopback bind by the sandbox;
the network-enabled rerun passes. Transaction-crate all-target compilation,
root lint, exact native/vendor identity and artifact/link checks pass. An initial local integration commit passed its actual hook before timer self-review.
Final resynchronization, adapter/lint checks, all-target compilation and source
identity checks pass. The final amendment repeats the actual hook; a separate
locked server build after that amendment gates push; their
results and the final TiDB commit are recorded in the publication response. Logs are `/private/tmp/pd-retry-*.log`.

## Outcomes and limits


P06 remains partial; P03 and P07 stay open. The register remains **85 tracked /
77 unresolved / eight repaired**. This package does not resolve whole discovery,
TSO, PD root, metadata concurrency or shutdown ownership. Live PD/microservice
topology, Linux and sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. No throughput
or latency improvement is claimed.

Disk maintenance reclaimed **4.11 GiB** from 13 ignored TiDB incremental-cache
directories whose complete contents were older than 24 hours. Process inspection
confirmed no cargo/rustc process was active before deletion. Sources, final
binaries, logs and uncommitted work were preserved. Manifest:
`/private/tmp/pd-retry-cache-candidates.json`.
