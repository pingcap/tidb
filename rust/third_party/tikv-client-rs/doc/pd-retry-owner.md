# Own PD retries before completing discovery

This living ExecPlan follows TiDB PLANS.md. Complete pinned PD pkg/retry is the
atomic implementation/acceptance unit. PD root, discovery, TSO and options
remain open; this dependency is necessary for their shared lifecycle.

## Purpose and context


Fresh TiDB master 93a01d31f6da205ae4bf376825293903a6899fdb selects PD client
v0.0.0-20260805103528-afa43111d149. Native starts at bcf74b7282b01372f93fb814ba601eb4c22b5d12;
TiDB integration starts at 5a01a45d918aaae59fa9bac1f43521460d3a83cd. Both branches
were refreshed and clean. Package membership is exactly backoff.go,
interval_retry.go and backoff_test.go. No doc.go, generated inputs/artifacts,
platform variants, fixtures or package build files exist. The JSON inventory
hashes all artifacts, five shared support files, module/build/license inputs and
all source consumers. It describes each implementation and integration decision.

Native PD initialization currently returns its first failed membership probe.
Go serviceDiscovery.initRetry uses MaxRetryTimes (default 100) and a one-second
ticker. Native membership probes also ignore their configured timeout, so a
retry policy alone cannot recover from a stalled response. Bound those probes
and migrate initialization through the complete common retry owner.

## Progress


- [x] Refresh native/TiDB/master, inspect all package source/test artifacts and source callers.
- [x] Reproduce both transport defects with loopback tests on unchanged production code.
- [x] Implement the whole retry owner and original cases; integrate default initialization and bounded membership probes.
- [x] Pass original Go race/goleak with failpoint enable/disable and byte-for-byte source restoration.
- [x] Pass native focused/full tests, strict Clippy, all targets, formatting and inventory checks.
- [ ] Publish native master, synchronize TiDB, validate adapters and root lint, commit through actual locked server hook, rerun locked build and push.

## Plan of work and milestones


First extend the existing loopback server in src/pd/timestamp_tests.rs with
finite membership failures and a stalled member response. Retain the two
failures before production edits. A single transient failure must eventually
initialize; a stalled member probe must finish inside the requested deadline.

Implement src/pd/backoff.rs and backoff_tests.rs as one complete Go package:
exponential base/max/total normalization, optional checker and overwrite,
warning cadence, original error identity, one lazily initialized reusable timer, budget spent only
on completed waits, deferred reset, context attachment and failpoint probe.
A done future carries the owning context error; reuse TraceContext rather than
creating another context type. Fixed interval retry has a ticker anchored at
entry, drops missed ticks, waits after the final failure, and returns the last
operation error on cancellation. Default microservice policy is ten/500ms.

Integrate RetryClient::connect through shared fixed-interval retry with Go's
default 100/one-second initialization policy. Retain a successfully constructed
Cluster for publication without changing metadata retries or streaming policy.
Connection::connect bounds the complete dial/probe future and sets the request's
wire timeout. This fixes the ignored parameter and lets the retry owner progress.

Run the original Go package in a disposable source copy, enabling only that
copy's failpoints and disabling in a trap. Compare restored hashes. Then run
focused/all native checks. On success publish native, use TiDB's maintained
sync script, verify exact vendored identity, run adapter suites/lint and both
required locked server gates. No generated output is hand-edited.

## Validation and acceptance


Native root commands:

    cargo test --locked --lib source_retry_ -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The two red failures are /private/tmp/pd-retry-red.log. Source tests run from
/private/tmp/tidb-pd-retry-go-source with scoped failpoint instrumentation:

    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable pkg/retry
    go test -mod=readonly -race ./pkg/retry -count=1
    /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable pkg/retry

Always restore source even after a failed test. No Go/Bazel/module/import/test
changes are made in TiDB, so bazel_prepare is not required. After native push,
from TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets

Then from TiDB root:

    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared Go PD retries'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

## Surprises & Discoveries


The two source retry helpers intentionally return different errors when canceled:
Backoffer.Exec returns ctx.Err; Retry returns the last operation error. They both
invoke the operation before checking cancellation. A sleep after each attempt
would drift fixed retry intervals when operations take time, and dropping a
Rust future needs RAII reset in addition to normal return handling. Self-review
also reproduces eager timer allocation on immediate success and Tokio Interval
catch-up bursts below its five-millisecond missed-tick threshold. Pin optional
timer storage lazily and use Go runtime ticker phase arithmetic with a reusable
Sleep; both additional regressions fail before and pass after correction.

## Decision Log


Use one complete PD retry package and source initialization timing. Do not
replace client-go's distinct KV retry budget or relabel the existing PD root
request/reconnect loop as accepted; its service-discovery-owned replacement
must migrate with the full parent. Reuse native error/context/timer primitives.
Do not change unrelated initialization/service variants or introduce a public
shutdown method from an incomplete owner. Rust type names replace runtime Go
reflection for diagnostics; their spelling is language-specific.

## Outcomes & Retrospective


The complete retry package passes its original Go race/goleak tests, 95 focused
native PD cases and 1,470 library tests (two existing ignored). Strict Clippy,
all-target compilation, formatting and complete inventory/source-restoration
checks pass. Both transport regressions and both timer self-review regressions fail before
and pass after repair. Final focused/full/static gates were repeated after the
timer corrections. Initial native publication is 043e71e; its follow-up retains
Go timer activation and exact missed-tick semantics.
Native publication and TiDB integration still use the gates above. All 77 known findings remain open in their existing
workstream assignments. No live PD, Linux, sysbench, TPC-C, TPC-H or YCSB result
is claimed by this deterministic dependency repair.


## Revision — 2026-10-01 value ownership and signed domain


The grpcutil review invalidated the earlier unrestricted acceptance claim: the
previous implementation covered nonnegative timing but replaced signed wrapping
with saturation and could not copy its retry callback as Go does per RPC. See
`pd-retry-value-ownership.md` for the complete package re-review and replacement
evidence. Shared Arc callback identity, copied scalar state, ordered reusable
constructor options and signed inputs now replace those restrictions. The public
hidden logging builder and Go-absent Duration::MAX saturation assertion are
removed; original valid cap behavior/tests remain. Historical validation above
remains evidence for that earlier revision, not proof of the newly checked domain.
