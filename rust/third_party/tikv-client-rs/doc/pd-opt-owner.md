# Centralize PD options before completing discovery

This living ExecPlan follows TiDB PLANS.md. The acceptance unit is all of pinned
PD opt, not just its dynamic settings. Full PD root/discovery/TSO remain open.

## Purpose and context


TiDB Go master 93a01d31f6da205ae4bf376825293903a6899fdb selects PD
v0.0.0-20260805103528-afa43111d149. Native baseline is 2fd0ecebadf0e8274a2b10ebfed729b03284a52b;
TiDB integration starts at 41330e49321c82f4a948dc7f308908b2d7222d16. Both were
refreshed and clean. Package opt consists of option.go and option_test.go; no
doc.go, generated inputs, platform source or fixtures exist. All package files,
shared test support, build inputs and parent callers are inventoried in
pd-opt-package.json. Native currently hard-codes the initialization attempt count
and has a scan-specific subset of Go's GetRegionOp. Centralize those declarations
under one complete options owner before adding discovery modes.

## Progress


- [x] Refresh branches/pins, read the complete package and original cases.
- [x] Record absence of the whole owner using original-case tests before adding it.
- [x] Implement all static/dynamic/request options and migrate existing subset callers.
- [x] Pass original Go tests, native focused/library/static gates and full inventory checks.
- [ ] Publish native, sync TiDB, validate adapters/lint and both locked server build gates; push.

## Design and milestones


Add src/pd/opt.rs with all source declarations and option constructors. Keep
static values writable before sharing Options through Arc; dynamic settings use
sequentially consistent typed atomics and one CAS like Go. Each notification
uses a capacity-one Tokio channel, coalesces pending updates and preserves a
receiver on cancellation. A short receiver mutex adapts Tokio's single-consumer
interface; no task is spawned. Constructor closures can be reapplied in order.
Retain shared map, backoffer and range-end identity using Arc with native locks for maps and the existing backoffer. Range-end bytes use a fixed Arc slice of AtomicU8: byte updates remain shared, and one alias cannot resize every other slice.
Go gRPC dial options are opaque to opt; retain native Endpoint closures in append
order. No transport-interceptor or full gRPC equivalence is claimed here.

Use standard Duration for nonnegative intervals/timeouts. Negative durations
cannot be expressed by that safe native API; concurrency and retry counts retain
signed machine-width values without extra validation. Dynamic interval validation preserves
0..10ms inclusive and nanosecond precision. A failed CAS is not retried.

Remove the old two-field RegionScanOptions struct. Keep its exported name only
as a type alias to the full GetRegionOp and migrate every current Rust caller to
the complete fields. Initialization obtains its source default retry limit from
Options; retain the existing explicit timeout adapter. Full configurable root
construction and consuming follower/router/concurrency settings stay with the
unaccepted root/discovery/TSO owners; this package stores and notifies policy.
Do not silently enable those unfinished modes.

Original TestDynamicOptionChange and TestOptions map to deterministic native
cases. Add coverage of every static/request constructor, repeated option
application, shared identity, all three bounded notification channels,
concurrency and canceled receives. Run original Go opt tests with race/goleak
from an isolated source copy. opt has no failpoints; no instrumentation required.

## Validation and acceptance


From native root, run:

    cargo test --locked --lib pd::opt -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

From the isolated PD source module:

    go test -mod=readonly -race ./opt -count=1

Publish native master, then from TiDB root run the maintained sync script:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared PD options'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/Bazel/module/import changes trigger bazel_prepare. Preserve incoming commits
using normal merges and rerun affected checks and publication gates. No force
push, generated-code hand edits, benchmark claims or deletion of active caches.

## Surprises & Discoveries


The original test comment says follower-handle has no notification, but production
SetEnableFollowerHandle sends one. Follow production and cover that channel too.
Go permits negative concurrency/retry counts at this storage layer. Do not add
validation absent from the package. Its interval setter makes one CAS attempt;
a concurrent winner can retain its value even when the losing setter returns nil.

## Decision Log


2026-10-01: complete opt as the next native prerequisite. A private list of flags
or a scan-only struct would perpetuate policy duplication. A complete data owner
is useful without claiming the behavior of the still-unaccepted parent services.
Public Rust callers constructing RegionScanOptions must use the full source
fields and defaults; the alias retains one owner, not a second implementation.

## Outcomes & Retrospective


The original Go race/goleak suite passes. All eight option tests, 103 focused PD cases and 1,478 native library tests pass (two pre-existing ignored). Strict Clippy, all targets, formatting and complete inventory checks pass. The red baseline is missing-package compilation, not a claimed runtime reproduction. Native publication/TiDB integration use the gates above. All 77 known findings retain their status.
No Linux, live PD or workload measurement is implied by this package.
