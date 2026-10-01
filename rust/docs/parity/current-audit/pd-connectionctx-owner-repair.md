# Complete PD connection-context owner and native TSO integration

## Source and acceptance unit


Freshly fetched TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb`
still selects PD client `v0.0.0-20260805103528-afa43111d149`. This repair covers
its complete `pkg/connectionctx` package: `manager.go` and `manager_test.go`,
all public operations, both original test cases and the TestMain leak gate.
There are no package-local generated/build/platform variants, fixtures, build
tags or doc.go. The [inventory](../../../third_party/tikv-client-rs/doc/pd-connectionctx-package.json)
also hashes all five testutil support files, including Linux/non-Linux variants,
shared module/build/license inputs and the reviewed TSO caller artifacts.
The [native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-connectionctx-owner.md)
records implementation and original-case mapping.

Native baseline is `5928b6e480b441496f9a3cd9bed6a7e8d56215a1`. TiDB starts at
`6b4f1a9d861d46930cdcdae554a80164ba58f3ce`, including the incoming partition-DDL
commit already present when work began. The complete native repair is published as
`4e3169ed93e433eab38638a1dd92a24f25f7caaf` on client-rust master. TiDB
validation and publication gates are recorded below. This is a
complete dependency leaf and its required native call-site integration;
PD root, TSO dispatcher and service discovery remain unaccepted.

## Root cause, ownership and removal


Native `Connection::reconnect` unconditionally created a new TimestampOracle
and retired the old one, including a healthy stream at the same leader URL.
Go `clients/tso/client.go::tryConnectToTSO` instead checks its shared connection
manager, retains the registered leader stream and collects stale URLs.
The fail-before loopback regression shows the healthy native stream restarting:
the mock's stream-local logical sequence returns 1 instead of continuing to 2.
This is stream-lifecycle evidence, not a claim that a real PD server returns
duplicate timestamps.

`src/pd/connectionctx.rs` implements the complete shared owner. One short
RwLock protects URL-to-Arc entries, and retrieval retains stream/context identity
without copying stream values. Store reports whether it accepted ownership.
Rejected candidates remain caller-owned and are not canceled by the manager.
Overwrite, Release, GC and ReleaseAll cancel entries before removal.
CleanAllAndStore cancels other URLs even when rejecting a duplicate at its own
URL. RandomlyPick uses Go's uniform reservoir-sampling algorithm with the native
random generator. The manager starts no task; cancellation callbacks and GC
predicates run under its write lock as in Go and must not re-enter it.

Cluster now owns this manager instead of a separately replaced oracle field.
Leader establishment returns the actual successfully dialed URL, rather than
assuming the first advertised address succeeded. Metadata refresh retains the
healthy matching stream; canceled contexts and changed leaders get replacements.
Failed metadata refresh leaves the current stream untouched. The cluster owner
explicitly releases all entries on drop. Retired oracle handles are retained
through worker joins, without holding a manager lock across the await; the
existing deadline and completed-result rules remain in force.

Changed native files are `src/pd/{connectionctx,cluster,mod,timestamp,timestamp_tests}.rs`
and the two connectionctx documents. TiDB receives their synchronized copies,
the sync log and current-audit/ExecPlan updates. No new dependency, runtime
setting, proxy mode or handwritten generated protocol is added. The existing
native single-leader mode and parent batching/retry policies remain explicit
parent-package obligations.

## Regression and validation evidence


From `/Users/qiliu/projects/client-rust`, before production changes:

    cargo test --locked --lib source_connectionctx_ -- --test-threads=1

The healthy-refresh regression failed with logical 1 instead of 2. After
implementation, the initial focused suite passed 59 tests. Final native
validation also adds the exact original shared-context case and a distinct
overwrite-lifetime control; the final suite results are recorded below.
Transport cases cover healthy reuse, changed leader, canceled same URL, failed
refresh, actual dialed URL and cancellation/join with retained pending work.
Manager tests cover every original assertion, rejected ownership, exclusive
duplicate cleanup, concurrent admission, retained non-clone streams, parent
cancellation, reuse after ReleaseAll and explicit resource release.

Unchanged pinned source in `/private/tmp/tidb-pd-connectionctx-go-source`:

    go test -mod=readonly -race ./pkg/connectionctx -count=1

Passed, including the source goleak TestMain. The package and selected support
path contain no failpoint calls; no instrumentation is required. Both production
and test source bytes are checked against the unmodified module cache. The
Linux support variant is reviewed but not executed on this macOS host.

Final native commands:

    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

TiDB commands from its repository root, after native publication:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets

Return to the repository root for:

    make lint
    python3 /private/tmp/check-pd-connectionctx.py --synced
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate Go PD connection-context ownership'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

The actual commit hook and fresh locked pre-push server build are mandatory.
No Go/import/module/Bazel/test-target changes occur, so bazel_prepare is not
triggered. Logs are `/private/tmp/pd-connectionctx-*.log`; the temporary checker
verifies package membership, artifact hashes, source-test identity, synchronized
native bytes and unchanged generated/lockfile inputs.

## Outcomes and remaining limits


Native final checks pass: **1,435 library tests**, zero failures and two
pre-existing ignored cases; strict Clippy, all-target compilation, formatting
and artifact validation pass. Native master publication succeeded. The maintained TiDB sync fetched exactly
that revision, applied all four patches and regenerated protocol outputs with
no generated-file or Cargo.lock changes. **10 bridge tests** and **26 PD tests**
pass, with one pre-existing ignored live-PD case. Transaction-crate all-target
compilation, root `make lint`, exact native-to-vendor source identity and
artifact/hash checks pass. The actual commit hook and fresh pre-push locked
server build remain required; the final publication response records their
results and the published TiDB commit.

The register stays **85 tracked / 77 unresolved / eight repaired**. P06 remains
partial: public PdRpcClient.close still does not retire the PD owner, and the
complete parent lifecycle is open. P03 service-mode discovery and P07 metadata
RPC serialization are not repaired by this leaf. Earlier source-continuity
snapshots retain their original pins; this receipt records subsequent changes.
No live PD failover, TLS/microservice validation, Linux run or workload benchmark
is claimed. Reusing a healthy stream removes needless restarts; throughput and
sysbench/TPC-C/TPC-H/YCSB performance remain unmeasured.
