# Complete explicit shutdown of the existing native PD owner

## Purpose and scope


Follow the pinned Go store/PD close lifecycle in the existing Rust owner. A
retained native PdRpcClient must stop timestamp work and metadata admission on
close, and shutdown must join background work even when callers overlap or an
async close is interrupted. This is maintenance of the existing owner under
P06, not transcreation or acceptance of the complete PD root/discovery/TSO
packages. Service-mode discovery and other P06 obligations remain open.

Both freshly pulled trees are clean and unchanged: TiDB hparser-integration
74f36495062a97c04916031e286a012fdb05e0dc; native client-rust master
952013279bc64e590f17c18b9c9222fdaf5a3604. Go master is
93a01d31f6da205ae4bf376825293903a6899fdb. Its client-go dependency is
v2.0.8-0.20260928031501-8edb23f6c7ee, PD client is
v0.0.0-20260805103528-afa43111d149. The PD root has no doc.go.

## Progress


- [x] Pull both repositories and trace source close order and native ownership.
- [x] Reproduce pending TSO, concurrent close, interrupted close and cache-RPC cancellation failures (four red/green cases).
- [x] Repair the existing owner and all close callers; six focused tests, 157 PD tests, 89 cache tests, strict Clippy and all-target compilation pass in the disposable candidate.
- [x] Validate and publish native master 19a56ccda1e128218cd33c69709038219aced9bc; remote SHA equality and clean tree verified.
- [x] Synchronize the maintained TiDB copy and validate its consumers: 96 cases pass, one existing ignored; all-target compilation, root lint and audit consistency pass.
- [x] Update P06 evidence and run lint; the actual commit hook passes the locked server build. Final publication is enforced by the fresh-build/push/remote-verification chain below.

## Design and milestones


Go tikv/kv.go KVStore.Close joins the region cache before TiKV and PD clients.
PD client.go Close delegates to inner_client.go close, which cancels the owner,
joins workers, closes TSO and closes discovery connections. clients/tso/client.go
Close joins TSO workers and dispatcher. Rust async close must retain unfinished
joins when its future is dropped; a boolean saying close started cannot mean
close completed. Preserve the existing region-before-client order.

First extend src/pd/timestamp_tests.rs's local tonic fixture and existing client
close tests. Run the new regressions against unchanged production code. Next
connect public close to the native RetryClient/Cluster lifecycle. Stop request
and reconnect admission, cancel discovery, join current and retired TSO streams,
and release connection handles. Keep request futures outside cluster locks as
P07 requires. An interrupted refresh must not lose a retired worker's join.
Preserve cluster identity after shutdown. Retained request handles cannot restart
closed owners. Test concurrent/interrupted close and stalled network operations.

After native validation, commit/push HEAD:master in /Users/qiliu/projects/client-rust.
From TiDB root run bash rust/scripts/sync-tikv-client-rs.sh. Verify maintained
patches and generated artifacts, update P06 without closing its broader claim,
and commit via TERM=xterm git -c core.hooksPath=hooks commit. After the final
commit rerun cd rust && cargo build --locked -p tidb-server immediately before
pushing HEAD:hparser-integration; verify remote SHAs and clean working trees.

## Validation


From native repository run cargo test --locked --lib source_pd_shutdown_ before
and after the fix, then cargo test --locked --lib pd:: and the affected cache
lifetime tests. Run cargo test --locked --lib -- --test-threads=1 because shared
close methods have store and transaction callers. Run cargo clippy --locked
--lib -- -D warnings, cargo check --locked --all-targets, cargo fmt --all --check
and git diff --check. Use bounded local transport tests, not a live PD claim.

In TiDB rust/ run cargo test --locked -p tidb-pd-client --lib --test all --
--test-threads=1, cargo test --locked -p tidb-txnkv --test all pd_region_loader_source,
and cargo check --locked -p tidb-pd-client -p tidb-txnkv -p tidb-server --all-targets.
Run root make lint. No Go/Bazel build inputs are planned, so bazel_prepare is
not required. Record exact final commands, source checks and results below.

## Surprises & Discoveries


Public PdRpcClient.close currently returns early once its closed flag is set,
so a concurrent caller can return before the first closes anything. RegionCache
close takes the task vector before awaiting; cancelling close detaches those
handles. These must be repaired along the public PD close path, rather than
adding a call that only stops an idle timestamp stream.

## Decision Log


Use the existing owner and shared async completion to model Go's joined close.
Do not add a shutdown thread or rely on final Arc drop: retained handles are
allowed and must not retain live workers. Reconnect's separately serialized
publication must be coordinated with close.

## Outcomes & Retrospective


The existing native explicit-shutdown boundary is repaired and published as
19a56cc; TiDB synchronization and consumer validation pass. The actual integration pre-commit hook passes the locked server build; the
final publication chain below enforces the fresh post-commit build before push. Counts remain 85 tracked, 73 unresolved,
12 repaired; P06 remains partial. Live failover, service-mode switching and
sysbench/TPC-C/TPC-H/YCSB performance are not established by this work.

## Implementation evidence


Automatic approval review rejected the initial broad replacement script. No
production edit from that script was applied. A disposable full source copy at
/private/tmp/client-rust-pd-shutdown-review was used to inspect an exact diff
and validate it. That caught an overbroad field insertion in the candidate and
an explicit pool-test adapter requirement, both corrected there. Applying the
reviewed diff with git apply --check was approved after regression/static
validation; both repositories remain on the authorized branches.

The first three new tests failed against native production 9520132 with no
production edits. The cache-RPC case independently failed in the candidate
before its additional cancellation fix. Native logs are
/private/tmp/native-pd-shutdown-red.log and
/private/tmp/native-pd-shutdown-cache-red.log. All six then pass in
/private/tmp/native-pd-shutdown-review.log. The exact candidate PD/cache/static
commands are the validation commands above, with outputs in
/private/tmp/native-pd-shutdown-review-{pd,cache,clippy,check}.log.

The owner uses an async OnceCell for public close completion and a retained
shared future for region-worker joins. Built-in worker registration shares the
shutdown task lock, and network waits observe the worker cancellation. PD
request/retry/discovery cancellation includes retry sleeps. One reconnect lock
serializes leader publication and close. Cluster retains retired TSO streams
until completion, drops metadata/keyspace clients at close, and retains the
cluster ID. Joining current/retired streams remains outside the cluster lock.
The PD cancellation check adds no boxed async-trait future per request.

Go module verification used an isolated writable copy at
/private/tmp/pd-shutdown-go. The packages contain failpoint references; enable
and cleanup covered only that copy. The exact command, run there, was:

    bash -c 'set -e; trap '\''/Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable . '\'' EXIT; /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable .; GOTOOLCHAIN=go1.25.14 go test -race . -run "^(TestClientCtx|TestClientWithRetry)$" -count=1; GOTOOLCHAIN=go1.25.14 go test -race ./clients/tso -count=1'

The two original root context/initialization cases pass (9.047 s); the complete
original clients/tso suite passes (8.844 s), with race detection and TestMain
leak verification. Log: /private/tmp/go-pd-shutdown.log. This is source-contract
verification, not a live Go/Rust cluster shutdown comparison.

## Native publication


After exact-patch application, cargo test --locked --lib -- --test-threads=1
passes 1,534 cases (two existing ignored) in 86.20 seconds. Strict library
Clippy, all-target cargo check, cargo fmt --all --check and git diff --check
pass. Logs are /private/tmp/native-pd-shutdown-{all,clippy,check}.log. The
complete suite covers all six focused regressions and existing PD/cache/store/
transaction callers, so no second full sweep was necessary. Native master
19a56ccda1e128218cd33c69709038219aced9bc is committed and pushed; git ls-remote
confirms its exact identity and git status --short is empty.

## TiDB integration validation


From TiDB root bash rust/scripts/sync-tikv-client-rs.sh synchronized native
19a56ccda1e128218cd33c69709038219aced9bc. All four maintained patches apply;
protobuf regeneration and Cargo.lock produce no diff. Four changed Rust files
and the native receipt match upstream byte-for-byte. client.rs differs only
by the existing 030 tonic-0.14 codec import/call patch. The initial byte-equality
check caught this expected adapter, which was compared against that patch.

From /Users/qiliu/projects/tidb/rust these exact commands pass:

    cargo test --locked -p tidb-pd-client --lib --test all -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test all pd_region_loader_source
    cargo test --locked -p tidb-txnkv --test all region_topology_source
    cargo check --locked -p tidb-pd-client -p tidb-txnkv -p tidb-server --all-targets

The first command passes 28 library and 46 integration cases (one existing
ignored library case); loaders pass 15 and topology passes seven. Thus 96
consumer tests pass. Existing unrelated compiler warnings remain unchanged.
Logs are /private/tmp/tidb-pd-shutdown-{pd,loader,topology,check}.log.
From repository root make lint and git diff --check pass. The JSON register's
85 unique IDs, statuses and finding/evidence/Go-owner fields match Markdown: 67
open, six partial, twelve repaired. No Go or Bazel build inputs changed, so
bazel_prepare was not triggered. Root lint log is
/private/tmp/tidb-pd-shutdown-lint.log.

The disposable native target was cleaned after native-repository validation;
du reported 8.0 GB physical allocation before cleanup. Cargo reported 6,708
files / 10.7 GiB logical bytes removed. Source evidence, logs, repository builds
and dependencies are preserved. After cleanup and integration builds, df shows
394 GiB available.

## Integration publication gates


Stage only this repair's twelve files and use the actual repository hook:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: complete native PD shutdown ownership'

The hook must pass cd rust && cargo build --locked -p tidb-server. After the
final commit or any amendment, run this fresh build/push/verification chain
from TiDB root; any failure stops publication:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration && git rev-parse HEAD && git ls-remote origin refs/heads/hparser-integration && git status --short

No service-mode, full parent-package, cross-platform, live multi-node failover
or sysbench/TPC-C/TPC-H/YCSB performance acceptance is claimed. The generic
PdRpcClient close path now requires its PD adapter to implement the existing
RetryClientTrait; native and all checked consumers compile, and pool-only test
adapters explicitly declare that they have no PD RPC behavior.

The actual pre-commit hook succeeded; /private/tmp/tidb-pd-shutdown-commit.log
records the locked server build and initial integration commit. The receipt
completion amendment also uses TERM=xterm git -c core.hooksPath=hooks commit
--amend --no-edit, then the fresh locked build runs on that final commit and
only on success permits push and remote verification. These logs are
/private/tmp/tidb-pd-shutdown-amend.log and
/private/tmp/tidb-pd-shutdown-prepush.log. Both native and integration remote
SHAs and clean working trees must be verified by the final publication chain.
