# Complete PD deadline prerequisite and native TSO integration

## Source and acceptance unit


TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb` selects PD client
`v0.0.0-20260805103528-afa43111d149`. This repair implements the complete
`github.com/tikv/pd/client/pkg/deadline` package: `watcher.go` and
`watcher_test.go`, including NewWatcher, Start, Watch, every TestWatcher assertion
and the TestMain lifetime gate. There are no package-local generated inputs,
fixtures, build files, doc.go, build tags or platform-specific production sources.

The [native inventory](../../../third_party/tikv-client-rs/doc/pd-deadline-package.json)
records both source artifacts, their hashes, both timer-adapter artifacts, all
five test-support files including Linux/non-Linux variants, and shared
module/license/build inputs. The [native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-deadline-owner.md)
records integration decisions, original-case mapping and red/green evidence.
This is one complete dependency package plus required caller integration. It is
not acceptance of the PD root, service discovery, the TSO dispatcher or all
client-go packages.

Native baseline is `6f663b396552eec6d1bfad76b65f813e317884a4`; the reviewed
repair is published as `5928b6e480b441496f9a3cd9bed6a7e8d56215a1` on
ngaut/client-rust master. TiDB integration starts at
`33a8f2e380e92e52310282579c42c92131ebc757`. The maintained synchronization script
fetched exactly that native revision, applied all four compatibility patches
and regenerated protocol outputs. Generated files and Cargo.lock have no diff.
No independent TiDB deadline implementation is added.

## Behavior and removal


One serial watcher starts each timer before bounded admission. It preserves
zero-capacity rendezvous, explicit batch completion, caller cancellation during
admission, and watcher-parent cancellation afterward. Timeout invokes the
provided cancellation callback. Dropping the completion handle does not disarm
a deadline. A Tokio monotonic expiry instant replaces Go pooled timer objects;
there is no stale timer channel to drain. Callback execution is outside the
queue lock. Close joins the retained watcher; final-owner drop cancels it.

The native TSO owner starts this deadline before yielding each batch to tonic,
so it covers response headers and body. Batch completion disarms the deadline
without closing the shared stream. Stream failure/timeout releases pending
requests and closes the watcher. The oracle retains its worker; replacement
joins the old owner, and dropping its last handle retires a stalled stream.
The previous manual AtomicWaker/poll implementation and discarded worker handle
are removed. Existing native batching bounds remain a parent-package obligation.

The shared cancellation adapter also had a lost-wakeup window: cancellation
could notify after the state check but before waiter registration. A deterministic
per-instance test hook reproduces it. The adapter now registers/enables its wait
before checking state; both direct and inherited cancellation keep their existing
API and error identity. This dependency correction is necessary for reliable
watcher shutdown. No global test hook or new cancellation policy is introduced.

Completed timestamp results survive stream retirement. A fail-before regression
caught a race between a delivered result and the new stream cancellation signal;
the request receive path now preserves an already-delivered result, as Go's
separate request/stream lifetimes require.

## Validation


Original unchanged Go source in `/private/tmp/tidb-pd-deadline-go-source`:

    go test -mod=readonly -race ./pkg/deadline -count=1

Passed, including the source goleak TestMain. The package and selected test path
contain no failpoint calls. The Go module cache was not modified.

From `/Users/qiliu/projects/client-rust`:

    cargo test --locked --lib source_tso_ -- --test-threads=1
    cargo test --locked --lib source_tso_completed_result_survives_stream_retirement -- --test-threads=1
    cargo test --locked --lib cancellation_cannot_be_lost_between_check_and_wait
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The first three are red runs before their respective corrections. A configured
20 ms timeout previously exceeded the 500 ms outer test bound for stalled headers
and bodies; both now return an error. Live-stream completion is a passing control.
The completed-result and cancellation-registration tests also failed before
repair. Final full library validation passes **1,422 tests, zero failures and
the same two pre-existing ignored cases**; no new case is ignored. Native Clippy,
all-target compilation and formatting pass. Additional cases cover blocked and
zero-capacity admission, elapsed queued deadlines, shutdown, discarded done
handles, 256 concurrent allocations and active stream retirement.

TiDB commands from repository root unless scoped below:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets

Return to the repository root for:

    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate the complete native PD deadline owner'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

TiDB validation passes: **10 client-bridge tests**, **26 PD-adapter tests with
one pre-existing ignored case**, transaction-crate all-target compilation and
`make lint`. Source/input hashes and native-to-vendor source identity were also
verified; no generated files or lockfile changed. The final response records
the actual commit-hook and fresh pre-push locked build results; previous builds
or unchecked hook bypasses do not qualify. No Go/import/Bazel/test-target
changes occurred, so make bazel_prepare is not triggered.

## Remaining work and limits


P06 advances to partial. Public PdRpcClient.close still needs to retire the PD
owner, and the full root/discovery/retry/request-context lifecycle is unaccepted.
P03 service-mode discovery and P07 metadata serialization remain open. The known
register therefore stays **85 tracked / 77 unresolved / eight repaired**.
The prior source-continuity snapshot retains its original baseline; this receipt
records the subsequent changed native/vendored paths and exact source revision.

No live PD/TiKV failover, TLS/microservice test, Linux execution or workload
benchmark is claimed. Existing parent-package gaps, batching policy and ignored
unrelated native cases remain open. Sysbench/TPC-C/TPC-H/YCSB performance is
unmeasured; this change provides bounded waiting and owned cleanup.

Disk maintenance removed ten ignored TiDB incremental-cache directories whose
contents were all older than 24 hours, after verifying no compiler was active.
They measured about 5.3 GiB before deletion. Source, final build outputs, test
logs and uncommitted work were preserved. Logs for this repair are under
`/private/tmp/pd-deadline-*.log`.
