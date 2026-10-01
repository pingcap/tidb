# Complete PD batch controller and default native TSO integration

## Source and acceptance unit


Fresh TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb` selects PD client
`v0.0.0-20260805103528-afa43111d149`. This repair covers the complete
`pkg/batch` package: `batch_controller.go`, `batch_controller_test.go`, every
production operation, all three original test cases and TestMain leak checking.
There is no doc.go, generated source/input, package build file, fixture, build
tag or platform-specific production file. The
[native inventory](../../../third_party/tikv-client-rs/doc/pd-batch-package.json)
also hashes all five testutil support artifacts, shared module/build/license
inputs and referenced TSO/router/options/metrics inputs. Its parent packages
remain unaccepted. The [native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-batch-owner.md)
records implementation, source mapping, validation and publication gates.

Native baseline is `4e3169ed93e433eab38638a1dd92a24f25f7caaf`; TiDB starts at
`8bc2354d8fa85f180c121abe71a5e110a6cbedde`. Both branches were current at the
initial pull. The reviewed native repair is published on client-rust master as
`bcf74b7282b01372f93fb814ba601eb4c22b5d12`.

## Root cause and replacement


Native TSO used a private drain loop with a 64-request maximum and a semaphore
allowing 65,536 outstanding batches. Go's default dispatcher creates a
20,000-entry request queue/controller and supplies one RPC token. Go can collect
requests while waiting for that token; token ownership then follows the batch
until completion. Changing only the size constant would preserve the wrong
collection, completion, timing and cleanup lifecycle.

`src/pd/batch.rs` supplies the whole shared controller: optional token and first
request admission, bounded drains, extra waiting, caller-timer collection,
borrowed request views, short-circuit iteration, indexed finish callbacks,
default-finisher fallback, additive target adjustment and observation before
adjustment. The token-late full-batch return preserves the previous extra-start
time, including the initial zero value. Pending-fetch failure finishes owned
requests and returns the token; timer-only failure preserves caller ownership.

Rust adapters use the existing Cancellation, mpsc, monotonic timers and owned
semaphore permits. Dropping a pending-fetch future performs the same cleanup
as cancellation, with token return before callbacks. Source channels remain
open for their owners' lifetimes; Rust channel closure is terminal rather than
synthesizing a zero request. A zero Duration represents nonpositive extra wait.
Rust request values need not be Clone: completion moves them, releases their
resources and retains the buffer allocation. Original retained-pointer tests
use Arc. The source initial target of eight and max+1 buffer bound are retained;
actual TSO/router maxima are above eight.

Native `timestamp.rs` replaces the old drain loop and outstanding-batch policy
with this controller and Go default queue/token values. Each in-flight batch
owns its controller until completion. A short pool lock reuses the completed
empty buffer; it never crosses an await or callback. With one RPC token, buffers
are bounded to the in-flight batch and next collector. Success and discarded
pending batches both return the token before completing requests. The existing
deadline, connection-context and completed-result ownership remains in place.
Timestamp subtraction retains its original order to avoid a new intermediate
signed overflow. The best-size observer uses the source default metric name,
help and buckets through the existing Prometheus dependency.

Changed native paths are `src/pd/{batch,batch_tests,mod,timestamp,timestamp_tests}.rs`,
`src/stats.rs`, and the two batch documents. TiDB receives synchronized copies,
the sync log and its current-audit/ExecPlan updates. No new dependency, public
configuration setting or generated protocol is introduced. Dynamic TSO
concurrency/pacing, option variants, router integration and complete metrics
remain parent-package obligations.

## Reproduction and validation


Before production changes, from `/Users/qiliu/projects/client-rust`:

    cargo test --locked --lib source_batch_ -- --test-threads=1

Two new regressions failed; five existing controls passed. A prequeued 20,001
request workload yielded a 64-request first batch instead of 20,000, and a
second RPC proceeded before the first completed. After replacement the same
workload yields batches of 20,000 and one, preserves every timestamp result,
and the next batch waits for the first token. This is deterministic batching
evidence, not measured sysbench/TPC-C/TPC-H/YCSB throughput.

Self-review also added and ran this regression before correcting discard order:

    cargo test --locked --lib source_batch_discard_returns_rpc_token_before_request_completion -- --test-threads=1

It failed because the callback saw no available RPC token. The final field-drop
and success-callback order returns the token first. A separate test verifies
the same allocation is reused after completion without retaining old senders.

Unchanged pinned source in `/private/tmp/tidb-pd-batch-go-source`:

    go test -mod=readonly -race ./pkg/batch -count=1

Passed, including goleak. The leaf and used support path have no failpoint calls.
No source instrumentation is needed; the module cache is unchanged and both
source artifacts are byte-checked. Linux/non-Linux support variants are
inventoried; Linux execution is not claimed on this macOS host.

Native validation commands:

    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The focused suite passes 78 cases, including all original assertions and extra
token/timer/cancellation/closure/ownership branches. The final library run passes **1,453 tests** with two pre-existing ignored
cases. Strict Clippy, all-target compilation and formatting pass. The original
Go race/goleak suite and complete artifact checks also pass.

TiDB commands from repository root after native publication:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver::client_bridge -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets

Return to repository root:

    make lint
    python3 /private/tmp/check-pd-batch.py --synced
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate Go PD batch ownership and default TSO limits'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/import/module/Bazel/test-target changes are planned, so bazel_prepare is
not triggered. The checker verifies complete package membership, all source
hashes, unchanged Go test source, exact native/vendor identity, documentation
links, finding counts and stable generated/lockfile inputs. Logs are under
`/private/tmp/pd-batch-*.log`. The actual hook and fresh pre-push locked build
must both pass; the final publication response records their outcomes.

## Outcomes and limits


Native validation and master publication pass. The maintained TiDB sync fetched
exactly bcf74b7, applied all four patches and regenerated protocol outputs with
no generated-file or Cargo.lock diff. **10 bridge tests** and **26 PD tests**
pass, with one pre-existing ignored live-PD test. Affected transaction-crate
all-target compilation, root `make lint`, artifact/hash checks, documentation
links and exact native/vendor identity pass. The actual hook and fresh pre-push
locked server builds passed for local integration commit b41949f774, but its push
was rejected because the remote advanced. The merge described below repeats
both gates before publication; the final response records the published commit.

The register remains **85 tracked / 77 unresolved / eight repaired**. P06 is
partial. Default batch collection is repaired, but public PD close, full
dispatcher request/error/option handling, discovery and metadata concurrency
remain W01 obligations. This leaf does not close P03 or P07 or certify the full
PD, client-go or TiDB packages. Earlier source-continuity snapshots keep their
original pins; this receipt records subsequent changes. Real PD failover,
TLS/microservice mode, Linux and workload benchmarks remain unverified.

Disk maintenance removed ten ignored TiDB incremental-cache directories after
confirming that no cargo/rustc process was active. Every entry was older than
24 hours; the directories measured **3.88 GiB** before deletion. Sources,
final binaries, logs and uncommitted work were preserved. The deletion manifest
is `/private/tmp/pd-batch-cache-candidates.json`.

## Incoming TopN integration regression


The remote advanced to `107e8e2a5d1c314172bd8a5efcfbafcd335c0c14` during
publication. Its added truncation discards candidates beyond the initial LIMIT.
Two existing executor tests fail on that code, and the new
`topn_keeps_candidates_beyond_the_limit_in_the_first_child_chunk` regression
fails before repair: input [3, 0, 1] with ascending LIMIT 1 returns 3 instead of
0. Descending order is a control. The merge preserves the remote commit while
removing its truncation and the older limit-derived RequiredRows request.

Fresh master `pkg/executor/sortexec/topn.go::loadChunksUntilTotalLimit` explicitly
keeps complete child chunks to avoid smaller TiKV batches; `executeTopN` trims
the initialized heap afterward. The full focused suite also exposes a
pre-existing disconnected post-spill worker path: 49d7ed0fca removed its caller.
Go `executeTopNWhenSpillTriggered` spills the first heap then starts its workers.
Rust now makes that same transition, removes the repeated serial-segment loop,
and reuses the existing worker implementation even for concurrency one. The
worker regression was expanded to one and more-than-CPU-count workers and fails
before this change, then passes afterward. No assertions were suppressed.

Only the existing Rust TopN owner and its nearest tests change. This is a
regression repair during integration, not transcreation or acceptance of the
whole `sortexec` Go package. Its complete source/test/platform/build inventory
and original Go package validation remain outside this batch receipt.

Commands from `rust/` (the first two preserve failing evidence before repair):

    cargo test --locked -p tidb-executor --lib topn_keeps_candidates_beyond_the_limit_in_the_first_child_chunk -- --test-threads=1
    cargo test --locked -p tidb-executor --lib spilled_topn_uses_parallel_workers_and_preserves_the_answer -- --test-threads=1
    cargo test --locked -p tidb-executor --lib topn -- --test-threads=1
    cargo test --locked -p tidb-session --lib tests_topn:: -- --test-threads=1
    cargo check --locked -p tidb-executor --all-targets
    rustfmt --edition 2021 --check crates/tidb-executor/src/topn.rs

Commands from repository root:

    make lint
    python3 /private/tmp/check-pd-batch.py --synced
    git diff --check
    git diff --cached --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'Merge integration and restore Go TopN chunk and spill ownership'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

All **53 executor TopN tests** pass, including independent Go heap survivors,
Sort-plus-Limit comparisons, RankTopN, one/multiple workers, repeated spills,
output-stage spills, cancellation and file cleanup. Executor all-target
compilation passes, and the changed file passes Rust 2021 formatting. A broader
`cargo fmt -p tidb-executor --check` reports
pre-existing formatting differences in unrelated files; no formatting churn is
included. The first sandboxed lint attempt could not resolve proxy.golang.org;
the network-enabled rerun passes. Logs are `/private/tmp/pd-batch-merge-*.log`.

The session suite has **six passing cases and two pre-existing EXPLAIN failures**:
`a_group_by_pipeline_fuses_above_the_aggregate` and
`select_distinct_fuses_above_aggregation_and_keeps_gos_rows`. Both expect HashAgg
row estimates of 8000 while this branch returns 1000. Replacing only topn.rs
with unchanged b41949f774's version, running the identical session command, and
restoring the reviewed file reproduces precisely those two failures and the
same six successes. The baseline log is
`/private/tmp/pd-batch-merge-topn-session-baseline.log`; the fixed run is
`/private/tmp/pd-batch-merge-topn-session.log`. This repair does not change planner
estimates or accept those failing expectations. Original Go sortexec tests,
live TiKV spill behavior and performance benchmarks were not run for this
integration repair. Full sortexec and planner acceptance remain open.
