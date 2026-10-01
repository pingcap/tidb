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
locked server builds still gate publication; the final response records their
results and the published TiDB commit.

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
