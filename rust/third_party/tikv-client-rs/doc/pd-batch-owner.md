# Use the complete Go PD batch controller

This living ExecPlan follows TiDB PLANS.md and W01. The acceptance unit is the
complete pinned pkg/batch package, with necessary native TSO caller integration;
the PD root, router, metrics, options and full TSO dispatcher remain open.

## Purpose and context


Native TSO still uses a private 64-request drain loop and permits 65,536 batches
in flight. Go uses pkg/batch for both TSO and router requests. Its default TSO
caller supplies a 20,000-request limit, a 20,000-entry request queue and one RPC
token. Replace the private collector with the shared source owner, preserving
all collection, cancellation, timing, completion and adjustment behavior.
Reuse batch buffers as the Go caller does rather than allocating 20,000 entries
per RPC. This is correctness and bounded-resource work; workload speedups must
be measured separately, without claiming synthetic batching as SQL throughput.

Pins: TiDB Go master 93a01d31f6da205ae4bf376825293903a6899fdb selects PD client
v0.0.0-20260805103528-afa43111d149. Both implementation branches are up to date.
Native baseline is 4e3169ed93e433eab38638a1dd92a24f25f7caaf; TiDB starts at
8bc2354d8fa85f180c121abe71a5e110a6cbedde. The complete package consists of
batch_controller.go and batch_controller_test.go. There is no doc.go, generated
source/input, build/platform variant, fixture or package-local build file.
Inventory both files, all original/support artifacts and shared build inputs.

## Progress


- [x] Refresh both implementation branches and master; inspect both batch artifacts and TSO/router callers.
- [x] Reproduce native wire batch-size and default in-flight-token mismatches before production changes.
- [x] Implement the complete controller, original three cases and branch/lifetime tests.
- [x] Replace the native collector, completion and default queue/token policy; reuse controller buffers and preserve existing deadline/connection ownership.
- [x] Run unchanged Go race/goleak cases, full native tests, strict Clippy, all-target compilation, formatting and artifact checks.
- [ ] Publish native master, synchronize TiDB and pass adapter/lint/build/publication gates.

## Plan of work and milestones


Add deterministic stream tests to src/pd/timestamp_tests.rs. A prequeued 20,001
request workload must yield batches of 20,000 and one. A second batch must wait
until the first completes in default single-RPC mode. Run these on baseline.

Implement src/pd/batch.rs, including optional token admission, collection before
token arrival, max-batch early return, bounded extra wait, caller-owned timer
collection and final nonblocking drains. Start the extra-batching timestamp only
at Go's corresponding point; the full-batch/token early return leaves it alone.
Implement request views, short-circuit iteration, indexed finish callbacks and
the source additive-increase/additive-decrease best-size rule with observation
before adjustment. Preserve the original shared-pointer test cases with Arc.

Use the existing Cancellation, tokio mpsc, monotonic timers and semaphore permit
as native adapters. A successful fetch transfers its permit to the caller. Error
or dropped fetch returns the permit before invoking the default finisher; no
request or token can be stranded by dropping a Rust future. Timer-only fetch
leaves requests with its caller on error. Rust channel closure is an explicit
terminal error; source callers never close request/token channels. A zero
duration represents Go nonpositive optional extra wait. Document all adapters
and the source small-max/default-best-size behavior without inventing settings.

Integrate in src/pd/timestamp.rs. Keep each controller with its in-flight batch,
finish it on response/error/drop, and recycle its empty buffer through a short
pool lock. Use the source default TSO queue/controller limits and RPC token.
Source dynamic concurrency, latency-driven pacing and option plumbing remain
full parent-package work, not features added by this leaf. Connect the existing
Prometheus dependency to the exact source best-size histogram; no new library.

## Validation and acceptance


From native client-rust, run:

    cargo test --locked --lib source_batch_ -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Run unchanged pinned source in a temporary module copy:

    go test -mod=readonly -race ./pkg/batch -count=1

No failpoint calls occur in this leaf or its used support path. No Go/Bazel
changes in TiDB are planned, so bazel_prepare is not triggered. After native
publication run TiDB's maintained sync script, bridge and PD tests, affected
all-target check, root make lint, actual hook locked server build, and a fresh
locked server build immediately before normal push. Record exact commands and
results in the TiDB receipt. Preserve incoming work; never force-push.

## Surprises & Discoveries


Both wire regressions failed before implementation: first batch 64 rather than
20,000, and a second RPC advanced before completion returned the first token.
The new discard-order regression also failed before correcting RequestGroup
field drop order. Final completion explicitly returns the permit before invoking
request callbacks; discarded fields return it before the pooled controller is
finished. The timestamp callback retains the prior subtraction order, avoiding
an intermediate signed overflow. Buffer reuse is checked by allocation identity
and absence of retained finished senders. Logs are /private/tmp/pd-batch-*.log.

The shared controller has two distinct error lifetimes: FetchPendingRequests
returns its token and finishes its requests, while FetchRequestsWithTimer keeps
them for its caller. A generic collect-until-timer helper would lose that
distinction. Best-size observation precedes adjustment, and the token-late full
batch returns before setting extraBatchingStartTime.

## Decision Log


Decision (2026-10-01): port the whole pkg/batch prerequisite and replace the
native collector. Merely changing 64 to 20,000 would leave token, timing,
finisher and reuse ownership incorrect. Preserve source default single-RPC
behavior without claiming full concurrent dispatcher/options/router parity.

## Outcomes & Retrospective


The complete batch leaf and native default-mode integration pass all 78 focused
PD tests, 1,453 native library tests (two pre-existing ignored), the original Go
race/goleak suite, strict Clippy, all-target compilation, formatting and artifact
checks. Native publication and TiDB synchronization/build gates are recorded in
the TiDB receipt after completion. P06 remains partial and P03/P07 remain open. Native
single-leader/default-mode evidence cannot certify full PD/TiDB parity or
sysbench/TPC-C/TPC-H/YCSB performance. Original source variants are inventoried;
Linux and real cluster execution remain unverified unless recorded otherwise.

## Recovery and artifacts


Keep fail-before logs and source hashes. If a gate fails, repair its owner and
rerun affected checks; do not suppress cases. Native publication precedes TiDB
synchronization. The inventory is doc/pd-batch-package.json and the final
integration receipt is rust/docs/parity/current-audit/pd-batch-owner-repair.md.
