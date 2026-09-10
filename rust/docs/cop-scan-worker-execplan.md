# Remove unnecessary runtime crossings while preserving Go request ownership

This living ExecPlan follows root PLANS.md. Preserve the full sysbench/TPC-C
throughput and latency objective. The current increment reduces CPU use but
does not meet throughput acceptance or whole-Go-package completion.


## Purpose / Big Picture

Make generic Rust SQL execution faster without workload-specific shortcuts,
different SQL results or weaker cancellation/lifecycle semantics. Go source is
the authority for task concurrency, batching, paging, retries and results.
Acceptance requires beating the immutable faster Rust control on matched work,
then validating TPC-C and mixed writes. CPU savings alone are insufficient.


## Progress

- [x] Read pinned Go batch send/receive and cop-worker ownership.
- [x] Preserve native result replies, notification and absolute-deadline parking
  from the earlier increment.
- [x] Push checkpoint 431d637dec to hparser-integration and verify remote SHA.
- [x] Co-locate the existing command owner and its connection tasks on one
  transport-owned event loop; remove per-connection runtime allocation.
- [x] Run one consolidated release validation: 2,056 passed, zero failed,
  16 ignored; server build passed with twelve jobs.
- [x] Match seven live SQL comparisons and before/after workload hashes.
- [x] Complete 40 fixed-work trials: 240,000 measured plus 40,000 warmup
  transactions, zero SQL errors/reconnects.
- [x] Capture native and CPU profiles separately from timing. Verify the named
  send-to-driver kernel-wake path disappears from the sampled candidate chains.
- [x] Compare actual Go/Rust range plans and region-task counts. Both need four
  tasks; do not bypass the required workers.
- [x] Stop five supervisors; verify eight owned PIDs gone and sixteen ports
  closed. Preserve fixture data and update the current receipt.
- [x] Rerun the 2,056-test release scope, server build, formatting and Ready lint
  for the user-requested checkpoint. Confirm all 32 measured source/config files
  and the measured server SHA256 match current bytes.
- [ ] Attribute remaining four-region request latency before the next connected
  optimization: collection, I/O, cop-worker/SQL rendezvous and join.
- [ ] Beat the faster control on throughput/latency and validate TPC-C/mixed writes.
- [ ] Complete required whole-package/platform validation before parity claims.
- [x] Compile the merged executor/session/server and all their test targets.
- [x] Remove 52 identified Rust-only tests and obsolete helper scaffolding.
- [x] Run retained Go memory-cleanup, sort-spill and session-variable tests:
  three passed, zero failed. Ready lint passed.
- [ ] Complete the repository-wide Go-test provenance audit.
- [x] Reconcile the upstream merge through b77c90cde6 with no unresolved entries.
- [x] Build release server/smoke binaries and compare 81 live Go/Rust SQL cases.
- [ ] Finish consolidated socket-enabled tests and verify normal remote publication.


## Context and Orientation

rust/crates/tidb-txnkv/src/rpc/execution.rs::TransportIo owns one native thread
running a Tokio current-thread event loop. transport_runtime.rs spawns the
existing command task on that loop and supplies the same Handle to all configured
connection slots. Connection count still controls sockets. ConnectionTasks keeps
each physical channel generation's independent task scope; shutdown closes those
scopes before stopping and joining TransportIo, including panic reporting.

Cop workers continue on the shared multi-thread execution_runtime. No SQL or
blocking recovery is moved onto the transport event loop. Batch policy,
identities, retries and deadlines are unchanged. The increment changes two
production files, plus this plan and the current benchmark receipt.

rust/crates/tidb-distsql/src/cop_paging/cop_iterator.rs owns independent workers,
ordered two-response buffers, unordered producer/consumer rendezvous and join.
rust/crates/tidb-exec/src/cop_scan.rs decodes responses on the SQL consumer.
These are real Go responsibilities, not optional synchronization to remove.


## Decision Log

Decision (2026-09-10, user request): remove tests absent from the Go suites,
including Rust-only source-text protection checks and retired implementation
scaffolding. Keep tests ported from Go even when their Rust names differ.

Decision (2026-09-10, user approval): retire the unused crate-root physical
builder/readers and join builder, preserving the active driver builder. Use one
JoinOutput for serial and parallel output/defaults, one concurrency value, and
checked child access at the merged ownership boundaries. Compile after the
coherent integration batch; do not treat compilation as semantic acceptance.

Decision (2026-09-10): move publication to its I/O owner, not a different batch
policy. Pinned client-go conn_batch.go:193-339 collects requests, chooses a
connection and sends from the batch owner. Rust's prior separate runtimes added
a kernel wake at packet publication. Co-location removes that boundary while
retaining asynchronous connection tasks and their distinct lifetimes.

Decision (2026-09-10): retain the measured CPU reduction as WIP, not throughput
success. Against immediate prior, SQL CPU is -8.07 percent, throughput -0.39
percent and p95 unchanged. Do not promote a slower candidate over the faster
control or claim the remaining latency is solved.

Decision (2026-09-10): preserve source-required worker behavior. Go creates an
unbuffered unordered result channel. Rust removes terminal active attempts at
response acceptance, so terminal state does not itself cause false fallback.
Live Go EXPLAIN ANALYZE and Rust traces both show four cop tasks for the same
bounded range. Region boundaries explain those tasks; they are not redundant.


## Surprises & Discoveries

Tokio 1.53.0 current_thread::Handle.schedule pushes same-runtime wakes into its
local task queue. External wakes enter the remote queue and unpark the driver.
The driver calls mio 1.2.2; macOS kqueue wake issues kevent. The prior source
crossed that boundary from publish_batch/send_group for every packet.

Two separately captured eight-client Time Profiler traces contain 28,727 prior
and 25,593 candidate Running samples. The predecessor's named
send_group -> wake_by_val -> I/O Handle.unpark -> kevent chain accounts for
193ms sampled CPU among the top-65 synchronization chains; it is absent from the
candidate list. Source independently confirms local publication. This does not
prove every remote wake or kevent is gone, nor quantify the entire latency gap.

Against the faster control at 1/8/32 clients, throughput is -2.63/-10.03/-6.11
percent, SQL CPU +20.06/+29.81/+15.95 percent, and p95 +0.99/+11.00/+7.45 percent.
All seven throughput ABBA blocks regress. ABBA runs control, candidate,
candidate, control to reduce simple ordering bias.

Against immediate prior at eight clients, three ABBA blocks give throughput
-0.39 percent, SQL CPU -8.07 percent and identical mean p95. Shared cop-worker
condition-variable waits/signals, native SQL/worker rendezvous and reply parking
remain prominent. Named profile categories are not complete critical-path
attribution. Native all-thread sample counts include waiting and are not CPU
percentages or interchangeable with Go pprof units.

The diagnostic range id BETWEEN 50000 AND 50099 has matching Go/Rust
TableReader -> Projection -> TableRangeScan plans. Go EXPLAIN ANALYZE reports
four tasks/four RPCs, and Rust traces four tasks. SHOW TABLE REGIONS confirms
four real regions intersect that range. No task-count shortcut is justified.


## Plan of Work and Milestones

The transport co-location milestone is implemented and measured; preserve its
evidence before subsequent edits. Do not revisit the removed publication
boundary as though it still existed.

The next milestone measures elapsed stages along the real four-region query:
batch collection, socket completion, cop worker, SQL response consumption and
worker join. Use existing request timestamps where possible and reconcile each
stage with pinned Go. The asynchronous batch collector still owns a native timer;
Go reuses and stops its timer, while Rust currently discards its Delay when idle.
That is a source difference to measure, not an established cause or permission
to relax deadlines.

Implement one connected, source-backed change, run the affected release
validation once, rebuild and repeat matched work. Do not tune arbitrary batch
sizes/thread counts, bypass independent workers or reconstruct the Go runtime.


## Concrete Steps and Validation

From /Users/qiliu/projects/tidb/rust, both commands exited zero:

    cargo test --offline --locked --release -j12 --no-fail-fast -p tidb-distsql -p tidb-txnkv -p tidb-unistore -p tidb-exec --lib --tests
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server

From repository root:

    rustfmt --check --edition 2021 --config skip_children=true rust/crates/tidb-txnkv/src/rpc/execution.rs rust/crates/tidb-txnkv/src/rpc/transport_runtime.rs
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    git diff --check -- rust
    ruby /private/tmp/tidb-transport-owner.gHQJhr/verify-cleanup.rb

Validation reused existing transport, cancellation, deadline, paging and
shutdown suites. No Go/Bazel changes require bazel_prepare. Ready checks passed
again for the requested commit and push, with fresh logs in
/private/tmp/tidb-push-checkpoint.rAQYpO. This remains a WIP performance increment,
not release readiness. Full workspace tests, seven known executor
failures, live transaction difftests, TPC-C/mixed-write performance and complete
platform/package semantics remain unverified.

Live measurement commands:

    ruby /private/tmp/tidb-transport-owner.gHQJhr/candidate/read_parity.rb
    ruby /private/tmp/tidb-transport-owner.gHQJhr/prior-comparison/measure.rb
    ruby /private/tmp/tidb-transport-owner.gHQJhr/candidate/measure.rb

Each trial uses 6,000 measured plus 1,000 warmup transactions. The immediate-prior
comparison has twelve trials at eight clients; the faster-control comparison
has 28 at one/eight/32 clients. Build and profiling never overlap timing.
The desktop is shared, not an isolated performance host. Live Go differs from
checkout; selected SQL equality does not establish full source parity.


## Outcomes & Retrospective

Candidate SHA256 e61d432de6c71681b9868d7df9a0e81786c56551093742ed95f1793c5c130c8c
reduces fixed-work SQL CPU relative to immediate prior 76f6f08fa without improving
throughput or p95. It still trails faster control 84c39b216. Co-location removes
a verified cost, but not the full bottleneck. The full objective remains open.


## Idempotence and Recovery

Current evidence is /private/tmp/tidb-transport-owner.gHQJhr. before-source.tar.gz
preserves pre-edit RPC source and the preceding baseline; candidate-source.tar.gz
holds the measured source/configuration. Frozen binaries, raw trial logs,
SQL hashes, native samples and CPU traces remain local. The current summary is
rust/benchmarks/cop-progress-baseline.json.

All five owned supervisors are stopped. Preserve data at
/private/tmp/tidb-live-workload.nIrZBe; archive its logs/manifest before reuse.
Never restore source archives over unrelated work. Publish only to PingCAP's
origin/hparser-integration with a normal push, preserving unrelated local work
and concurrent remote commits. The measured base commit is 431d637dec; verify
the published merge commit on the remote before reporting delivery.

The authorized merge is being assembled in /private/tmp/tidb-upstream-merge.nzY42N,
preserving the original checkout and its uncommitted files. Local merge
cf5e822e99 combines 59700b3180 and cf6990b012. The newer remote history through
b77c90cde6 is also integrated; there are no unresolved index entries.

The active executor owns physical construction and ResolveIndices. The unused
crate-root builder/readers and duplicate join state are retired. Scan pruning
preserves stored column identities; projection offsets resolve against the
actual child schema. Preserved-side outer-join predicates remain join conditions.
Prepared scope resolvers forward the execution parameter snapshot. Window
execution includes ranking, value/relative functions and RANGE comparison columns.
TiKV select-response errors retain typed code, SQLSTATE and message through
storage, executor, recordset, text/binary writer and cursor materialization.
No string marker or substring-based error-code parsing remains.

Client identity, authentication, privilege attachment and connection kill
handling remain in public open_session. Both boot paths configure the factory
before sharing it; the binding writer borrows a weak factory reference.
The cleanup removes 52 identified Rust-only tests and obsolete scaffolding,
not every test lacking a completed provenance audit. Retained shared fixtures
must still compile against the current public interfaces.

Merged validation is separate from earlier measured performance evidence.
Bazel preparation passed with twelve jobs after matching the WORKSPACE Go pin
to go.mod 1.25.12. Earlier foundation runs passed 1,188 expression, 1,192 planner
and 641 transaction tests (99, 1,071 and 10 ignored respectively). Retained Go
memory-cleanup, sort-spill and session-variable tests passed. The latest broad
runtime run (merge-latest-runtime.log) reports executor 1,278 passed / 13 failed
and session 1,522 passed / 157 failed / 210 ignored. These failures include
plan/receipt expectations, unsupported SQL shapes and result/metadata differences;
they have not all been established as pre-existing and are not waived or deleted.
The merged code is not complete Go parity or a fully green release.

Release server and smoke builds passed. The real TiKV scan differential ran
81 Go/Rust SQL cases with no row/error divergence, including exact COT(0)
code 1690, SQLSTATE 22003 and message. Its 78 coprocessor receipts were unavailable
because cluster-session-smoke reports PD shutdown with three live request handles.
This proves the exercised SQL comparisons, not pushdown/batching acceptance.
Playground cleanup completed and ports 46199/47820/47920 are closed.
Evidence: merge-live-scan-pushdown.log under /private/tmp/tidb-counter-window.jbkXVN.

Final socket-enabled client/executor checks are in progress. An earlier
sandboxed run passed the non-socket checks; all 31 socket failures were
PermissionDenied at bind. The authorized rerun is merge-final-verified.log.
Ready lint passed in merge-final-lint-2.log with
GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint. Publication remains pending a normal
push to origin/hparser-integration and remote SHA verification.

Remaining semantic work includes statement-context-aware cached-plan rebuilding,
physical selected-row locking, unsupported aggregate/window/subquery shapes,
and the broad failures listed above. CachedSelectPlan::bind and CachedDmlPlan::bind
still use parameter-only rebuild context, whereas Go plan_cache.go::adjustCachedPlan
uses the current session's ranger context. One current statement context must own
cache rebuilding and execution. No complete package or performance acceptance
is claimed by this merge.

Counter diagnostics at /private/tmp/tidb-counter-window.jbkXVN/probe.json show
that TiKV metrics continue publishing buffered work after a benchmark ends.
Settled eight-client windows show roughly 10 gets and 400 coprocessor keys per
transaction for both Go and Rust. Immediate snapshots cannot support a claim
that Rust reads more rows. Rust returned about 3.8% more coprocessor bytes in
these diagnostic windows; attribution remains open. These shared-host trials
are not accepted performance evidence. All probe services are stopped.


## Interfaces and Dependencies

The measured co-location increment changed internal scheduling ownership, not
protocol rules. The subsequent full upstream merge changes public Rust
interfaces and dependencies; its API reconciliation and validation are tracked
separately above. TransportIo remains the native transport scheduling owner.

Updated 2026-09-10 after the co-location measurement: replaced the earlier
per-connection experiment's active status with current source, evidence,
rejected hypotheses and next latency-attribution work.

Updated 2026-09-10 during the authorized full upstream merge: corrected the
publication remote, recorded source-backed resolutions and counter visibility,
and separated prior-binary evidence from pending merged-code validation.
