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

- [x] Rebuild and measure merged 6d26dab06d against the immutable control:
  fourteen fixed-work trials, 84,000 measured transactions, equal SQL hashes.
- [x] Profile current Rust, control and Go separately. Current Rust has 1,044
  statistics-load workers among 1,164 native threads; control has none.
- [x] Move statistics-load worker ownership from session catalog creation to
  the domain-shaped factory, and replace mutex-held timed receives with Go's
  channel-select shutdown/task lifecycle. Retained catalog/statistics tests pass.
- [x] Reproduce the commit-history ownership cycle, then store data-only catalog
  snapshots and reattach live owners when serving stale reads. All 52 scoped
  catalog, statistics and transaction tests pass; Ready lint passes.
- [x] Measure the combined worker/history lifecycle fix against immutable current
  and faster-control binaries: 20 trials, 120,000 measured transactions and matching
  SQL hashes. At 32 clients, throughput +6.87%, SQL CPU -12.74%, p95 -6.94% versus
  pre-fix. One/eight-client throughput remains effectively flat; control still wins.
- [x] Profile after repeated connection churn: six named statistics workers,
  126 total native threads. No per-session worker growth or receiver-mutex polling.
- [x] Stop this increment's owned services: verify 19 PIDs exited and 16 ports
  closed. No compared server required forced termination; preserve fixture data.
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
- [x] Remove 56 identified Rust-only tests and obsolete helper scaffolding.
- [x] Run retained Go memory-cleanup, sort-spill and session-variable tests:
  three passed, zero failed. Ready lint passed.
- [ ] Complete the repository-wide Go-test provenance audit.
- [x] Reconcile the upstream merge through b77c90cde6 with no unresolved entries.
- [x] Build release server/smoke binaries and run the 81-case live SQL harness.
- [x] Identify and remove PD's Go-incompatible reference-count shutdown gate.
- [x] Verify retained-handle/in-flight cancellation and live shutdown.
- [x] Repaired SQL assertions expose 11 real row divergences, all returning
  unfiltered rows when physical predicates are not fully described remotely.
- [x] Keep residual conditions authoritative across chunk/row reads,
  projection, TopN/Limit, partial aggregation and scan reopening; rerun live SQL.
- [x] Live result equality: 78 row, two error and one projection assertion pass,
  plus five related LIMIT/TopN/COUNT/SUM checks. Missing receipts now fail loudly.
- [ ] Repair receipt validation: removed diagnostic output and an early return
  concealed both missing receipts and unperformed SQL equality assertions.
- [x] Pass 78 scoped socket/executor/planner tests, workspace all-target compilation,
  formatting, shell syntax and Ready lint.


## Context and Orientation

rust/crates/tidb-txnkv/src/rpc/execution.rs::TransportIo owns one native thread
running a Tokio current-thread event loop. transport_runtime.rs spawns the
existing command task on that loop and supplies the same Handle to all configured
connection slots. Connection count still controls sockets. ConnectionTasks keeps
each physical channel generation's independent task scope; shutdown closes those
scopes before stopping and joining TransportIo, including panic reporting.

Cop workers continue on the shared multi-thread execution_runtime. No SQL or
blocking recovery is moved onto the transport event loop. Batch policy,
identities, retries and deadlines are unchanged. The earlier transport-only increment changed two
production files. Current statistics/history ownership changes are described below.

rust/crates/tidb-distsql/src/cop_paging/cop_iterator.rs owns independent workers,
ordered two-response buffers, unordered producer/consumer rendezvous and join.
rust/crates/tidb-exec/src/cop_scan.rs decodes responses on the SQL consumer.
These are real Go responsibilities, not optional synchronization to remove.

The current statistics increment is owned by executor catalog/sync_load.rs and
server ClusterSessionFactory. The factory shares one worker pool across catalogs;
commit history stores CatalogSnapshot data without back-references to its ring.


## Decision Log

Decision (2026-09-10, initial merged-code profile): fix statistics worker ownership before
batch-timer experiments. ClusterSessionFactory.open_storage_session creates a
fresh statistics service/pool per catalog, including internal pooled sessions.
Go Domain.StartLoadStatsSubWorkers starts one pool for its StatsHandle. The
Rust workers also serialize recv_timeout(10ms) behind a shared receiver mutex;
Go selects tasks or domain shutdown without idle polling. Make the worker pool
an explicit shared domain-owned argument and use native multi-consumer channels.
Preserve per-request cache publication, urgent/expired priority, queue limits,
singleflight, retry and worker join semantics.
Decision (2026-09-10, catalog lifecycle): committed snapshots held the same
Arc<CommitHistory> that contained them. This retained session catalogs and their
statistics services indefinitely. Store only committed data in the ring; attach
statistics, analyze-options and history owners to returned live views, matching
Go's distinction between snapshot infoschema and domain services. Do not clear
history at session shutdown or add reference-count gates.

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

Fresh post-merge evidence is in /private/tmp/tidb-current-throughput.6qK3A7.
At eight clients Rust averages 1,654 TPS versus the faster control's 1,963 TPS;
SQL CPU is 14.20 versus 8.62 seconds for equal 6,000-transaction trials.
The separate CPU trace attributes 11.472 seconds of sampled Running weight to
statistics-loader synchronization chains, out of 43.211 seconds total. This
is not a latency percentage. The native sample confirms 1,044 such workers,
created per session/internal catalog, whereas Go owns its pool per domain.
No current throughput acceptance is claimed; the older baseline stays intact.

The worker-only fresh-process ABBA run is in /private/tmp/tidb-stats-workers.WDt2ZF:
20 trials, 120,000 measured transactions, zero errors/reconnects and identical
full SQL hashes. A separate native sample shows six statistics workers, rather
than 1,044. Throughput changes are small and do not establish a performance win.
The original simultaneous-server profile is diagnostic, not comparable fixed-work
CPU evidence; observing thousands of native threads can itself be expensive.

A second fail-before test proves the catalog history cycle. After data-only
snapshots, the last stale view releases both history and statistics workers.
The original stale-read transaction fixtures still pass. Evidence:
catalog-cycle-red.log and catalog-cycle-green.log under the worker artifact root;
Ready lint and the final binary are tracked under
/private/tmp/tidb-stats-lifecycle.BPa0w4. The final binary SHA256 is
27c9b05fe9476821aea654a7e9578faa6c679667e9a4abd674a754b078902fa3.

The final matched-work run uses fresh compared servers in before/after/after/before
order, and brackets the run with faster-control and Go trials. All 120,000 measured
plus 20,000 warmup transactions finish without errors or reconnects; every full SQL
hash matches. At 1/8/32 clients, throughput changes versus pre-fix are
-0.69/-0.38/+6.87 percent; SQL CPU changes are -0.05/-1.61/-12.74 percent; p95 changes
are +0.78/0/-6.94 percent. The candidate still trails the faster control by
11.81/15.26/10.03 percent throughput. These are shared-host measurements with two
samples per server/concurrency, not general performance acceptance. TPC-C/mixed
writes and full Go-package parity remain open. The current receipt contains exact
commands, binary identities and aggregate measurements; raw traces remain separate.

Catalog/statistics/transaction validation used:

    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-session -p tidb-server --lib -- driver::catalog tests_core::transactions cluster_session_node::tests::statistics --test-threads=12
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    ruby /private/tmp/tidb-stats-lifecycle.BPa0w4/measure.rb
    ruby /private/tmp/tidb-stats-lifecycle.BPa0w4/profile.rb


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

Current correctness increment: follow pinned PD client afa43111d149
client.go::Close -> inner_client.go::close (cancel, wait, close services), and
client-go e4905600583b tikv/kv.go::Close (stop background tasks, region cache,
TiKV transport, then PD). Remove only PD's reference-count prerequisite, not
cancellation, worker joining or actual error reporting. Existing retained-handle
and in-flight fixtures must fail before the production change and pass after.
Make missing receipts fail the differential, and compare SQL independently.

The corrected live run exposes a second production root cause. TableScanExec
trusts a backend's receipt for a subset of the original conditions, and its
Open removes retained filters. Use one complete-description decision for remote
post-Selection operations; keep the executable filter across Open/Close.
Go PhysicalSelection.ToPB encodes the complete Conditions list, never treating
an empty encoded subset as proof of the whole Selection. Existing index lookup
paths already combine their receipt with fully_described; table scans must too.

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

Post-merge root-fix evidence is under /private/tmp/tidb-counter-window.jbkXVN.
PD close tests fail twice before the production change (SharedOwners) and pass
after: 70 passed, one ignored across the PD library and aggregate test target.
The actual smoke path closes cleanly. Removing a reference-count gate is not
removing worker cancellation/join/error checks; those remain tested.

The corrected live harness initially found 11 wrong-result cases: omitted
conditions made the table-scan fast path return all 2,000 rows. A single
complete-description decision now protects remote post-Selection operations;
Open no longer erases the executable filter. The subsequent run has 78 row,
two error and one projection equality assertion passing. Five additional SQL
comparisons against the same Go node also pass:

    SELECT id FROM t WHERE sbig + 1 > 999 ORDER BY id LIMIT 1
    SELECT id FROM t WHERE abs(sbig) ORDER BY id DESC LIMIT 3
    SELECT COUNT(*) FROM t WHERE sbig + 1 > 999
    SELECT SUM(sbig) FROM t WHERE NOT mod(sbig, 100)
    SELECT id FROM t WHERE mod(sbig, 2.5) ORDER BY id LIMIT 3

The live harness still exits 1 for 78 unavailable coprocessor receipts, not SQL
differences. The removed smoke diagnostics must be replaced with current
runtime statistics before claiming pushdown/batching acceptance. This is not
performance evidence. Index partial-aggregation receipt completeness also needs
follow-up source/live verification; the table-only fixture does not prove it.

Commands for this increment (Cargo and script from the isolated rust directory):

    cargo test --offline --locked --release -j12 -p tidb-pd-client --lib --test all -- close_cancels
    cargo test --offline --locked --release -j12 -p tidb-pd-client --lib --test all --no-fail-fast
    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-exec --lib --test all --no-fail-fast -- kv_table::table_scan predicate_pushdown cop_scan_
    KEEP_LOGS=1 SCAN_PUSHDOWN_PORT_OFFSET=43820 CARGO_BUILD_JOBS=12 bash scripts/run-realtikv-scan-pushdown.sh
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint

The script builds release server/smoke with twelve jobs. Ready lint passed;
formatting, bash syntax and git diff checks pass. The final scan/predicate/cop
suite passed 42 tests with zero failures (112 passes including PD). One obsolete
Rust-only refusal-message protection test and unused helpers were removed;
the existing residual SQL fixture now covers NOT, projection and TopN/Limit.
Logs are pd-close-before.log, pd-close-after.log, pd-close-live.log,
scan-filter-after-live.log, scan-filter-scoped-verified.log and scan-filter-lint.log. Both isolated playgrounds
are stopped and their PD/Go/Rust ports 46199/47820/47920 are verified closed.
No Go or Bazel source changed, so no new bazel_prepare run is required.

Changed production files: tidb-pd-client/src/client/mod.rs and src/error.rs,
tidb-executor/src/kv_table/table_scan.rs, and lifecycle comments in
tidb-exec/src/real_tikv_read.rs and tidb-server/src/bin/cluster-session-smoke.rs.
Changed fixtures: tidb-pd-client/tests/pd_worker_lifecycle_source.rs,
tidb-exec/tests/cop_scan_narrowed_output_source.rs and
cop_scan_partial_predicate_limit_source.rs. The differential script, this living
plan and baseline validation metadata carry the corrected evidence. Historical
performance measurements are unchanged. Full runtime/package/platform parity
and throughput remain unverified; the earlier broad-suite failures remain open.


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
The cleanup removes 56 identified Rust-only tests and obsolete scaffolding,
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
81 cases, including exact COT(0) code 1690, SQLSTATE 22003 and message. A subsequent
source audit found that missing receipts returned before SQL equality assertions;
the earlier claim of 81 proven comparisons was incorrect. Two independent issues
exist: PD shutdown rejects retained handles, and the smoke binary no longer emits
the receipt lines parsed by the script. Neither full SQL equality nor pushdown
acceptance is established by that earlier run.
Playground cleanup completed and ports 46199/47820/47920 are closed.
Evidence: merge-live-scan-pushdown.log under /private/tmp/tidb-counter-window.jbkXVN.

Scoped Ready verification passed: 71 client/executor/recordset tests, seven
Go-derived planner fixtures, workspace compilation with all targets, formatting,
shell syntax and lint. Logs are merge-final-verified.log,
merge-planner-fixtures-final.log, merge-workspace-ready-2.log and
merge-final-lint-2.log. The final live rerun is merge-live-scan-final.log;
it compares full Go error text and explicitly reports incomplete receipts.
The earlier 31 socket failures were PermissionDenied at bind and all passed
with authorized localhost access.

The final workspace check also reconciled explicit callback completion types,
native expression-backed sort items and reader explain arguments in retained
fixtures. Four tests of the retired Rust-only ProjectionInlineExpr model were
removed; real-plan join-reorder tests remain. Production merge da0bf5ab86
preserves both local history and the shared upstream head b77c90cde6.
Publication target is origin/hparser-integration, normal push only.

Exact final commands (Cargo commands from the isolated worktree's rust directory;
lint and formatting from its root):

    cargo check --offline --locked --release -j12 --workspace --all-targets
    cargo test --offline --locked --release -j12 -p tidb-executor -p tidb-exec -p tidb-server --no-fail-fast --lib --tests -- coprocessor_cot_error_matches_go select_response_errno_survives_recordset_mapping resultset_writer_source mysql_client_lifecycle_source initial_database_tcp_source concurrent_mysql_sessions_source mysql_native_auth_lifecycle_source prepared_typed_markers tests_executor_part19_source shared_physical
    cargo test --offline --locked --release -j12 -p difftest-planner-tests --test all -- physical_sort physical_table_reader
    KEEP_LOGS=1 SCAN_PUSHDOWN_PORT_OFFSET=43820 CARGO_BUILD_JOBS=12 bash scripts/run-realtikv-scan-pushdown.sh
    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    git diff --name-only --diff-filter=ACM -- '*.rs' | xargs rustfmt --check --edition 2021 --config skip_children=true
    bash -n rust/scripts/run-realtikv-scan-pushdown.sh rust/scripts/run-realtikv-lock-recovery.sh
    git diff --check

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
