# Review of all known unresolved owners after the removals

All **77 known unresolved findings remain unresolved: 72 open and five partial**.
The partial entries are A01, C04, E02, T02 and P06. Eight repaired entries retain
their status; none is reopened. This review changes documentation and evidence,
not production behavior or package acceptance.

The central remaining problem is incomplete production ownership. Removing a
false-success path prevents that path's bad behavior, but does not implement
Go's supported operation. Conversely, a source-derived helper, metric family,
configuration variable or test-only constructor does not establish a running
owner with all callers and shutdown wired.

## Baseline and scope


Fresh fetches confirm integration `68d6de685a5e58c559a861ec7b85d10bc8a2aa60`,
Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, and native client-rust
master `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`. The Go-selected client-go,
PD, protobuf and other module versions have not changed. TiDB already contains
this native revision; no dependency bump is warranted.

Compared all 138 literal source references from the previous 313b3cfea3 review:
21 reference occurrences changed. Read every intervening production diff,
including changed callers, rather than assuming unchanged leaf files prove
integration unchanged. Only 13 crate paths changed, including tests and the
deleted capture; no native, Go, Cargo or other runtime inputs changed. Reviewed
the affected DDL, session and runtime-table owners and rechecked high-risk
account, generated-value, startup/GC and native-close/request boundaries. The
shared persisted-job control/error/MDL function is byte-identical to the previous
review; repartition removal did not repair D04/D05/D07.

[Per-ID continuity](post-removal-recheck/source-continuity.json) preserves all
85 dispositions, prior/current source blobs, complete changed-path lists and
fresh diagnostic associations. Unchanged source carries earlier evidence;
this is not 77 new runtime tests or proof that every possible defect is known.
The existing [register](structural-findings.md) remains the detailed complete
known list. Its original package/fixture/build/platform/generated obligations
remain open until whole-package acceptance; the inventory is not certification.

## Fresh correctness evidence and priority


The six retained diagnostics were rerun against the reviewed revision. They
record behavior and exit successfully even when the behavior is wrong. These
are observations, not passing parity tests.

| Finding | Current observation | Required owner and review limit |
| --- | --- | --- |
| D11 | A duplicate-column error on the second ALTER action leaves the first new column installed. | Go collects subjobs and coordinates the whole action list through the durable multi-schema job. Rust's FK-only staging is insufficient. Reproduced in-process; not a demonstrated partial cluster metadata commit. |
| A04 | PASSWORD HISTORY 3 is accepted; changing the password and reusing the original succeeds; stored policy is NULL. | Go account policy and history persistence/verification must move together. Remove accepted no-op option handling through that complete owner; no authentication attack was attempted. |
| K03 | Strict ordinary TINYINT rejects 1000; a stored generated TINYINT accepts it as 127 with no warning. | Go table.CastValue receives the active mutation context. Rust defaults/discarded conversion warnings differ. Fix the shared INSERT/UPDATE/index/DDL contract, not one literal or expression. |
| N01 | A raw Latin-1 E9 in COM_QUERY SELECT HEX becomes C3A9. | Preserve source bytes and charset across wire, parser, literal and datum ownership. A HEX-only fix is inadequate. |
| N03 | Valid token-limit, performance.stats-lease and security.ssl-ca TOML is rejected; auto-TLS defaults true. | The private NodeConfig whitelist/defaults remain a second config authority. Move effective source config and consumers together. Go-removed run-auto-analyze is correctly refused. |
| I01 | A created sequence is absent from SEQUENCES; a bounded CLUSTER_LOG query still reports missing start time. | Typed runtime retrievers must replace empty/captured behavior. CLUSTER_CONFIG now refuses instead of returning captured settings. |
| O18 | Enabling both summary switches and executing SQL still yields zero cumulative summary rows. | Statement completion, summary-mode selection, reader and sink lifetime need one integrated contract. Reproduced through the in-process SQL owner. |
| S03, S04, E07 | Session migration, historical read and BR job statements remain refused. | These are missing supported runtimes; syntax/settings/helpers do not supply them. |
| K02, Q01, N02 | CACHE, Cascades and token-limit configuration are accepted. | Acceptance alone does not validate leases, optimizer selection or concurrency permits. Their absent consumers remain source findings; the probes do not prove a distributed cache failure or measured performance loss. |

Outputs: [account/IMPORT](post-removal-recheck/expanded-ownership.txt),
[table/session](post-removal-recheck/subsystem-structure.txt),
[DDL/runtime tables](post-removal-recheck/session-ownership.txt),
[summary](post-removal-recheck/remaining-structure.txt),
[wire/config](post-removal-recheck/expanded-server.txt), and
[partition preservation](post-removal-recheck/partition-structure.txt).
The already-known missing tidb_opt_range_max_count upstream delta is still
observed; it remains an affected-package obligation, not another structural ID.

Source-only safety priorities remain **A02** (durable account-policy loss),
**K02** (CACHE metadata without read/write leases), **O09** (missing minimum
active timestamp reporting), and the coupled **O01/O02/I04/D01–D08** identity,
upgrade, schema and durable-job owners. Actual mixed-node policy bypass,
premature GC and stale-cache reads were not reproduced. O09 must precede GC
activation: a Go peer running GC also needs Rust transaction/cursor/internal
and recent-schema timestamps. Starting a Rust GC loop alone would not fix it.

## Retired code is not a current live defect


The earlier review is retained as historical evidence, with a notice linking
here. Its descriptions of admitted repartition row loss, ignored IMPORT options
and fabricated configuration must no longer be read as current behavior.

| Finding | Retired behavior confirmed now | Still missing |
| --- | --- | --- |
| D01 | Both RANGE-to-HASH and plain-to-HASH ALTER now refuse; old rows remain visible, and subsequent ordinary inserts work. Cluster lowering/planning and the thread-local metadata handoff were removed in d77a941f46. | General SQL still bypasses the durable worker except CHECK. Online reorganization, recovery and all D01–D08 obligations remain. |
| E05 | IMPORT now refuses before target changes; the private CSV/precheck/INSERT and SELECT rewrite are gone in ba62b48ee1. | Go file-import controller/jobs/tasks and SELECT importer, with options, encoding, cancellation and recovery. |
| I01/I02 | The compiled config capture/warning and unconditional tikv/store1 row are gone in 68d6de685a. Both live server tables now propagate discovery failures. | Live config retrieval, six other discovery sources, peer fanout, filtering, redaction and complete runtime lifetime. The previous removal's four red/green tests remain the error/topology evidence; this review does not claim to rerun them. |
| P06 | The isolated opt-in HTTP/2 preface experiment now passes its 14 cases; the original rejected accessor remains separately reproducible. | No production grpcutil integration or whole parent acceptance. TLS, options, backoff, GOAWAY, interceptors and connection-cache obligations remain. |

Fresh SQL controls retain the repaired UPDATE privilege denials, alias merge,
USING layout and FK enforcement. C01's bounded cache/flush diagnostic does not
reopen the removed competing session-cache owners. The eight repaired IDs
C01, E01, O12, P01, P02, P04, P05 and T03 keep their exact receipt limits;
their complete suites were not all rerun.

## Complete unresolved assignment


Every unresolved ID occurs once below. Workstreams are dependency groupings,
not acceptance units: obligations sharing a Go package still require one
complete inventory, integration decision and receipt.

| Workstream | All unresolved IDs | Review conclusion and retirement prerequisite |
| --- | --- | --- |
| W01 Native PD | P03, P06, P07 | Keep published deadline/batch/retry and other prerequisites. Complete service-mode/TSO discovery, public joined close and request-scoped transport. Native close still stops region/TiKV owners only; retry_mut still holds the cluster write lock across RPC await. Retire serialization only with safe leader replacement and complete request lifetimes. |
| W02 Storage routing | T02, T04 | TiDB/native routing remains duplicated; read-side health admission still depends on the update mutex. Move transaction, snapshot, DistSQL and MPP consumers to native routing/health/RPC owners before deleting TiDB algorithms. Preserve the repaired shared retry budget and PD circuit breaker. |
| W03 Shared SQL/table | S01, S02, S03, S04, A01, E02, E03, K01, K03, T01 | The optional configured one/two-table route and static two-table catalog remain live. Migrate them to ordinary session/compiler/storage adapters, with resolved FK/handle metadata, chunked writes, active conversion/assertion policy and allocator selection. SQL TxnManager, native KV transactions and table policy remain distinct as in Go. |
| W04 Schema/identity/GC | O01, O02, O03, O09, I04, K02 | Compose leased numeric identity, versioned upgrade/schema ownership, active timestamps and cached-table coordination. Latest-only catalogs and boolean bootstrap cannot replace these contracts. Prove timestamp protection before enabling collection; keep separate DDL delete-range registration and GC execution. |
| W05 Durable DDL/placement | D01, D02, D03, D04, D05, D06, D07, D08, D09, D10, D11, F01, F02, F03, O14 | Direct publication, serial scheduling, overwritten control states, lost error persistence, mandatory MDL and incomplete delete-range ownership remain. Migrate submitter/worker/waiter and full action lifecycle together, then retire direct publication and classic poll-owned placement. MV/MLog remain disabled seeds; do not activate them to fill syntax gaps. |
| W06 Accounts/wire/config/admin | A02, A03, A04, N01, N02, N03, N04, N05 | Preserve complete durable account policy/TLS state and source bytes; connect command permits, config consumers, remote KILL and HTTP handlers. Replace NodeConfig authority only when effective source options actually reach their consumers. Numeric identity/discovery gates remote KILL. |
| W07 Cache owners | B01, B02, C02, C03, C04 | Binding/coprocessor FIFO behavior and LFU Stretto admission mismatch remain; binding reload loses history and lacks the source incremental/GC/usage owner. Complete pinned Ristretto once, then each consumer's budget/config/lifetime. The instance plan cache and repaired session LRU have distinct Go designs and must not be deleted indiscriminately. |
| W08 Planning/typed workers | Q01, X01, E04, E06 | Cascades selection/memo, shared SQL/PB typed scalar/vector construction, eligible parallel Apply and joined projection cancellation remain incomplete. Preserve ordinary/merge shared candidate selection; unrelated removals neither certify the planner nor prove that old duplication returned. |
| W09 MPP/Compute | M01, M02, M03, M04, M05 | Scan-only single-task lowering, range envelopes with no continuation, full-stream buffering, private plaintext transport and absent Compute topology remain. Full fragments/ranges/streaming/cancel/security ownership must replace this path through W02/W03/W08, not another private MPP retry layer. |
| W10 Domain jobs/resources/bulk | O04, O05, O06, O07, O10, O15, E05, E07 | TTL, resource/runaway, RU history, statistics GC, DXF, cross-keyspace and import/BR runtimes still lack complete startup/producer/recovery/shutdown composition. Existing helpers, hooks and stores should be integrated, not removed as if they were competing live managers. |
| W11 Live information/observability | I01, I02, I03, O08, O11, O13, O16, O17, O18, O19 | Missing retrievers/fanout and plan-replay/TopSQL/AZ/workload-learning/telemetry/summary/load-metric producers remain. Remove constant providers through complete live retrievers. Keep legitimate schemas, metrics, workers and stores; a workload-repository sampler is not the workload-learning owner. |
| W12 Inference | X02 | Provider registry, batching, cache, expression/runtime integration and isolated cancellation remain absent. Keep explicit unaccepted evidence; do not invent providers or expose partial EMBED_TEXT execution. |

## Repair order and what should not be deleted yet


First address active data/security correctness in complete source-package
scopes: DDL control/error/atomicity, account policy, charset and generated-value
conversion. Coordinate schema/identity/timestamp/cache safety with DDL instead
of treating each as a local flag. These independent correctness packages need
not wait for every PD transport experiment.

In parallel as a dependency plan, finish native PD and routing ownership, then
the shared session/table and schema consumers that permit retirement of the
configured SQL route, direct DDL publisher and duplicate TiDB transport. This
sentence describes independent workstreams, not a partial-package dispatch.
Complete cache dependency semantics before replacing the four source consumers;
retain the distinct instance/session plan caches. Compose Domain/runtime
services whenever their complete prerequisite owners are ready.

Do not blindly remove constructors, working workers, transaction layers, cache
instances or protocol helpers. For each live duplicate, trace construction,
every call, state/error/retry/cancellation path and close through the replacement
before deletion. For unsafe admitted unsupported behavior, withdrawal can be a
separate containment decision, but the Go-supported operation remains open.
No additional runtime deletion was made in this review.

For sysbench/TPC-C/YCSB, PD RPC serialization, routing, table policy, cache
admission and chunked DML remain relevant structural targets. TPC-H also depends
on MPP range/task correctness, streaming, parallel Apply and typed execution.
No benchmark or speedup is claimed. Correct results, warning/error behavior,
isolation and complete package acceptance precede performance claims.

## Validation and limits


Use run-expanded-probes.py's existing run function with the six retained probe
files, example prefix post_removal_, and separate absolute output paths. It
creates temporary examples and removes each in a finally block. From rust/ the
executed commands were:

    cargo run --locked -p tidb-session --example post_removal_expanded_ownership
    cargo run --locked -p tidb-session --example post_removal_subsystem_structure
    cargo run --locked -p tidb-session --example post_removal_session_ownership
    cargo run --locked -p tidb-session --example post_removal_remaining_structure
    cargo run --locked -p tidb-server --example post_removal_expanded_server
    cargo run --locked -p tidb-session --example post_removal_partition_structure

Compiler logs are /private/tmp/post_removal_*-build.log. The evidence checker
validates unique IDs, counts, full 77-ID assignment, Markdown/JSON agreement,
prior/current source blobs, local report links and temporary-example cleanup.
The check command is python3 /private/tmp/check-post-removal-review.py.
Publication requires git diff --check, the actual hook's locked server build,
and cd rust && cargo build --locked -p tidb-server after the final commit.
The living [audit ExecPlan](../../remaining-structure-audit-execplan.md) records
gate outcomes. No Go/Bazel inputs or production code changed; no new unit-test,
failpoint or Bazel preparation obligation is introduced by these review files.

Not rerun: full original Go/native suites, distributed TiKV/TiFlash behavior,
TLS rotation, durable crash/owner handoff, cross-keyspace services, GC/cache
lease interaction, platform variants or workload benchmarks. Source-only
consequences remain inferences. The review updates what is known; it does not
fix the 77 remaining owners or certify the remaining inventory.
