# Complete known remaining structural register — 2026-10-01

Historical review at integration 771e62b287. The
[follow-up at 313b3cfea3](structural-review-followup.md) supersedes its current
status descriptions: P06 now has deadline/cancellation/join prerequisites, while
the public PD owner remains unfinished. Keep the observations below at their
recorded baseline; the follow-up has separate fresh outputs for all six probes.

There are **77 known unresolved structural findings**, including partial repairs,
in the [full register](structural-findings.md). Eight other entries are repaired,
for 85 tracked entries. This review adds 11 previously unregistered boundaries
and corrects the stale classification of T03. No production repair is included.

The register is the complete known list at these pins, **not proof that all
semantic mismatches have been discovered**. The package inventory is complete
for the scopes enumerated below; semantic acceptance, original-test equivalence,
generated/platform/build variants and distributed integration are not complete.
Missing owners are included: a helper or flag without its production lifecycle
does not satisfy Go parity. Rust memory management need not imitate Go objects.

## Source pins and continuity

Both implementation branches were pulled before review and were already current.
TiDB integration starts at `771e62b2871890eeae2296cbeed04a19b1316201` on
`hparser-integration`; fetched Go master is
`93a01d31f6da205ae4bf376825293903a6899fdb`. Native client-rust is published at
`6f663b396552eec6d1bfad76b65f813e317884a4`. The normative external versions are
the modules selected by that Go master, including client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee` and PD client
`v0.0.0-20260805103528-afa43111d149`.

[Source continuity](structural-source-continuity.json) records 135 literal Rust
path references from the register, their current Git blob/tree IDs, and changes
since the preceding register's integration baseline `dae65456f9`. Shorthand
references remain in the finding text. Unchanged source retains the earlier
source evidence; it does not mean an old reproduction was rerun. Changed paths
were reconciled with the intervening repairs. For example, dispatch changes
resolve UPDATE privileges without adding IMPORT, session migration, stale reads
or cluster-table fanout; boot changes add global-config synchronization without
adding the other Domain workers. The LFU, candidate and health repairs retain
their explicitly open dependency/integration boundaries.

Only 11 upstream artifacts changed between the old inventory's Go `e953a09d9d`
and this master. The substantive new change is the skyline range-count threshold
and its variable/upgrade propagation; the other changes are original tests.
External module pins and inventory sizes are unchanged. Fresh Go master source,
not the integration checkout's older Go files, was used for the comparison.

## Every known unresolved finding

Each ID below has Rust evidence, its Go owner and the required replacement
boundary in the full register. IDs describe review findings, not independent
packages or necessarily independent root causes.

| Area | Complete remaining list |
| --- | --- |
| Authorization/accounts | **A01** shared planner privilege visits remain incomplete after UPDATE repair; **A02** durable account-policy image loses security state; **A03** MySQL TLS/client-certificate policy and reload lifecycle; **A04** password-history/reuse policy is accepted without enforcement. |
| Bindings | **B01** FIFO cache differs from shared Ristretto admission; **B02** full-image reload, write-triggered GC and missing usage-persistence lifecycle. |
| Other caches | **C02** instance plan cache is uncomposed; **C03** coprocessor FIFO/configuration differs from Go; **C04** statistics LFU's Stretto admission/metrics differ despite repaired lifetime. |
| Durable DDL | **D01** normal SQL bypasses persistent jobs; **D02** index backfill lacks online/resumable phases; **D03** serial scheduler lacks the shared conflict/worker lifecycle; **D04** pause/cancel state gates; **D05** shared error/retry persistence; **D06** CHECK terminal cancellation; **D07** MDL-disabled synchronization; **D08** delete-range registration; **D09** MV build transaction/reorg owner; **D10** MV/MLog dependencies and rollback; **D11** multi-action ALTER atomicity. D09/D10 remain disabled seed paths. |
| DML/execution | **E02** FK physical-plan integration remains after runtime repair; **E03** materialized DML handoff and reconstructed target identity; **E04** serial Apply despite parallel metadata; **E05** private IMPORT loop lacks source options/job lifecycle; **E06** projection Close does not cancel/join workers; **E07** BRIE execution/job lifecycle. |
| TiFlash placement | **F01** poll-owned placement maintenance; **F02** replica availability/partition lifecycle; **F03** progress cache, unavailable-table backoff and PD HTTP discovery. |
| Information schema | **I01** dynamic providers return captured/default facts; **I02** incomplete/fabricated cluster discovery; **I03** cluster tables lack peer fanout; **I04** versioned/lazy schema-cache owner. |
| Table policy | **K01** single-point auto-ID service selection; **K02** cached-table flags lack shared leases; **K03** generated-column conversion loses statement policy/warnings. |
| MPP | **M01** private scan-only plan/task generation; **M02** range gaps and completeness lost in one envelope; **M03** full-stream materialization and missing remote cancellation; **M04** private plaintext transport/recovery; **M05** absent TiFlash Compute topology/dispatch owner. |
| Wire/server | **N01** byte-preserving charset ingress; **N02** command token admission; **N03** second configuration whitelist/defaults; **N04** remote KILL forwarding; **N05** administrative HTTP/configuration owner. |
| Domain identity/storage | **O01** numeric server-ID lease lifecycle; **O02** versioned bootstrap/upgrade; **O03** store GC worker; **O04** TTL job/task managers; **O05** resource-control/runaway controller; **O06** RU-history writer; **O07** statistics GC; **O09** node minimum-active-start-TS reporting. |
| Domain execution/observability | **O08** plan-replayer lifecycle; **O10** DXF managers; **O11** TopSQL profiling/reporting; **O13** closest-adaptive AZ policy; **O14** PD affinity groups; **O15** cross-keyspace runtime/session manager; **O16** workload-learning analysis/cache worker; **O17** telemetry collection/log loop; **O18** live statement-summary producer and persistent-mode integration; **O19** statistics-load metric producers. |
| PD/native client | **P03** service-mode/independent-TSO discovery; **P06** native timestamp deadlines/cancel/join lifecycle; **P07** native PD metadata RPCs serialized by a network-duration write lock. |
| Optimizer | **Q01** Cascades flag lacks integrated memo exploration/implementation. Full ordinary/merge candidate package acceptance also remains a validation obligation, not an assertion that every earlier removed duplicate returned. |
| Sessions | **S01** separate configured session/planning pipeline; **S02** static two-table catalog in that mode; **S03** session migration; **S04** shared historical timestamp/schema provider. |
| Transaction/routing | **T01** generic insert assertion policy belongs in the table owner; **T02** remaining competing cache/recovery/RPC owners and missing TiDB health-event wiring; **T04** native active-feedback admission incorrectly depends on the feedback update mutex. |
| Expressions | **X01** split SQL/PB typed construction and incomplete vector execution contract; **X02** missing domain-owned inference/provider/cache/batcher runtime. |

The repaired IDs excluded from those 77 are **C01, E01, O12, P01, P02, P04,
P05 and T03**. The five earlier transaction review defects also retain their
repair receipts. A broader open routing finding does not reopen those defects.

## Newly consolidated evidence

The added IDs are T04, P06, P07, O14, O15, O16, O17, O18, O19, X02 and M05.
Several already existed as separate package-boundary receipts, so these are
new to this register, not all newly introduced regressions.

- **T04:** native `locate.rs:307` obtains `feedback.try_lock()` before deciding
  whether a feedback RPC is due. On contention it returns false. Go
  `internal/locate/store_cache.go:964` reads atomic feedback/time/score state,
  can perform the RPC, and only then tries the update lock. The recently fixed
  nonblocking writes remain correct; this read-side admission difference needs
  a separate repair. No outage or stress result is claimed.
- **P06:** native `pd/timestamp.rs:56` discards the spawned worker handle;
  `:66` and `run_tso` have no request timeout. `pd/retry.rs:456` awaits this
  operation under a cluster read guard. `pd/client.rs:1665` closes cache/TiKV
  workers but has no PD/TSO shutdown call. Go PD `clients/tso/client.go:151`
  cancels and joins its owner, and `clients/tso/dispatcher.go:325` starts a
  deadline watcher. A completed retry count cannot bound a stalled await.
- **P07:** native `pd/retry.rs:259` keeps the global cluster write guard across
  `$call.await`; region/store/scan RPCs use that macro. Go PD `client.go:715`
  obtains a service client and sends `GetRegion` without retaining a global
  write lock over the RPC. This is an unnecessary serialization point; no
  throughput gain is claimed before measurement.
- **O14/O15:** current Go infosync initializes the affinity manager and DDL
  maintains its groups; Domain initializes/acquires/closes cross-keyspace
  runtimes. Rust carries affinity metadata and keyspace encodings, but the
  corresponding production owners remain absent. Rechecked the current
  entrypoints against the existing `domain_affinity` and `domain_crossks`
  receipts; codec support and CPU affinity are not substitutes.
- **O16/O17:** Go Domain starts the owner-gated read-cost learner and cache
  refresh loop, and its telemetry loop performs an initial/periodic log report
  plus window rotation. Current Rust has settings/admission helpers, but no
  production consumers implementing those loops. Go's telemetry target here
  is log output, not an external upload.
- **O18:** Go `ExecStmt.SummaryStmt` populates `StmtExecInfo` and calls the v2
  dispatcher, which selects v1 or persistent v2. Rust's
  `tidb-exec/src/adapter.rs:288` explicitly ends at a population boundary.
  All production callers were searched: no summary add/setup/close pipeline
  exists, while SQL readers/tests can consume injected records. The SQL probe
  below confirms the missing in-process producer.
- **O19:** the Rust load-counter/read-histogram references consist of definitions
  and bootstrap materialization. There are no producers at the live
  request/dedup/wait/read events. Go `stats_syncload.go` records them at those
  lifecycle points. The workers themselves exist and are not counted absent.
- **X02/M05:** the inference and TiFlash Compute boundary receipts remain
  consistent with current production scans. Go Domain owns `EmbedFn` creation
  and close; Go batch coprocessor construction consumes the configured topology
  fetcher and dispatch policy. Rust's variable/endpoint carriers do not compose
  either runtime. No external provider or AutoScaler was contacted.

## Executed SQL diagnostic and fresh upstream drift

Source: [remaining-structure-probe.rs](remaining-structure-probe.rs).
Output: [remaining-structure-probe.txt](remaining-structure-probe.txt).
The runner copies the diagnostic temporarily to
`rust/crates/tidb-session/examples/remaining_structure_probe.rs`, invokes:

    cd rust
    cargo run --locked -p tidb-session --example remaining_structure_probe

It removes the temporary example in a `finally` block. The successful run sets
both `GLOBAL tidb_enable_stmt_summary` and `GLOBAL tidb_stmt_summary_internal_query`
to ON, executes CREATE/INSERT/SELECT, verifies both switches read back as 1,
then observes zero rows in `information_schema.tidb_statements_stats`. The
in-process observation plus the production call graph supports O18. This does
not substitute for a live multi-node/wire or persistent-file test.

The probe's first attempt used a session-scoped assignment and a misspelled
internal-query variable; those setup errors were corrected before retaining
the output. They are not counted as findings.

Fresh Go master also adds `tidb_opt_range_max_count` (default 1000), its
statement hint/session/global propagation, skyline `compareEqOrIn` threshold
and bootstrap version 318. Rust has no such variable and the probe receives
UnknownSystemVariable. Record this **additional confirmed upstream feature
delta separately from the 77 structural findings**: it is not a new root-owner
mismatch by itself. Migrate the whole source package/configuration chain when
repairing it; do not introduce a private hardcoded threshold. Bootstrap owner
O02 remains relevant.

## Coverage and rejected stale allegations

Regenerated inventories enumerate **856 TiDB Go package directories, 41
client-go, 41 kvproto, 24 PD-client and seven etcd-API directories**, plus all
**83 Rust crates**. The scope generator assigns each exactly once. This totals
969 inventoried Go package directories; it does not accept 969 packages.
TiPB is additionally covered by the maintained descriptor provenance checks.
Other external dependencies need their own inventories before acceptance.

The refreshed keyword candidate file contains **2,043 lines**, not defects.
[Boundary-receipt candidates](boundary-receipt-candidates.tsv) retain a separate
101-line/84-file search over historical non-batch receipts. That search includes
already repaired code, language-specific boundaries and test infrastructure;
it is neither an 84-defect claim nor semantic verification of every receipt.
No ignored test is automatically treated as an unimplemented feature.

Current production code disproves broad absence claims for workload-repository
workers, memory arbitration, statistics load workers, automatic analyze,
native pessimistic-primary-mismatch handling and native health-event workers.
Keep those owners. In particular, a `TODO` on primary mismatch is stale:
native `get_txn_status_from_lock` handles the pessimistic case and has a
regression. TiDB health integration is still open even though native client
health integration exists. Current schema diff loading also does not establish
the distinct versioned/lazy owner tracked by I04.

The previous partition/statistics test failures remain **root-cause review
candidates**. They were not rerun here and are not counted as ten independent
structural defects. No new notifier exclusion is justified by that old output.

## Repair order and validation limits

Prioritize consistency and security boundaries: durable DDL/delete-range/GC
and minimum-start-TS reporting, table assertion/cache-lease policy, durable
account state and charset/generated-value policy. Then complete the shared
client routing/PD lifecycle and migrate all TiDB consumers before removing the
remaining duplicate cache/RPC owners. Domain services, shared cache admission,
planner/executor handoffs and MPP must be accepted with their whole Go packages.
Empty or missing owners need integration, not deletion of useful helpers.

For workload performance, the evidenced structural targets are PD metadata
serialization, shared region/health ownership, Ristretto admission, streaming
DML/MPP, parallel Apply and typed vector execution. There are **no measured
sysbench/TPC-C/TPC-H/YCSB improvements** in this audit, and no performance fix
should change Go-visible behavior.

Validation run from the repository root:

    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/build-structural-coverage.py
    python3 /private/tmp/tidb-remaining-structure-check.py
    rustfmt --check --edition 2021 rust/docs/parity/current-audit/remaining-structure-probe.rs
    git diff --check
    make lint

All pass. The one-off checker verifies all 85 unique IDs, exact Markdown/JSON
agreement, each of the 77 open entries listed once, links, the 135 source
references, matching source/module pins and removal of the temporary example.
Lint includes the maintained protocol checks and dashboard validation. Logs:
`/private/tmp/tidb-remaining-structure-{probe,check,lint}.log`.

The actual pre-commit hook and the separate post-commit/pre-push locked server
build remain required publication gates; their result is recorded in the
[ExecPlan](../../remaining-structure-audit-execplan.md). There is no Go/Bazel
change, so Bazel preparation and failpoint enablement are inapplicable here.

The source-only findings do not claim live failure injection, benchmark results
or whole-package acceptance. Runtime production code and the client-rust
dependency pin are unchanged by this audit. No earlier regression suite,
distributed SQL/TiKV/TiFlash experiment or workload benchmark was rerun.
