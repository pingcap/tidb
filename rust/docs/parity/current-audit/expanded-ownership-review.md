# Expanded production-owner review, 2026-09-30

This review compares integration `13689e0b133b32af45862f5a69ee99382471fc12`
with TiDB Go master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`. Both were
freshly pulled/fetched. Client-rust master was independently checked at
`b2b3783cee3982ad39c0a70df2654176aafc784d`; this audit changes no dependency.
Go comparisons use `git show origin/master:<path>`, not integration's Go files.

The [full register](structural-findings.md) now has 54 tracked findings:
50 unresolved, including one partially repaired finding, and four repaired
ownership/contract findings. Thirteen findings are new here. This is a
review checkpoint, not a claim that every mismatch has been found, a repaired
package, or a production fix. No runtime implementation was changed.

## Complete current register index

| Area | IDs | Current disposition |
| --- | --- | --- |
| Privilege compilation and account policy | A01–A04 | Four open, new |
| Binding cache and maintenance | B01–B02 | Two open, new |
| Session/instance physical cache | C01–C02 | C01 ownership repaired; C02 open |
| DDL submission, scheduler, transitions, GC and atomicity | D01–D11 | Eleven open; D09–D10 are unintegrated seeds |
| DML/executor handoff, parallel Apply and import | E01–E05 | E01 repaired, E02 partial, E03–E05 open; E05 new |
| TiFlash replica lifecycle | F01–F03 | Three open |
| Runtime information-schema providers | I01–I03 | Three open |
| MPP fragments, ranges, streaming and transport | M01–M04 | Four open |
| Wire encoding, command admission and config | N01–N03 | Three open, new |
| Domain/process services | O01–O09 | Nine open; O07–O09 new |
| Protocol contracts and PD discovery | P01–P03 | P01/P02 schema ownership repaired; P03 open |
| Alternate configured session pipelines | S01–S02 | Two open |
| Transaction, routing and operation lifetime | T01–T03 | Three open |

Each ID has concrete Rust evidence, its Go owner, impact, validation limit
and the boundary to migrate before deleting anything. A source inventory
entry is not an accepted package. Earlier repairs remain valid at their
recorded revisions; this review does not rerun all their validations.

## Executable observations

The retained [session probe](expanded-ownership-probe.rs) uses real Session
execution, attached privilege registries, independent accounts and a temporary
two-row CSV. Its [output](expanded-ownership-probe.txt) records the following:

| Finding | Setup and actual Rust result | Go comparison |
| --- | --- | --- |
| A01 | An account granted only global SELECT receives `PrivilegeCheckFail("Update")` for single-table UPDATE and qualified joined `SET a.x=12`. The same join with `SET x=13` succeeds and persists 13. | `buildNewAssignments` resolves the target's schema/table and appends UpdatePriv for each assignment; qualifier spelling does not remove the check. |
| Disproved A01 candidate | A registry with SELECT only on column x cannot execute `SELECT x`; it receives table-level denial. | Rechecked 2026-10-01 on master 93a01d31f6da205ae4bf376825293903a6899fdb: Go also returns 1142. `buildDataSource` records table SELECT with an empty column. The earlier claim that this SELECT planner supplied a column request was incorrect; see `update-privilege-repair.md`. |
| A04 | PASSWORD HISTORY 3 is accepted, but a change from First!1234 to Second!1234 and back succeeds. The persisted history setting is NULL. These are dummy probe credentials. | `simple.go` stores policy/history and `checkPasswordHistoryRule` rejects a password in the retained history. |
| A03 | CREATE USER with REQUIRE X509 is refused with an explicit unsupported error. | Go's TLS/account owners support CA verification and X509 constraints when configured. |
| E05 | IMPORT INTO with skip_rows=1 imports both `1,10` and `2,20`, returning affected=2. | `importer/import.go` maps the option to IgnoreLines; the controller/import task owns decoding and job completion. |

The [server probe](expanded-server-probe.rs) uses the existing public
PipelineSessionFactory, ConfiguredUserStore and MySQL connection owners. It
opens one ephemeral localhost connection, performs a protocol-4.1 handshake
with Latin-1 collation, sends raw non-UTF8 query bytes, then quits and joins
the server thread. Its [output](expanded-server-probe.txt) records:

| Finding | Actual Rust result | Go comparison |
| --- | --- | --- |
| N01 | Raw byte E9 in `SELECT HEX('<byte>')` returns C3A9 over the real wire. | `pkg/parser/charset/encoding_latin1.go::Transform` returns the original bytes. Rust's wire conversion changes literal content before parsing. |
| N03 | Valid token-limit, performance.stats-lease and security.ssl-ca TOML options are refused by UnsupportedConfigOptions. Default auto_tls is true. | Master's Config accepts these fields and defaults AutoTLS to false. Supporting the fields also requires their real runtime consumers. |

N02's token-limit CLI flag is accepted, but that observation alone does not
prove a concurrency failure. The missing command-token consumer is a source
finding: the command loop, ConcurrentSqlNode and server startup have no
limiter construction/acquisition/release. No concurrent stress test ran.

The probes complete with exit zero because they are diagnostics. They do not
assert that the observed incorrect behavior is correct and are not passing
parity tests. The first local session-probe build used a private `bit()`
method; it was corrected to the public `mask()` API. An initial account
probe lacked a registry; the retained version installs root identity and
the real registry. Only the corrected final outputs are evidence here.

## Source-only findings and limits

A02 traces every field of `LoadedUser` and `ClusterPrivileges`, the full
loader, `registry_from_cluster`, the default registry records, reload
publication and `authenticate_admitted`. The loader has no global_priv
record collection, and USER_COLUMNS/LoadedUser omit the original password
time/lifetime and user-attribute policy. Reconstructed users receive new
defaults. Thus a Go-created REQUIRE SSL/X509 policy cannot reach the live
account check; similarly, expiry and automatic-lock policy cannot retain
their durable values. No remote account, real credential, production
authentication attempt or mixed-node security experiment was used.

A03 separately traces the transport: `MysqlServerTls::from_material` uses
`with_no_client_auth`, loads material once, and carries no verified-client
chain into authentication. Go's `LoadTLSCertificates` carries CA,
minimum-version and reload policy, and `checkSSL` consumes verified chains
and certificate properties. Config projection/defaults belong to N03;
losing persisted account policy belongs to A02.

B01 compares the actual CostLruStore implementation, not only its historical
module comment, with master's Ristretto construction and Get/Set/Wait
calls. Rust reads do not change access history and insertion evicts from
the front of a VecDeque. The cluster reload recreates that store in digest
order. This is a confirmed policy difference; no exact random upstream
victim or throughput ratio is asserted.

B02 traces the entire BindingReloader loop and all `gc_global_bindings`
callers. The reloader only refreshes. GC runs in binding-write operations;
there is no owner timer or production call to the usage writer helper.
The full storage reload has no master's overlap-aware timestamp watermark.
The existing independent global-binding transaction writer is already wired
and is not a duplicate to remove.

O07 follows `ClusterSessionFactory::gc_stats` through its implementation and
every caller: production provides the method, while the external calls are
direct tests. Boot starts usage, auto-analyze, analyze-job cleanup and
historical-stat workers. AnalyzeJobsCleanupWorker's `gc_interval` deletes
job history, not table/column statistics. It does not close the missing
Go `gcStatsWorker` lifecycle.

O08 checks the plan-replayer definitions and consumers across all Rust crates.
The domain helpers have unit fixtures, but no server constructs the
collector/dumper/archive-GC pipeline. Session capture flags are not task
submission. Go's `StartPlanReplayerHandle` and `DumpFileGcCheckerLoop`
provide these retained and stopped workers.

O09 checks server-info synchronization, startup, all Rust crates and the
vendored native client for min-start-TS publication. It is absent. Go's
`pkg/domain/serverinfo/syncer.go::ServerInfoSyncLoop` calls its reporter,
and infosync includes active sessions, cursors, internal transactions and
recent schema timestamps. Rust's server-info/topology lease publication
does not report that minimum. The resulting difference in GC protection
requires mixed-node failure testing; premature collection was not observed.

E05's diagnostic is in-process. The server supplies ordinary Session
execution, but a real TiKV import was not attempted. The complete source arm
never reads import options or column assignments, materializes the local
file, and executes generated INSERT text. Go has controller validation,
standalone/distributed task submission, import-specific encoding and job
status/cancellation. Fixing skip_rows alone would leave this architecture.

## Controls and rejected candidate findings

The following are not counted as new mismatches:

- `GRANT SELECT(a)` succeeds. Column privilege lists are split before the
  old rejection branches in `resolve_scoped_privs`. Keyword hits in those
  branches alone would falsely report missing column GRANT support.
- The privilege reloader and watch are live. The loader's old "one-shot"
  comment is stale and does not establish missing reloads.
- Auto-analyze, usage and analyze-job cleanup workers are started.
- The process resource manager is started and stopped in server startup.
- GBK/GB18030 decoding uses the encoding registry. N01 is the separately
  verified Latin-1 byte-preservation contract, not a claim of no decoding.
- `performance.run-auto-analyze` is removed in Go as well. Its diagnostic
  refusal is a passing control, unlike the three valid settings above.
- Binding plan evolution's LLM predictor is also a stub in Go. A matching
  upstream stub is not a Rust-only feature gap.

## Coverage boundary and validation

Artifact enumeration was refreshed for all 856 TiDB package directories,
41 client-go, 41 kvproto, 24 PD-client and seven etcd-API directories. It
retains original tests, platform/generated/build inputs, fixtures and
unassigned artifacts. All 83 Rust manifests remain awaiting current-master
acceptance. The search file now has 2,047 candidate lines, not 2,047 defects.
Inventories were unchanged except for candidate line locations/removals
following the prior cache repair.

The newly traced packages are planner/core privilege compilation,
privilege/privileges, executor account/import execution, executor/importer,
bindinfo, domain/serverinfo/infosync, server input/config/admission and their
Rust consumers. Package-level doc.go entrypoints were checked where
applicable. This selected flow review does not accept any entire package.
Complete original-test/variant review of all inventoried packages, other
external modules, all historical failed validations and workload benchmarks
still remain. Historical receipts do not fill these review gaps automatically.

Exact commands from the repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    git ls-remote https://github.com/ngaut/client-rust.git refs/heads/master
    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/docs/parity/current-audit/run-expanded-probes.py
    rustfmt --check --edition 2021 rust/docs/parity/current-audit/expanded-ownership-probe.rs rust/docs/parity/current-audit/expanded-server-probe.rs
    git diff --check
    make lint

The runner executes, from rust/, these exact commands and removes both
temporary example sources in a finally block:

    cargo run --locked -p tidb-session --example expanded_ownership_audit
    cargo run --locked -p tidb-server --example expanded_server_audit

Both probes completed. Build logs are
`/private/tmp/expanded_ownership_audit-build.log` and
`/private/tmp/expanded_server_audit-build.log`. Existing compiler warnings
and the local jemalloc `Malformed conf string -- prof` diagnostic remain
visible in the logs; neither prevented completion.

Formatting and diff checks passed. Root `make lint` passed; its first
sandboxed attempt could not resolve proxy.golang.org while installing the
pinned lint tool, and the network-enabled rerun succeeded. The final log is
`/private/tmp/tidb-expanded-audit-lint.log`. A separate Python check verified
54 unique IDs, all new receipt links, the inventory counts, runner syntax
and removal of temporary example sources.

Publication still requires the real commit hook's locked server build and
a separate fresh locked server build before push. Outcomes are recorded in
the living ExecPlan and publication response. No full Go suite, real
TiKV/TiFlash cluster, TLS rotation, GC retention, parallel admission stress,
or sysbench/TPC-C/TPC-H/YCSB benchmark was run. Audit artifacts introduce no
production behavior change and do not fix the reported risks.


2026-10-01 follow-up: the reproduced joined UPDATE bypass and separate EXPLAIN
privilege bypass are repaired by resolved target checks. A01 remains open for
the remaining AST visit collectors and complete planner/session ownership;
see [repair receipt](update-privilege-repair.md). The original probe above is
historical fail-before evidence, not the current result.
