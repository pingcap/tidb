# Subsystem ownership review, 2026-09-30

The latest request is to find **all structural mismatches**, across the complete
Rust implementation and native client-rust. This follow-up adds 18 established
ownership/contract gaps to the register: **72 tracked, 68 unresolved, four
repaired**. It does not certify exhaustive semantic review or package acceptance.
No production code or dependency is changed by this audit.

## Baseline and scope

Integration was pulled at `dae65456f9528d1747d47daf542e890ba8db952c`; it was
already current. Freshly fetched TiDB master remains
`e953a09d9d5e29e60c62f42d3aacebb819af49a5`. The native repository's master
was checked remotely and remains `b2b3783cee3982ad39c0a70df2654176aafc784d`.
The Go reference is fetched master, not integration's Go working tree. Selected
external module versions remain those in [the register](structural-findings.md).

The [scope matrix](structural-coverage.md) assigns every one of the 83 Rust
crates and 856 inventoried TiDB package directories to a primary review queue.
Its generator rejects missing/duplicate Rust assignments and mixed Go baselines.
It preserves an explicit queue for unassigned upstream product surfaces and
links the 113 recorded external-module package directories. Other external
modules still need inventories. A primary queue is an organizational grouping,
not a one-to-one Go-package/Rust-crate translation claim.

This pass followed entrypoints and consumers through optimizer selection,
expression construction/batch evaluation, table allocation/caching/generated
writes, catalog loading, session migration/historical reads, projection close,
BRIE, status/KILL, DXF, TopSQL/global-config and adaptive replica reads. It also
checked generated-protocol consumers outside the descriptor comparison. The
full per-package production/build/platform/generated inputs, original tests,
fixtures and support artifacts remain acceptance obligations. None of the
coverage rows is marked complete merely because its entrypoint was inspected.

## New findings and shared repair boundaries

Exact Rust source anchors, Go owners, severity, consequences and limitations
are in the register. The additional findings group as follows:

| IDs | Structural boundary | Required migration |
| --- | --- | --- |
| Q01, X01 | Optimizer/signature selection and typed execution | Compose Cascades selection/memo/rules/implementation; share selected builtin construction and its scalar/vector contracts. Retain native Rust ownership and intentional Go fallbacks. |
| C03 | Coprocessor cache configuration and admission | Consume effective configuration and preserve the full access/admission/cost/publication owner. FIFO is not the Ristretto policy used by Go. |
| K01–K03 | Table identity, cached-table leases and mutation policy | Select the eligible single-point auto-ID service; carry table cache leases through reads and commits; route generated values through the current statement's conversion/error/warning policy. |
| I04, S04 | Schema versions and historical reads | Compose the versioned/lazy catalog and a single historical timestamp/schema/safe-point provider. Existing schema diffs and temporary-table overlays must be retained. |
| S03 | Session migration | Move variables, prepared statements, bindings and validation together through the registered encode/decode handlers. |
| E06 | Projection completion | Join/cancel queued and running evaluation work at Close, including early exit and reopen. Dropping an Arc-backed receiver does not wait for workers. |
| E07, P04 | BR executor and complete protocol consumers | Compose the BR job owner and retain full generated backup metadata through range grouping/cloning and import handoff. Existing helpers are seed evidence. |
| N04–N05 | Remote connection control and administrative HTTP | Consume shared identity/config/discovery; route KILL to the correct node; compose Go's administrative handler packages and dependencies. |
| O10–O13 | Domain background service composition | Start and stop DXF managers, profiling/reporting, global-config publication and adaptive replica policy with their real dependencies and lifetimes. Metric registration alone is insufficient. |

Deletion follows migration of responsibility. For example, K01 needs both
allocator modes, X01 must preserve unsupported/non-vectorized fallbacks, and
C03 must retain a disabled-cache path. Removing a helper or accepting another
flag without moving its consumers does not resolve the structural mismatch.
These are not claims that Rust must copy Go's interface or memory layout.

## Executable observations

Retained source: [subsystem-structure-probe.rs](subsystem-structure-probe.rs).
Output: [subsystem-structure-probe.txt](subsystem-structure-probe.txt).
The runner creates a temporary example in tidb-session, executes it with the
locked dependency graph, and removes it even on failure. Exit zero means the
diagnostic completed; it does **not** mean these mismatches pass parity tests.

| Observation | Evidence established | Limit |
| --- | --- | --- |
| Strict ordinary TINYINT rejects 1000; generated TINYINT stores 127 and immediate SHOW WARNINGS is empty | K03 has a concrete public-session write-policy failure, consistent with the default-flags conversion path | No live TiKV or original Go runtime oracle was executed; Go's active-context `table.CastValue` source is the comparison |
| SHOW/SET SESSION_STATES are refused | S03's parsed statements lack the migration executor | This does not test every state handler or authentication token |
| SHOW BR JOB 1 is refused | E07 lacks the job-query entrypoint | No backup/restore side effect was requested |
| tidb_read_staleness=-1 is accepted, then SELECT is refused | S04's ordinary historical-query path is not integrated | No safe-point failure or historical row comparison was run |
| Cascades flag is accepted and reads back as 1 | Configuration can enable the flag | Q01's missing selection/memo owner is established by source, not this output |
| ALTER TABLE CACHE succeeds and SHOW CREATE reports CACHED ON | K02's cache metadata is accepted | Missing read/write lease ownership is established by source; mixed-node stale reads were not reproduced |

## Controls and candidates excluded from the count

The review did not promote a keyword, stale comment, alternate Rust type or
unused proto field to a behavior defect without comparing current Go ownership.

- Parallel projection, parallel sort/spill and hash-join V2 have production
  builders. E06 concerns projection completion, not absence of parallelism.
  Other lane-pool owners already have completion barriers.
- Statistics cache/loading and automatic analyze have live callers. Synchronous
  load workers include shared in-flight request ownership, queueing and retries;
  asynchronous loading is also wired. O07 remains specifically statistics GC.
- Native client-rust local latches and region TTL/jitter/cleanup have live owners.
  Old TODO wording does not reopen them. The five earlier concrete transaction
  diff comments retain their repair receipts; T02/T03 are separate residual work.
- The workload repository starts its worker; the resource manager also has
  startup wiring. Neither is counted as wholly missing.
- PD/TiKV/etcd TLS helpers exist. MySQL TLS policy (A03) and the specific MPP
  connection path (M04) are separate findings, not a claim that all cluster TLS
  is absent. MySQL zlib/zstd negotiation and LOAD DATA LOCAL have live wire paths.
- Temporary-table overlays, recursive CTE storage and TABLESAMPLE have live
  owners. The latest catalog also applies schema diffs. I04 does not negate
  these implementations.
- Classic Go also rejects some NextGen/standby/starter behavior. Build variants
  remain explicitly unreviewed; classic refusal alone does not justify adding
  those features to Rust classic.
- The manual coprocessor envelope is a pre-region/task projection. Missing
  fields from that envelope alone do not establish an RPC field-loss finding;
  complete task assembly and variant consumers need separate review.
- Statistics has handwritten protobuf-facing models/codecs. Generated TiPB
  RowSampleCollector includes singleton_sketch and sketch_sample_count, but
  current Go's baseCollector also does not preserve those fields in its
  ToProto/FromProto. Their absence from the Rust collector therefore does not
  establish a Go-master behavior gap. Nil/presence adapters require semantic
  comparison before deletion.
- TopSQL's handwritten output models are part of O11's uncomposed reporter
  boundary; no additional wire omission was established for them. BR P04 is
  different: range utility inputs/results replace complete generated File
  values with an intrinsically narrower public type. It remains a seed contract
  finding, not a production-loss claim.

## Validation and remaining work

Exact commands from repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    git ls-remote https://github.com/ngaut/client-rust.git refs/heads/master
    python3 rust/docs/parity/current-audit/run-expanded-probes.py --probe structure
    python3 rust/scripts/build-structural-coverage.py
    rustfmt --edition 2021 --check rust/docs/parity/current-audit/subsystem-structure-probe.rs
    git diff --check
    make lint

The diagnostic, coverage generation, formatting and root lint passed. The initial
lint attempt could not resolve the Go module proxy inside the sandbox; the
network-enabled retry passed. Static evidence checks cover
Python syntax, 72 unique register IDs, all 83 crate assignments, all 856 package
assignments, current source anchors, local receipt links and temporary-example
cleanup. Publication additionally requires the actual pre-commit hook's locked
server build and a separate fresh locked build immediately before push:

    TERM=xterm git -c core.hooksPath=hooks commit -m "docs: audit remaining subsystem ownership gaps"
    (cd rust && cargo build --locked -p tidb-server)
    git push origin HEAD:hparser-integration

Gate outcomes are recorded in the living ExecPlan and publication response.
No full Go package has gained acceptance. No production repair, benchmark
improvement, distributed failure behavior, mixed-node cache lease, remote KILL,
live PD mode switch, TLS rotation, GC/TTL recovery or all-build-variant parity
is claimed. Sysbench, TPC-C, TPC-H and YCSB remain unmeasured. Earlier baseline
test failures remain open; this audit does not change or suppress them.
