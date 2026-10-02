# Review of the 77 unresolved structural findings

> Historical review at 313b3cfea3. For current behavior after repartition,
> IMPORT and cluster-fixture removal, see the [latest review](post-removal-structural-review.md).
> The reproduction outputs below are preserved at their original baseline.

All **77 known unresolved entries remain unresolved: 72 open and five partial**.
Eight repaired entries retain their status, for 85 tracked entries. The five
partial entries are A01, C04, E02, T02 and P06. This review changes no production
implementation or dependency. It corrects stale PD descriptions and adds fresh
data-preservation evidence to D01, rather than counting the same DDL owner twice.

The most urgent new observation is that accepted `ALTER TABLE … PARTITION BY`
makes preexisting rows invisible. Other fresh diagnostics still reproduce
password reuse despite an accepted history policy, silent generated-column
truncation in strict mode, partial multi-action ALTER, byte-changing Latin-1
ingress, ignored IMPORT options and missing live statement summaries.

## Baseline and review method


Both repositories were clean before review. Fetch confirmed TiDB integration
`313b3cfea3500e3026a862d2be11a7fbb3d65481`, native client-rust
`6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`, and Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Client-go remains
`v2.0.8-0.20260928031501-8edb23f6c7ee`, PD client remains
`v0.0.0-20260805103528-afa43111d149`, and the other normative module pins are
unchanged. The existing TiDB native snapshot already contains that published
client-rust revision; this review does not require another dependency update.

Read the full register and compare every original literal Rust source reference
with the previous reviewed baseline. Of 135 references, 13 changed; the new D01
references bring the retained total to 138. Reconcile intervening production
changes using published package receipts and focused caller/owner source reads,
so unchanged evidence files cannot conceal new caller integration.
[Per-ID source continuity](structural-recheck/source-continuity.json) records
every status, disposition, source blob/tree and fresh diagnostic association.
It includes all changed TiDB crate and native source paths. The
[detailed register](structural-findings.md) retains every Go replacement owner.

This is a complete reconciliation of the known register, not 77 new runtime
tests or proof that all possible mismatches are known. Unchanged source carries
earlier evidence forward. Original package tests, generated/build/platform
variants and distributed acceptance remain obligations of their whole packages.
The 969 inventoried Go directories and 83 Rust crates have not become accepted
packages through this review. Go master did not change, so no inventory refresh
or new upstream feature count is warranted.

## DDL: accepted repartition does not preserve data


The [new diagnostic](partition-structure-probe.rs) creates a RANGE-partitioned
table containing 1 and 11. `ALTER TABLE … PARTITION BY HASH(a) PARTITIONS 2`
returns success, and the next SELECT returns no rows. Inserting 2 and 12 then
returns only 2 and 12. The nonpartitioned-to-HASH case also returns success and
loses visibility of both old rows. See the complete
[output](structural-recheck/partition-structure.txt). This establishes lost
visibility, not physical deletion of the old keys or a live TiKV experiment.

`tidb-executor/src/ddl/alter_table.rs::repartition_partition_action` constructs
new physical partition IDs and calls `set_partition` without migrating rows.
It also substitutes empty index/handle metadata for the real table contracts.
Go master's `pkg/ddl/partition.go::onReorganizePartition` validates the real
table/index state, persists the dual-write phases, runs `doPartitionReorgWork`
in StateWriteReorganization, and only then publishes replacement partitions
and retires old ranges. The missing owner is a durable online reorganization,
not a formatter or a partition-name substitution.

The cluster route is separately source-reviewed, not runtime-reproduced here.
`cluster_ddl.rs::apply_repartition_change` feeds the ordinary ADD/DROP planning
arm. That arm compares only partition names for an already-satisfied result,
allocates IDs from the net count increase, modifies definitions without using
the returned new partition method/columns, and selects the DROP action for the
repartition case. It assumes an old partition exists. The added
`last_built_partition_metadata` thread-local side channel is also read by
ordinary ADD/DROP/TRUNCATE callers without proving a build occurred on their
thread. These are additional evidence that the direct publisher is the wrong
owner; a local row-copy patch would leave the durable, concurrent and rollback
contracts unresolved. Keep them in D01 and its existing D02/D08 dependencies.

D11 is independently still reproduced: `ADD COLUMN added INT, ADD COLUMN id
INT` fails on the second action but leaves `added` present. The FK-only staging
path does not provide general multi-action ALTER atomicity. Partition additions
did not repair D02–D10 or the F02 replica lifecycle.

## What the fresh diagnostics establish


| Evidence | Current observation and limits |
| --- | --- |
| [Accounts and IMPORT](structural-recheck/expanded-ownership.txt) | A04 password reuse succeeds and history policy remains NULL; A03 REQUIRE X509 is refused; E05 skip_rows=1 imports both rows. Repaired UPDATE privilege denial remains intact (A01 partial); ordinary column-only SELECT denial is not reopened as a defect. |
| [Table and session contracts](structural-recheck/subsystem-structure.txt) | K03 ordinary TINYINT rejects 1000, while a generated TINYINT stores 127 without warnings under the same strict mode. S03 state migration and E07 BR job commands are refused; S04 accepts the staleness variable then refuses the read. K02 CACHE and Q01 Cascades setting acceptance are confirmed, not their missing concurrency/optimizer behavior. |
| [DDL and virtual tables](structural-recheck/session-ownership.txt) | D11 retains the first column after failure; I01 still hides a created sequence, fabricates cluster config and rejects a bounded log query. Earlier alias/USING and FK runtime repairs still behave as recorded; E02/E03 remain open for their physical-plan/chunk ownership, not those repaired symptoms. The cache diagnostic does not reopen C01. |
| [Summary and upstream delta](structural-recheck/remaining-structure.txt) | O18 still returns zero summary rows after both global switches are ON and SQL executes. `tidb_opt_range_max_count` remains unknown; retain it as the existing functional upstream delta rather than another structural ID. |
| [Wire and configuration](structural-recheck/expanded-server.txt) | N01 raw Latin-1 E9 becomes C3A9. N03 still rejects valid token-limit/stats-lease/ssl-ca configuration and changes the auto-TLS default. N02 flag acceptance is reproduced, not a command-concurrency test. Go-removed run-auto-analyze configuration is correctly rejected. |
| [Partition data](structural-recheck/partition-structure.txt) | D01 now has the fresh data-preservation failure described above, on two admitted repartition shapes. |

Each diagnostic exits zero when it finishes recording observations, including
incorrect behavior. These successful runs are **not passing parity tests**.
The stored earlier outputs remain unchanged; the fresh outputs have their own
directory and baseline.

## PD and native-client corrections


P06 must no longer say TSO has no deadline or joined worker owner. Published
deadline, connectionctx, batch, retry, opt, metrics, circuitbreaker and errs
prerequisites and the retry value-copy correction are real progress. The current
public `PdRpcClient::close` nevertheless stops region/TiKV workers and drains
TiKV clients without closing the PD/TSO owner. P03 service-mode/independent-TSO
discovery and P07 the RPC-duration cluster write lock are still present gaps.
The complete parent packages remain unaccepted.

The grpcutil experiment remains isolated and rejected: eight candidate tests
pass and two fail Go's server-preface readiness contract. Its source correction
also matters: Go's default pick_first enters Idle after losing an established
Ready connection and reconnects on demand. Do not require unsolicited reconnect
in that state. Initial background dialing/retry is a separate contract. This
review reads that receipt; it does not rerun or accept the candidate.

T02 must exclude the private native region-metadata breaker that was already
replaced with the shared PD package. TiDB/native routing, health-event and RPC
ownership migration still remains. T04's read-side update-mutex admission still
differs from client-go; the repaired nonblocking writes and existing native
health workers remain valid. No native code is deleted by this review.

## Complete unresolved assignment and repair priorities


All 77 IDs appear exactly once in the following primary assignment. Cross-owner
dependencies do not authorize separate partial-package acceptance claims.

| Complete owner workstream | All unresolved IDs | Current review conclusion |
| --- | --- | --- |
| Native PD | P03, P06, P07 | Preserve published prerequisites; complete public lifetime, discovery, transport and concurrent request ownership. |
| Native KV and TiDB consumers | T02, T04 | Migrate every storage/DistSQL/MPP consumer before retiring competing routing and transport; retain fixed breaker and retry behavior. |
| Shared SQL/transaction/table | S01, S02, S03, S04, A01, E02, E03, K01, K03, T01 | Configured alternate planning, static catalogs, physical metadata handoff, generated casts and generic assertion policy remain. Runtime FK/privilege repairs are partial prerequisites. |
| Schema, identity and GC safety | O01, O02, O03, O09, I04, K02 | Numeric identity, upgrades, historical/lazy schemas, active timestamps and cache leases require coordinated owners. Min-start-TS reporting matters even when a Go peer is the GC worker. |
| Durable DDL, placement and affinity | D01, D02, D03, D04, D05, D06, D07, D08, D09, D10, D11, F01, F02, F03, O14 | Highest-priority data-preservation work. Durable submission, shared state/error gates and resumable reorg precede action activation or direct-publisher deletion. MV/MLog paths remain disabled seeds. |
| Account/wire/configuration/admin | A02, A03, A04, N01, N02, N03, N04, N05 | Independent correctness work should proceed as whole source owners become ready; account policy loss and byte preservation do not depend on the PD transport experiment. |
| Shared cache and consumers | B01, B02, C02, C03, C04 | Complete admission/access/publication semantics and consumer refresh/config/lifetime. Do not replace the distinct instance-plan-cache design with Ristretto. |
| Optimizer/typed execution/workers | Q01, X01, E04, E06 | Complete shared construction/candidate/execution ownership, parallel Apply and joined projection close; settings and helper classes do not establish integration. |
| MPP and TiFlash Compute | M01, M02, M03, M04, M05 | Full fragment/task planning, preserved disjoint ranges, streaming/cancellation and shared secure transport/topology remain missing. |
| Domain job/resource/bulk services | O04, O05, O06, O07, O10, O15, E05, E07 | Construct the source role-gated managers and producers with their shutdown/recovery, replacing private IMPORT and other displaced loops after migration. |
| Runtime information/observability | I01, I02, I03, O08, O11, O13, O16, O17, O18, O19 | Live providers, peer fanout, worker lifetimes and event producers remain; keep the existing stores, workers and legitimate static metadata. |
| Inference | X02 | Complete provider, batching, cache and cancellation ownership before exposing the missing source execution path. |

The review priorities are: (1) DDL/data preservation and coupled schema/GC/cache
safety, (2) account security and charset/generated-value correctness, then
(3) full native/shared SQL ownership and dependent services. This is a risk order,
not permission to bypass dependency closure. Independent account/charset/cache
packages need not wait for the entire PD transport effort. Do not add more
admitted partial DDL paths to make test strings pass.

Remove implementations only after replacing their complete owner and migrating
all callers: direct DDL publication, alternate configured SQL interpretation,
TiDB/native routing duplication, private cache policies, poll-owned classic
TiFlash placement, private IMPORT and MPP transport/materialization. Missing
Domain owners, useful helpers and completed prerequisites need composition,
not indiscriminate deletion. Shared ordinary/merge candidate acceptance remains
an obligation; recent unrelated TopN changes neither close it nor prove that
previously removed candidate duplication has returned.

For sysbench/TPC-C/YCSB, PD metadata serialization, routing/health ownership,
cache admission and streaming DML remain structural targets. TPC-H additionally
depends on full MPP/streaming, parallel Apply and typed vector execution. No
workload was benchmarked and no speedup is claimed.

## Reproduction, checks and limits


The six fresh runs used the existing diagnostic runner's `run` function with
separate absolute output paths. From the repository root, the following repeats
the review without overwriting older evidence (the server case binds localhost):

```python
import importlib.util
from pathlib import Path

path = Path("rust/docs/parity/current-audit/run-expanded-probes.py")
spec = importlib.util.spec_from_file_location("diagnostics", path)
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)
for name in ("expanded-ownership", "subsystem-structure", "session-ownership",
             "remaining-structure", "expanded-server", "partition-structure"):
    crate = "tidb-server" if name == "expanded-server" else "tidb-session"
    diagnostics.run(crate, name + "-probe.rs", "review_" + name.replace("-", "_"),
                    "/private/tmp/structural-review-" + name + ".txt")
```

The invoked commands, each from `rust/`, are:

    cargo run --locked -p tidb-session --example review_expanded_ownership
    cargo run --locked -p tidb-session --example review_subsystem_structure
    cargo run --locked -p tidb-session --example review_session_ownership
    cargo run --locked -p tidb-session --example review_remaining_structure
    cargo run --locked -p tidb-server --example review_expanded_server
    cargo run --locked -p tidb-session --example review_partition_structure

The runner creates each temporary example exclusively and removes it in a
finally block. Compiler logs are `/private/tmp/review_*-build.log`. Only the
retained sources/outputs are committed. Publication additionally requires:

    python3 /private/tmp/check-structural-review.py
    rustfmt --check --edition 2021 rust/docs/parity/current-audit/partition-structure-probe.rs
    git diff --check
    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m "rust: review all remaining structural findings"
    cd rust && cargo build --locked -p tidb-server

The one-off checker verifies all IDs/counts, Markdown/JSON row agreement, the
77-ID assignment, current/prior blobs, local links and temporary-example cleanup.
The actual hook must run the locked server build; the separate build must run
after the final commit before push. Gate results belong in the living
[review ExecPlan](../../remaining-structure-audit-execplan.md).

No Go source/import/module/Bazel changes were made; Bazel preparation and
failpoint enablement are inapplicable. No original Go or native-client test suite,
live mixed TiDB/TiKV/TiFlash cluster, TLS rotation, durable crash/owner handoff,
Linux/platform matrix or workload benchmark was rerun. Source-only risks such
as mixed-node account-policy loss, cache leases and minimum-start-TS protection
are not promoted to observed distributed failures. Production risks remain
open; documentation and diagnostics do not fix them.
