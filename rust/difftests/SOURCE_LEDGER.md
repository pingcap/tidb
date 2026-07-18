# Production source porting ledger

Tests define observable obligations, but a test inventory alone cannot prove
that every production implementation owner was inspected. The checked
`corpus/coverage/go_source_inventory.tsv` is the complementary source-first
queue: every non-test Go source file in this repository, its size, its target
Rust crate or explicit non-SQL boundary, generated-code status, and
conservative porting evidence.

Generate or verify it from `rust/` with 12 Cargo jobs:

```sh
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --write
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --check
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --summary
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --queue tidb-codec
```

The checked inventory is generated; do not edit it. Reviewed evidence belongs
in source-owned sparse fragments under `corpus/coverage/evidence/source/*.tsv`.
Each feature wave owns one fragment named for its stable owner; agents may add
or extend that leaf without serializing on a monolithic evidence file. Every
fragment row preserves the exact five-field schema and must cite an existing
source path and evidence artifact. The generator reads regular `.tsv` files in
sorted path order and rejects unknown directory entries and duplicate source
paths across fragments. `COVERED` means the complete production source
contract and its original tests have executable Rust evidence. `PARTIAL`,
`BLOCKED`, and `UNTRIAGED` remain open work.

Parser production evidence is imported from the separately checked generated
`corpus/coverage/parser_translation_manifest.tsv`, whose source-owned inputs
live under `corpus/coverage/evidence/parser/*.tsv`. `ported` maps to `COVERED`,
`partial` maps to `PARTIAL`, and `unassigned` remains `UNTRIAGED`. Do not copy
top-level parser rows into a generic source fragment: duplicate evidence is
rejected so one source owner cannot drift between the parser inputs and the
generic source evidence.

`target_crate` is a routing decision, not a parity claim. `unassigned` is a
visible architecture-triage queue for historical packages that have not yet
been given a defensible design-crate home. It must shrink through source and
dependency inspection; it must not be hidden by mapping all of `pkg/util` or
another mixed package to a junk-drawer crate. `deferred-external`,
`test-support`, and `tooling` remain inventoried so the SQL-node scope is
explicit rather than silently filtered away. `eliminated-go-runtime` is
narrower: it names exact Go GC/runtime adaptation code that the approved
design requires Rust to delete, not port. Those rows still need reviewed
evidence; routing them there does not make them covered.

`pkg/objstore` is split only where a real owner is already evidenced, rather
than recreated as a common Rust crate. The dependency-closed compressed-stream
leaf used by LOAD DATA routes to `tidb-exec`; access recording and globally
registered API metrics route to server observability. BR-only batch/lock/CLI/
no-op behavior remains `deferred-external`, while the memory store and generated
mocks remain `test-support`. The cloud/local clients, shared `storeapi` and
`objectio` contracts, backend construction, and the mixed `compress.go` file
remain visibly `unassigned`: DDL, executor, ingestor, planner, BR, Lightning,
and Dumpling are all real production consumers, so choosing one of them would
only hide a missing architecture seam. Routing is not coverage.

`pkg/util` is likewise routed at source-file granularity after inspecting its
production consumers. The `dbutil` database-client files used by BR,
Lightning, Dumpling, and standalone utilities are `deferred-external`; only
`dbutil/table.go`'s table-mode metadata invariant belongs to `tidb-catalog`.
The dependency-closed algorithm leaves have concrete owners: the statistics
bounded heap, expression RNG/int-disjoint-set/multi-value-map/float-set, chunk
generic disjoint set, planner ID/generic-set helpers, executor SQL-output and
memory-accounted aggregate sets, and server CPU-average/token-limit helpers.
This is intentionally not a precedent for mapping their neighboring files:
the adjacent generic sync map, integer/string sets, and general math helpers
still have incompatible consumers and remain `unassigned`.

Two exact `pkg/util` implementation mechanisms do not survive the language
boundary: the private Go `runtime.cheaprand` link and the
`runtime.ReadMemStats` cache. They route to `eliminated-go-runtime`; the actual
SQL RNG contract, RAII memory accounting, jemalloc/cgroup pressure, and query
kill/spill behavior remain real porting work. The broader memory
tracker/arbitrator is therefore still `unassigned`, as is `size.go`: its
unsafe Go-layout constants disappear, but its portable byte-unit contract
does not, so the mixed source file cannot honestly be called eliminated.
Shared caches, backoff/compression, string/time/redaction/sync helpers,
object-store prefetch, the mixed BR/storage worker pool, and the unused watcher
and error helpers also remain unassigned. Those rows expose missing
architecture or deletion decisions instead of manufacturing a `tidb-common`
junk drawer.

The normal parallel assignment is dependency-closed:

1. choose an exact source file or tightly coupled source domain from one
   target-crate queue;
2. include its original test-ledger entries and fixtures;
3. port its normal data flow and error surface into one leaf Rust owner;
4. add a real consumer across the next stable crate boundary when one is
   required;
5. land focused evidence by adding or extending only that wave's source and
   test fragments; the evidence steward owns the generated inventories and
   merged ledger checks, not every feature-owned evidence row.

Do not create empty design-shaped crates or opaque placeholder types merely to
move a source row. A new crate requires a complete source-backed API, an
immediate consumer, and executable original-test evidence in the same wave.
