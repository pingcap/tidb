# Close the chunk allocator and pool contract

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

TiDB's `pkg/util/chunk/alloc.go` and `pool.go` bound reusable chunk and column storage, preserve physical column layouts, serialize concurrent reuse, and keep aliased column owners from being published twice. The Rust crate already contains most of that behavior, but the accepted production files and their original tests remain entirely unclassified in the package receipt. One Rust constructor also eagerly reserves the configured maximum number of chunk shells, unlike the accepted source, so a large valid configuration can allocate enormous storage before any chunk is used.

After this plan, creating an allocator is allocation-light regardless of the configured reuse ceiling; allocation, reset, alias, borrowed-column, hook, synchronization, global-capacity-bucket, and pool-return behavior are exercised through one public contract; and every obligation in the four accepted files has a final `PORTED`, evidence-backed `DECLINED`, or `UNREACHABLE` verdict. The package as a whole remains incomplete until all other source ledgers close.

## Progress

- [x] (2026-08-10 17:00Z) Read accepted `alloc.go`, `alloc_test.go`, `pool.go`, and `pool_test.go` completely at source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`.
- [x] (2026-08-10 17:10Z) Mapped the current Rust allocator/pool implementation, original tests, package receipt, and direct-consumer graph.
- [x] (2026-08-10 17:15Z) Identified eager `Vec::with_capacity(free_chunk_limit)` construction as an observable configuration/memory defect.
- [x] (2026-08-10 17:32Z) Added `allocator_configuration_does_not_eagerly_reserve_cache_limit`; baseline failed with capacity `4096`, then `Vec::new()` passed while both configuration clamp branches remained exact.
- [x] (2026-08-10 17:40Z) Added `allocation_pool_contract`, covering public configuration, allocator/default/empty/hook/sync wrappers, empty and nonempty paths, reachable widths, global capacity buckets, and aliased-owner return.
- [x] (2026-08-10 18:05Z) Added content-addressed runtime/proof evidence for intentionally excluded Go pointer, GC, benchmark, fixed-loop, and impossible-width mechanics.
- [x] (2026-08-10 18:15Z) Classified all 372 obligations in the allocation/pool cluster: 329 `PORTED`, 12 `DECLINED`, 31 `UNREACHABLE`, and zero `UNCLASSIFIED`.
- [x] (2026-08-10 18:32Z) Added five exact symbols/rules and five current mutation operators; every current mutation was independently run and verified `KILLED` with the production source restored byte-for-byte.
- [x] (2026-08-10 18:38Z) Removed the evidence-runner circular dependency on unrelated final verdicts while preserving atomic `check`/`write-receipt`; the focused Python regression and the real allocator/pool probe both pass.
- [x] (2026-08-10 18:50Z) WIP gates passed: all 58 checker tests, the five-test public contract, all 184 `tidb-chunk` unit tests, strict `tidb-chunk` Clippy, direct-dependent all-target checks, and workspace all-target check.
- [ ] Run remaining WIP gates, Ready gates, and push a race-safe checkpoint to both `hparser-integration` remotes.

## Surprises & Discoveries

- Observation: `ChunkAllocator::new` currently constructs both `pending_chunks` and `free_chunks` with `Vec::with_capacity(free_chunk_limit)`.
  Evidence: `rust/crates/tidb-chunk/src/alloc.rs` initializes both vectors from the global limit, while accepted `NewAllocator` creates nil `allocated` and `free` slices and only stores `freeChunk`.

- Observation: Rust has no production caller of the allocation or global chunk-pool APIs outside `tidb-chunk` tests.
  Evidence: a repository-wide Rust symbol scan finds the APIs only in `rust/crates/tidb-chunk/src/{alloc,pool}.rs`. Accepted Go uses the allocator in session/result-set and join paths and the global pool for histogram bounds and sort spill buffers. Direct Rust integration therefore remains a later cross-crate completion seam; this tranche closes the owning public package surface without falsely claiming those consumers.

- Observation: accepted `Pool.PutChunk` chooses a bucket from the caller's field type and can enqueue one aliased `*Column` twice under different logical slots. Rust deduplicates owners and buckets by the physical column width.
  Evidence: `pool::tests::put_chunk_buckets_aliased_owner_by_physical_width` is the regression for the ownership bug. This is an intentional Rust ownership correction, not Go runtime emulation.

- Observation: the accepted pool declares a fixed-16 bucket, but accepted `getFixedLen` returns only variable, 4, 8, or 40 for every valid `FieldType`; `Time` is eight bytes.
  Evidence: the first public-contract draft expected timestamps to use 16 and failed with the observed widths `[-1, -1, 4, 40, 8, 8, 8, 8]`. The corrected contract covers every reachable width and the fixed-16 switch route will be structurally `UNREACHABLE`.

- Observation: `run-evidence --kind probe` previously called the full package verdict validator and therefore refused to collect a finished probe while any unrelated obligation was still `UNCLASSIFIED`.
  Evidence: the real allocator/pool probe initially stopped at `chunk.go` obligation `O5b3fd4fe6f45c425`, although all 12 obligations declared by the probe were already final. `_probe_plan_for_execution` now validates the selected plan, exact accepted-source ledger identities, exact DECLINED references, and declared obligation set; full `check` and `write-receipt` remain unchanged and still reject every non-final package.

- Observation: the first pool mutation draft named both the local pool-width rule and the global capacity-bucket rule.
  Evidence: the immutable checker requires each current mutation to alter exactly one semantic rule and bind that rule's registered production definition plus exact boundary anchor. The final receipt has separate `M_CHUNK_POOL_ROUTING` and `M_CHUNK_GLOBAL_POOL_CAPACITY` operators and both are verified `KILLED`.

## Decision Log

- Decision: remove eager vector reservation instead of capping the configuration with another special case.
  Rationale: the source limit is an admission ceiling, not an instruction to allocate the ceiling at construction. Empty vectors preserve the same reuse bound and make every configured value follow one normal path.
  Date/Author: 2026-08-10 / Codex

- Decision: keep `AllocatedChunk`'s ownership lease and require logical users to end the lease before reset.
  Rationale: Go can retain a raw pointer while `Reset` recycles its fields; safe Rust cannot preserve two mutable owners. TiDB's observable contract is reuse after the caller is finished, which the lease expresses directly. Raw-pointer invalidation is evidence-backed `DECLINED`.
  Date/Author: 2026-08-10 / Codex

- Decision: do not reproduce `sync.Pool` garbage-collection eviction or benchmark iteration machinery.
  Rationale: those are Go runtime/testing mechanisms. Synchronized physical-width reuse, row values, chunk shape, and alias safety are the package behaviors to preserve. The absence of a current Rust production consumer is recorded rather than hidden.
  Date/Author: 2026-08-10 / Codex

- Decision: permit incremental execution of a selected measured probe, but do not weaken package completion validation.
  Rationale: executable evidence should be collected when its exact obligations and plan are complete; requiring thousands of unrelated verdicts first creates a circular workflow and slows falsification. Atomic completion still belongs exclusively to `check`/`write-receipt`, which continue to require every obligation and helper contract to be final.
  Date/Author: 2026-08-10 / Codex

## Outcomes & Retrospective

The allocator constructor is now allocation-light regardless of the configured cache ceiling. Fail-before observed `pending_chunks.capacity() == 4096`; pass-after observes zero for both pending/free vectors and separately proves `u32::MAX` clamps to `i32::MAX`. The four accepted files now contain 329 `PORTED`, 12 evidence-backed `DECLINED`, 31 structurally `UNREACHABLE`, and zero `UNCLASSIFIED` obligations. The measured runtime probe has matching run/verify observations. The five current mutations for configuration, lifecycle, wrappers, local width routing, and global capacity routing all passed before mutation, failed under mutation, restored exact source hashes `8eced314...` / `c8bb7669...`, and passed after restoration. The official whole-package checker now advances to unrelated `chunk.go` obligation `O5b3fd4fe6f45c425`; this is an explicit partial-package checkpoint, not a completed `pkg/util/chunk` claim. WIP validation is green across all 58 checker tests, the five public contract tests, all 184 `tidb-chunk` unit tests, strict Clippy, all direct dependents, and workspace all-target compilation. The source-size ratchet still reports the pre-existing `tidb-executor/src/kv_table.rs` 2,785-line file; final Ready validation remains after remote integration.

## Context and Orientation

The accepted authority is `pkg/util/chunk/alloc.go`, `alloc_test.go`, `pool.go`, and `pool_test.go` at commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. Rust production code lives in `rust/crates/tidb-chunk/src/alloc.rs` and `pool.rs`. `AllocatedChunk` is an owning lease that dereferences to `Chunk`; dropping it returns admitted owners to the allocator's pending generation. `Pool` is a synchronized set of physical-width buckets used by `Chunk::destroy` and `new_chunk_from_pool_with_capacity`.

The immutable package receipt lives under `rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown`. Its four ledgers currently contain 112, 173, 39, and 48 unclassified obligations respectively. A `PORTED` obligation needs a Rust symbol, a boundary test, a rule, and mutation evidence. `DECLINED` and `UNREACHABLE` obligations need accepted-source quotes plus immutable evidence artifacts.

## Plan of Work

First, add an internal allocator regression that configures a deliberately large but safe cache limit and asserts that a fresh allocator has zero capacity in its pending and free chunk vectors. Run that exact test against the current implementation and preserve the assertion failure. Replace the eager `Vec::with_capacity` calls with `Vec::new`, rerun the exact test, then run the existing allocator tests.

Second, add `rust/crates/tidb-chunk/tests/allocation_pool_contract.rs`. It will use only public APIs to prove capacity and required-row sizing, empty allocator behavior, reset/reuse after lease end, live-alias safety across reset, hook-once and synchronization behavior, every physical pool width reachable from a valid `FieldType`, global capacity separation, and aliased-owner deduplication. The accepted fixed-16 pool exists but no accepted `getFixedLen` case can select it (`Time` is eight bytes); that route will receive structural evidence rather than an invented input. The target will also emit canonical observations for runtime mechanisms that are intentionally not copied.

Third, create evidence files for the safe lease adaptation, `sync.Pool` GC/benchmark exclusions, fixed positive loops, and the impossible `getFixedLen` no-match cases. Add exact symbols and rules grouped by configuration, allocator lifecycle, column admission, wrapper allocators, pool widths/global routing, and original tests. Production and semantic test rows become `PORTED`; fixed positive zero-iteration paths become `UNREACHABLE`; Go raw-pointer/GC/benchmark mechanics become `DECLINED`.

Fourth, add focused mutation operators for constructor laziness, allocator bounds/type safety, hook serialization, and pool width/alias routing. Run every mutation through the public target or the narrow unit target, restore the exact source bytes after each attempt, verify KILLED results, and update the append-only mutation history.

Finally, run the official package checker. It should advance beyond `alloc.go`, `alloc_test.go`, `pool.go`, and `pool_test.go` and stop only at the next unrelated unclassified source file. Run WIP tests while iterating. Before shipping, run formatting, strict Clippy, full `tidb-chunk`, all direct-dependent compilation, workspace all-target compilation, and `make -j12 lint`; merge the latest remote tip without force and push the same SHA to both remotes.

## Concrete Steps

From repository root `/private/tmp/task326-chunk-reader`, use:

    cargo test --offline --locked -j12 -p tidb-chunk allocator_configuration_does_not_eagerly_reserve_cache_limit --lib -- --exact --nocapture

The test must fail before the constructor change and pass after it.

Run the public contract from `rust/`:

    cargo test --offline --locked -j12 -p tidb-chunk --test allocation_pool_contract -- --nocapture

Run the official receipt checker from repository root:

    python3 rust/scripts/go-package-lockdown.py --root . check --spec rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown/package.toml --accepted-source-commit 665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f

The expected intermediate result is failure at the next unrelated unclassified ledger, not in any allocation/pool file.

## Validation and Acceptance

Acceptance requires the fail-before/pass-after constructor proof; public contract coverage of all allocator wrappers and every reachable pool width; structural proof for the unreachable fixed-16 route; deterministic alias and concurrency tests; all allocation/pool ledgers at zero `UNCLASSIFIED`; mutation KILLED evidence whose source hashes match the restored sources; and clean scoped Cargo/Clippy/check gates. No package-completion claim is allowed while any other `pkg/util/chunk` ledger remains unclassified.

## Idempotence and Recovery

Tests and receipt checks are safe to rerun. Every mutation must save the production source bytes in a `mktemp -d` directory, mutate one operator at a time, and restore and byte-compare the source before the next operator. Do not reset or clean the shared main checkout. If a remote push races, fetch, merge, rerun gates for the incoming paths, and retry without force.

## Artifacts and Notes

Initial checker frontier:

    go-package-lockdown failed: obligation O18b750dbd3ca3f04 has no final verdict: 'UNCLASSIFIED'

Initial obligation counts:

    alloc.go       112 UNCLASSIFIED
    alloc_test.go  173 UNCLASSIFIED
    pool.go         39 UNCLASSIFIED
    pool_test.go    48 UNCLASSIFIED

## Interfaces and Dependencies

The public Rust interfaces remain:

    pub fn init_chunk_alloc_size(max_free_chunks: u32, max_free_columns: u32)
    pub trait Allocator: Send + Sync
    pub struct ChunkAllocator
    pub struct AllocatedChunk
    pub struct SyncAllocator
    pub struct ReuseHookAllocator
    pub struct EmptyAllocator
    pub struct Pool
    pub fn new_chunk_from_pool_with_capacity(fields: &[FieldType], init_capacity: usize) -> Chunk

No new dependency is required. The implementation uses `std::sync` and `tidb-datatype` through existing crate dependencies.
