# Share compute discovery and MPP invalidation


This living ExecPlan follows root PLANS.md. Baseline TiDB is d072d33b441578b7f7578a7888e9fb57f6aa1877 on hparser-integration; native client-rust master is 02880abbab5ed89a4dc603a4ddc7935853870ea6. Refreshed Go master is ab37692e9ebef44a736cd7243deef6020067b743; client-go remains 8edb23f6c7ee.

## Purpose and ownership boundary


MPP's non-autoscaler discovery must reuse client-go's independent compute-store cache. The cache belongs to the process region authority, while each request borrows it. PD lookup, empty/aliveness retries, dispatch, stream establishment and cancellation must agree on invalidation. Autoscaler and classic paths retain their separate source policy. Maintain T02/M04/M05 together; do not claim complete region-cache or coprocessor packages.

## Progress


- [x] Read current roots and pinned Go discovery/invalidation call sites; confirm native already has the cache policy but TiDB bypasses it.
- [x] Capture grouped socket regressions before production changes: two behavioral failures (three PD reads instead of one; warm cache lost on PD failure). Native extracted-owner test passes.
- [x] Extract native cache ownership without duplicating its policy; native test passed, de4c53c34f published after fresh locked server build, remote SHA verified, maintained sync and four patches/protobuf regeneration passed.
- [x] Compose process cache sharing and source-specific MPP failure invalidation across all affected callers. Combined MPP module: 46 passed, zero failed/ignored, including both former failures, dispatch/source matrix, cache isolation/empty reload and KILL. make lint passed.
- [x] Run grouped checks (46 MPP + one native test), make lint, scoped formatting/diff review and live validation (19 real MySQL/startup assertions). Update both registers and batch map; 54 broader roots remain unresolved.
- [ ] Finish actual precommit/fresh pre-push locked builds, remote verification and reusable Cloud checkpoint.

## Milestones and concrete steps


Extend the existing socket fixture in rust/crates/tidb-exec/src/tiflash_mpp_scan.rs with a generated PD service. Repeated scans and independently created sources over the same region authority must read PD once, retain cached topology during a later PD outage, and reload after source-defined failures. Run cargo test --locked -p tidb-exec --lib compute_cache_batch -- --test-threads=1 from rust/ after sourcing /workspace/.cloud-setup/env.sh and setting CARGO_BUILD_JOBS=1.

In /workspace/client-rust/src/region_cache.rs extract the existing private compute cache into a public reusable owner and have RegionCache delegate to it. Keep Go's read-through/invalidation behavior and filtering; no new timers, singleflight or caches. Validate existing native compute tests with the maintained compatible Cargo profile. Native publication requires a fresh locked TiDB server build, normal push and remote verification before rust/scripts/sync-tikv-client-rs.sh.

Attach one native cache owner to BackgroundRegionCacheShared. Migrate non-autoscaler MPP discovery from direct all_stores filtering. Carry its borrowed invalidation capability through dispatch, establishment, stream/cancel lifetimes; follow Go's differences between RPC errors, application packet errors, statement cancellation and autoscaler policy. Extend existing socket fixtures at the batch boundary, including negative invalidation assertions.

## Validation and recovery


Use source pkg/store/copr/{mpp,batch_coprocessor}.go and pinned client-go internal/locate/{region_cache,region_request,store_cache}.go. Retain cancellation and close tests, original native cache tests and source-backed regressions. Run the affected MPP module, native selected cache tests, make lint and scoped formatting. Actual hooks/pre-commit must pass cargo build --locked -p tidb-server; repeat immediately before each push and verify remote SHA. Never force-push, bypass hooks, overwrite concurrent edits or hand-copy vendor sources. Retire only completed owned executables when disk requires it; retain libraries and cache inputs. Evidence lives under /workspace/.cloud-setup/next-owner-batch and a committed validation JSON.

## Surprises & Discoveries


Native test linking first exhausted disk; the exact incomplete output and five obsolete native library artifacts were retired with hashes/inode checks. Final validation used a clean log.

The native cache is already implemented. TiDB uses its separate region adapter, so constructing another native RegionCache would duplicate the region lifecycle. Extract and reuse only the existing native compute-store owner under the current process authority.

## Decision Log


Keep classic placement, autoscaler topology and general region-cache consolidation explicit residuals. Do not import a second PD client, retry budget or connection fleet. Preserve Go's existing cache race semantics rather than inventing singleflight or generation rules.

## Outcomes & Retrospective


Native de4c53c34f is published and synced. TiDB composition passes 46 grouped MPP tests, including both baseline regressions, plus 19 real startup/SQL assertions; native owner validation, make lint and the locked server build pass. The duplicate PD metadata/filter path is removed. T02/M04/M05 remain partial only for broader recorded obligations; no parent finding or package is falsely closed. TiDB publication and Cloud checkpoint are pending. Whole-package, multi-node TiFlash and performance acceptance remain outside this maintenance batch.
