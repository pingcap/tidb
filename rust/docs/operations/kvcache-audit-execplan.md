# Certify `pkg/util/kvcache` as one Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB uses `pkg/util/kvcache` as its small byte-identity LRU primitive for JSON paths, apply results, prepared statements, server bookkeeping, and statement summaries. This certification proves that the Rust owner retains Go's lookup identity, recency, capacity, callback, memory-guard, removal, and package-global tracker behavior, and that the three live Rust consumers still exercise the shared owner rather than private cache copies.

## Progress

- [x] (2026-08-12) Fixed the complete four-file Go inventory and accepted source pin `6a85c6bbbd6cae7e0eea20a75ecd0853ac3545d6`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, build/platform variant, failpoint, generated input, fixture, benchmark, fuzz target, example, `go:generate`, or `go:embed`.
- [x] (2026-08-12) Read every Go production/test/harness/Bazel line and every Rust owner, direct test, compatibility re-export, tracker, and live consumer line relevant to cache behavior.
- [x] (2026-08-12) Ran all eight Go tests normally and under race.
- [x] (2026-08-12) Mapped every Go test branch to the existing eight Rust source-contract tests and audited the untested Go production branches.
- [x] (2026-08-12) Ran the owning Rust crate and focused JSON path, apply cache, session plan-cache, and global-tracker consumer tests.
- [x] (2026-08-12) Added the compact semantic receipt and completed the Ready profile: all owner/consumer gates, all-target check and Clippy, formatting, repository lint, source/inventory checks, and diff review passed.
- [ ] Synchronize the target, publish one certification commit, and verify all remote SHAs.

## Surprises & Discoveries

- Observation: the Rust port already covers more behavioral surface than the Go unit suite.
  Evidence: `equal_hash_updates_value_and_retains_the_first_key` covers Go's existing-entry early return and byte-hash collision identity; `explicit_removal_capacity_change_and_clear_do_not_call_on_evict` covers `Keys`, `SetCapacity`, `RemoveOldest`, and callback suppression; the memory-probe failure test covers Go's `InstanceMemUsed` error branch.

- Observation: Go's package-global memory tracker is an exported package object but `SimpleLRUCache` does not consume it in this source package.
  Evidence: `GlobalLRUMemUsageTracker` is initialized in `init`; no cache method calls it. Rust therefore keeps the byte-LRU owner dependency-free and exposes the stable, source-labeled tracker at the existing `tidb-util` compatibility boundary.

- Observation: Go identifies entries only by `string(key.Hash())` and retains the first key object when a colliding key updates the value.
  Evidence: `Put` replaces only `cacheEntry.value`. Rust stores the original key in its indexed node, maps a copied byte hash to that node, and has a direct collision regression.

- Observation: eviction callbacks belong only to automatic `Put` eviction.
  Evidence: Go calls `onEvict` in the capacity/quota loop, but not in `Delete`, `DeleteAll`, `SetCapacity`, or `RemoveOldest`. Rust has the same split and a callback-count regression spanning all four explicit removal paths.

## Decision Log

- Decision: Accept `6a85c6bbbd6cae7e0eea20a75ecd0853ac3545d6` as the complete Go package pin.
  Rationale: it is the latest package-history ancestor on the target; all four current blobs are byte-identical and it contains the current Bazel inventory.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep `tidb-kvcache` as the production owner and `tidb-util::kvcache` as a compatibility re-export plus global tracker.
  Rationale: the cache algorithm has no dependency on TiDB's memory tracker, while existing consumers already import the util boundary. This preserves a small native crate without duplicating the cache or changing consumer contracts.
  Date/Author: 2026-08-12 / Codex

- Decision: Make no production or direct-test code change during certification.
  Rationale: every Go assertion and every reachable source branch already has equivalent Rust evidence. Adding duplicate tests or refactoring a correct stable-index implementation would increase churn without increasing confidence.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

Authority, inventory, source reading, Go normal/race tests, direct Rust tests, focused consumer tests, the compact receipt, and the Ready profile are complete. No semantic defect has been found. Publication and remote verification remain.

## Context and Orientation

The Go package is exactly `BUILD.bazel`, `simple_lru.go`, `simple_lru_test.go`, and `main_test.go`. `SimpleLRUCache` maps a key's byte hash to a linked-list element. The front is most recently used and the back is oldest. `Get` and an existing-key `Put` promote; a new-key `Put` enforces capacity and, when quota is nonzero, repeatedly samples process memory and evicts from the back.

The Rust owner is `rust/crates/tidb-kvcache/src/lib.rs`. It uses stable indexed nodes plus explicit previous/next links, a hash-to-index map, and a free-slot list. The source contract is `rust/crates/tidb-kvcache/tests/kvcache_source.rs`. `rust/crates/tidb-util/src/kvcache.rs` re-exports the owner and supplies the package-global tracker.

Live consumers are `tidb-datatype`'s JSON path cache, `tidb-executor`'s Apply cache, and `tidb-session`'s non-prepared plan-cache key set. Each imports the same `SimpleLruCache`; none carries a private LRU implementation.

## Source-to-Rust Test Map

`TestPut`, `TestGet`, and `TestValues` map to `put_get_and_capacity_eviction_preserve_lru_order`. It asserts capacity eviction, callback order, newest-to-oldest keys/values, hits, misses, and promotion.

`TestZeroQuota` maps to the same constructor/put path: `SimpleLruCache::new` is the quota-zero specialization and enforces only capacity. `memory_guard_also_enforces_capacity_without_resampling` independently proves the quota-enabled capacity arm.

`TestOOMGuard` maps to `memory_guard_evicts_until_the_probe_falls_below_threshold`, which proves repeated oldest eviction and resampling at the strict `used > quota * (1-guard)` boundary.

`TestDelete` and `TestDeleteAll` map to `explicit_removal_capacity_change_and_clear_do_not_call_on_evict`, which also covers missing deletes and zero callback count.

`TestPutProfileName` maps to `profile_name_is_stable`. Constructor zero-capacity panic, equal-hash update behavior, `Keys`, `SetCapacity`, `RemoveOldest`, and memory-probe errors have additional direct tests.

## Plan of Work

Add `rust/crates/tidb-kvcache/tests/kvcache.semantic.toml` with the accepted Go pin, every owner/compatibility/consumer evidence file, and the seven focused commands already proven manually. Do not modify production code or duplicate the existing direct tests.

Run the semantic gate from its last tracked version, then the Ready profile: owning and compatibility crate tests, focused consumer tests, all-target checks and Clippy for affected evidence crates, formatting, repository lint, Go normal/race authority, source pin/inventory gates, and final diff review.

Fetch `hparser-integration` immediately before publication. If it advanced, rebase the single certification commit and repeat Ready. Push normally, fetch again, and require equality among local HEAD, `origin/hparser-integration`, and `git ls-remote`.

## Concrete Steps

From repository root, run the Go authority:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestPut|TestZeroQuota|TestOOMGuard|TestGet|TestDelete|TestDeleteAll|TestValues|TestPutProfileName)$' -tags=intest,deadlock -count=1 ./pkg/util/kvcache
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestPut|TestZeroQuota|TestOOMGuard|TestGet|TestDelete|TestDeleteAll|TestValues|TestPutProfileName)$' -tags=intest,deadlock -count=1 ./pkg/util/kvcache

From `rust`, use `CARGO_INCREMENTAL=0` and shared `CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target`:

    cargo test --offline --locked -j12 -p tidb-kvcache
    cargo test --offline --locked -j12 -p tidb-util kvcache
    cargo test --offline --locked -j12 -p tidb-datatype json_path::tests
    cargo test --offline --locked -j12 -p tidb-executor --test apply_cache_source
    cargo test --offline --locked -j12 -p tidb-executor --lib apply::tests
    cargo test --offline --locked -j12 -p tidb-session --lib tests_non_prepared_plan_cache::the_cache_is_bounded_by_its_size_variable -- --exact
    cargo check --offline --locked -j12 -p tidb-kvcache -p tidb-util -p tidb-datatype -p tidb-executor -p tidb-session --all-targets
    cargo fmt --all --check

Run all-target no-dependency Clippy with warnings denied for each evidence crate. From repository root, run the compact semantic gate, `make -o tools/bin/revive lint`, source pin/inventory checks, and staged diff review. Do not run `make bazel_lint_changed`.

## Validation and Acceptance

All eight Go tests must pass normally and under race. All eight direct Rust tests and the five consumer gates must pass. The all-target check, Clippy, formatting, semantic gate, repository lint, source pin, inventory, and diff checks must pass or have an exact clean-base failure recorded.

The final commit may add only certification metadata because no behavior gap was found. It must remain one `pkg/util/kvcache` unit, contain no optimizer or transaction implementation change, and be based on the latest target tip.

## Idempotence and Recovery

All commands are read-only except formatting, staging, commit, and push. The planned files are additive. Cargo uses the shared target with incremental compilation disabled; do not clean it wholesale. If the remote advances, rebase only this one commit and repeat Ready. Never force push.

## Artifacts and Notes

Failpoint decision: no accepted package artifact references failpoint or testfailpoint, and the Bazel target has no failpoint dependency. Ordinary Go tests are correct.

Bazel decision: the planned receipt and ExecPlan are non-Go metadata. They add no Go file, import, top-level Go test, Bazel file, Go module change, or Bazel target change, so `make bazel_prepare` is not required.

Initial evidence:

    Go normal: pass.
    Go race: pass with only the recurring macOS linker LC_DYSYMTAB warning.
    tidb-kvcache: 8 passed.
    JSON path: 5 passed.
    Apply cache source: 4 passed.
    Apply executor: 8 passed.
    non-prepared plan cache bound: 1 passed.
    global tracker: 1 passed.

Ready evidence:

    semantic package gate: 1 package, 7 unique commands.
    all-target check for tidb-kvcache, tidb-util, tidb-datatype, tidb-executor, and tidb-session: pass through the semantic gate.
    all-target no-dependency Clippy for the same five crates with -D warnings: pass.
    cargo fmt --all --check: pass.
    PATH=... GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint: pass.
    source pin, four-file inventory, staged whitespace, and Bazel prerequisite gates: pass.

## Interfaces and Dependencies

`SimpleLruCache<K, V>` exposes `new`, `with_memory_guard`, `set_on_evict`, `clear_on_evict`, `get`, `put`, `delete`, `delete_all`, `len`, `size`, `is_empty`, `values`, `keys`, `set_capacity`, and `remove_oldest`. `CacheKey::hash_bytes` defines byte identity. `PROFILE_NAME`, `InvalidCapacity`, and `MemoryProbeError` preserve source-visible constants and failures.

`tidb-util::kvcache` publicly re-exports the owner and exposes `global_lru_memory_tracker`. No new dependency or production interface is introduced by certification.

Plan revision note (2026-08-12): created after complete source/inventory reading, Go normal/race validation, direct Rust owner validation, and focused live-consumer validation found no production gap.

Plan revision note (2026-08-12): recorded the compact receipt and complete Ready certification evidence.
