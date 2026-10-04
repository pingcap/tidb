# Shared cache and parallel Apply continuation

This living ExecPlan follows PLANS.md. All work and builds run in Codex Cloud. No push is authorized. Editable TiDB hparser-integration starts at ed4fe8e91f580d9b533595be5c2ccac05ff114a6; comparison Go master is freshly fetched 93a01d31f6da205ae4bf376825293903a6899fdb. Native master remains 19a56ccda1e128218cd33c69709038219aced9bc.

## Purpose and scope

Repair multiple source-confirmed gaps together: C02 shared instance physical plan caching, and E04 parallel Apply execution. Reuse physical plan cloning, parameter compatibility/range rebuild, Joiner tuple semantics, statement memory and reusable worker pools. This is maintenance of existing Rust owners, not complete pkg/planner/core or pkg/executor transcreation acceptance. Native client sources are outside this batch.

## Progress

- [x] Confirm Cloud paths, clean branches/remotes and fresh upstream refs.
- [x] Recheck both findings against live sources; both remain true.
- [x] Capture failing production regressions before fixes.
- [x] Compose domain cache admission, independent clone-on-read/write, limits, periodic eviction, metrics and joined shutdown.
- [x] Compose Go parallel Apply eligibility, independently rebound workers, shared cache, ordering, error/cleanup and serial fallback.
- [x] Replace stale tests without losing source obligations; validate owners, session and real wire execution.
- [x] Run Ready checks, make lint and mandatory locked server build; self-review and maintain both finding registers and durable receipts.
- [x] Commit normally with actual hook, save recovery bundle and reusable Cloud draft; do not push.

## Source ownership and design

Go cache owner: pkg/planner/core/plan_cache_instance.go (all entry/admission/limits/eviction methods), plan_cache.go lookup/clone/rebuild; pkg/domain/domain.go InitInstancePlanCache, 15-second metrics/limits and 30-second eviction loops. Session LRU remains distinct; CLOSE only deletes session entries. Shared snapshots must never be bound in place.

Go Apply owner: pkg/planner/core/optimizer.go enableParallelApply (only outer recursion under Apply, clone eligibility, KeepOrder); pkg/executor/builder.go buildApply (independent inner clones, serial fallback); parallel_apply.go (bounded ordered pacing, shared inner cache, errors/panics and joined retirement). Rust must reuse NestedLoopApplyExec's Joiner rather than introduce duplicate SQL tuple policy. Concurrency is execution, not an EXPLAIN annotation.

## Validation

Run red regressions first and retain Cloud logs under /workspace/.cloud-setup/cache-apply-batch. Then targeted crate suites, checks for changed crate dependents, make lint and cargo build --locked -p tidb-server. Test cross-session hit/parameter isolation, admission/limits/flush, actual overlapping Apply evaluations, ordered/NULL/error results and early Close/reopen. Retain valid fixture expectations and record any unimplemented upstream obligation explicitly. No performance claim without measurements; no live multi-node TiKV claim without a cluster.

## Surprises and decisions

The existing plan visitor can rebind every occurrence of an owned correlated column to one fresh cell. Existing deep_clone alone preserves Arc binding cells and is insufficient for worker isolation. The shared cache must clone on admission as well as on hits because miss execution mutates the original tree.

## Outcomes

Implementation and Ready validation passed: 199 distinct targeted Rust cases, all-target checks, make lint, the locked server build and 20 checks across two live MySQL connections. Source 571fdb93c503dbae2e26a048a26bba3ff140dc36 committed through the actual enforced build hook. The final receipt commit and external recovery/draft handoff retain their own completion evidence. E04 retains CTE/shuffle/full-matrix residuals; no complete Go package claim. C02 repairs its recorded shared-owner absence; N03 remains partial.


## Additional decisions

The initial Apply red query decorrelated to HashJoin; it is excluded. The corrected NO_DECORRELATE query fails with the historical session context producer restored while new planner/executor code is present. The production switch regression is red/green, not a pristine whole-tree red receipt.

Fresh Go confirms ADMIN FLUSH INSTANCE expires session LRUs and does not erase the shared instance cache. Cluster keys must use persisted InfoSchema versions because independently rebuilt catalogs have distinct native image IDs. Temporary tables remain uncacheable. GLOBAL byte-size writes use source parsing/minimum/error text and decimal typed getters.

Parallel workers reuse the serial Joiner and nested-loop row lifecycle. Window arguments/frame calculation expressions need correlated extraction, not only visitor rebinding. The request cancellation carrier is an independently rotating child scope: early retirement cancels outstanding coprocessor children, joins lanes, then frees rows/cache/queues; the statement SQL killer remains usable. Recursive CTE optimization disables parallel Apply throughout nested lazy body optimization. Nonrecursive CTE/shuffle inner cloning remains broader serial fallback than Go, so E04 is partial.

Three empty ignored planner tests were replaced and their source obligations retained in a ledger. MATCH without a source fulltext index is refused, not manufactured as LIKE. Statement process publication and profiling reuse the canonical observation digest to remove duplicate normalization; the stale zero-normalization assertion now checks the actual published digest.
