# Share the native store-health owner

This living ExecPlan follows PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose / Big Picture


All Rust routing consumers should use the client-go store-health owner. Store health records latency trends and TiKV feedback; copying region topology must retain the same health object. Feedback and periodic decay should skip a concurrent update, as Go does, rather than block an RPC callback. Queue estimates must use elapsed monotonic time, independent of wall-clock observations used elsewhere.

## Progress


- [x] Refresh both repositories; trace health, load and topology-copy consumers against the pinned Go source.
- [x] Reproduce copied health state and blocking native feedback/tick; both regressions failed as expected.
- [x] Publish native 6f663b396552eec6d1bfad76b65f813e317884a4 and synchronize TiDB; four compatibility patches applied without generated-output changes.
- [x] Remove TiDB's slow-score and feedback algorithms, retain native health identity across topology copies, and use the native load estimate with Instant.
- [x] Native library: 1,406 passed/two ignored; strict Clippy and formatting passed. TiDB: 886 passed/14 ignored; root lint passed.
- [x] Commit through the locked-build hook; its cargo build --locked -p tidb-server passed. Publication uses a fresh post-commit locked build followed by push only on success.

## Context and Orientation


TiDB hparser-integration starts at a4ef2faed3; client-rust master starts at 5777c01c. Fresh TiDB Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go v2.0.8-0.20260928031501-8edb23f6c7ee. The Go internal/locate package has no doc.go. Its store_cache.go retains a *StoreHealthStatus per store and uses TryLock for feedback and decay. slow_score.go owns latency trend arithmetic.

Native src/locate.rs already implements these algorithms, but its health feedback/decay use blocking mutex acquisition and detail reads also acquire that mutex. Native src/region_cache.rs owns the decaying queue estimate. TiDB repeats both algorithms in region/slow_score.rs and region/store_health.rs. Its RegionStoreTopology clones StoreState during atomic metadata replacement, currently cloning health statistics as values.

## Milestones / Plan of Work


First extend existing native health tests with a held update lock; the feedback/tick thread must finish while the lock remains held. Drop the lock before joining even on timeout so a red test cannot hang. Extend TiDB's existing replica-health suite with a copied routing record; marking its health slow must be visible from both references.

Next expose the existing native health/detail/slow-score types through tikv, retaining the atomic client counters. Keep the TiKV score readable atomically independently of the feedback mutex, and use try_lock for mutations. Expose the existing queue-estimate type with default/update/estimated_wait methods so embedding callers do not repeat its arithmetic. Native cache callers must consume those same methods. Run native tests and Clippy, commit/push master, then synchronize using rust/scripts/sync-tikv-client-rs.sh.

Finally remove TiDB's slow_score.rs and health-feedback implementation. Alias native types; StoreRoutingHealth holds Arc<StoreHealthStatus>, so topology clones keep one object. Equality compares the health handle identity and load snapshot, not mutable health statistics, preserving metadata revision comparisons. Move health/load timestamps from wall-clock Duration to Instant. DistSQL's existing wall-clock observation callback still serves its other consumers; its ServerIsBusy observation must use Instant from the same clock as routing. Preserve existing behavioral tests using native methods and injected Instants.

## Surprises & Discoveries


TiDB's latency-recording and TiKV-feedback/decay methods have no production callers, only tests. Removing their duplicate algorithms does not by itself wire every production health observation or complete T02. Region cache replacement compares cloned store maps, so shared health identity must not cause revision changes when statistics update.

## Decision Log


Decision: use the native concurrent health object directly, not a new TiDB facade with its own score state. Retain only the routing record that associates load and the shared handle. Rationale: Go retains one health pointer per store and already owns all score calculations in client-go. Date/author: 2026-10-01, Codex.

Decision: keep the existing native atomics and skip contended updates. Rationale: replacing native counters with a coarse mutex would add contention and depart from Go's callback behavior. Monotonic Instant is the Rust representation of Go's elapsed time. This changes internal Rust APIs, not SQL or wire formats.

## Validation and Acceptance


From /Users/qiliu/projects/client-rust run the targeted new health regression, cargo test --locked --lib -- --test-threads=1, cargo clippy --locked --lib -- -D warnings, cargo fmt --all --check and git diff --check. Both regressions must fail before their fixes and pass after. Verify feedback rate limits, unchanged feedback timestamps, decay, queue exhaustion and shared identity.

From TiDB rust/ run cargo test --locked -p tidb-txnkv --lib --test all --test region_error_recovery_source and cargo test --locked -p tidb-distsql --lib --test all. Run make lint at TiDB root and rustfmt checks on changed sources. Rust-only changes need no bazel_prepare or Go failpoint mutation. Commit with TERM=xterm git -c core.hooksPath=hooks commit so the hook runs cargo build --locked -p tidb-server. Rerun that locked build after the final commit and immediately before git push origin HEAD:hparser-integration. Preserve logs under /private/tmp with store-health-owner prefixes.

## Idempotence and Recovery


Source synchronization is repeatable and applies the four maintained native compatibility patches; never hand-edit generated outputs. Keep unrelated work intact. A failed test/build blocks publication until explained or repaired. A held-lock regression always releases the guard before joining. Native publication precedes TiDB synchronization so the vendor revision is reproducible.

## Outcomes & Retrospective


The two red regressions now pass. TiDB's 138-line slow_score.rs is deleted, and store_health.rs retains only native aliases and the routing record. Native tests cover feedback contention and resumption; TiDB tests additionally prove health identity survives insertion of another region. All existing health trend, decay and busy-threshold cases now exercise native state. The 500/800/150 busy-sequence fixture explicitly prefers its intended first follower rather than depending on random ties.

The actual pre-commit hook passed the locked server build. The receipt amendment uses the same hook; final publication is guarded by another post-amend locked build followed by push only on success. This is maintenance of existing ports, not whole-package acceptance. T02's remaining cache, RPC, state-machine and health-event wiring remains open. In particular, sharing the type does not connect TiDB's missing production latency/feedback/tick callbacks. No complete original Go suite, live TiKV or benchmark result is claimed. Exact validation and file scope are recorded in parity/current-audit/store-health-owner-repair.md.

Revision note: recorded both failing regressions, the published native revision, removal of the duplicate algorithms, monotonic-clock migration and passing validation.
