# Shared store health and load state

Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go
v2.0.8-0.20260928031501-8edb23f6c7ee. internal/locate/store_cache.go
keeps a StoreHealthStatus pointer on each store, reads scores atomically,
and skips contended feedback/decay writes. slow_score.go owns latency trends.

StoreHealthStatus, HealthStatusDetail, SlowScoreStat and StoreLoadStats are
available through the tikv facade so TiDB can remove its parallel algorithms.
Existing native cache paths use the same queue-estimate methods. StoreLoadStats
accepts Instant observations and returns a saturated remaining Duration.

TiKV score reads are now atomic and independent of the feedback mutex.
Feedback and decay use try_lock; an active update is allowed to finish rather
than making another callback wait. The active-feedback check also defers while
feedback state is being updated. Client-side counter/update arithmetic remains
in the existing atomic owner.

The held-lock regression health_feedback_and_tick_skip_a_concurrent_update
failed before the repair because the worker did not finish within one second.
Afterward it completes while the lock is still held, preserves score 80, and
resumes decay to 60 after the lock is released. The test always releases the
lock before joining, including on failure.

Validation from the native repository:

    cargo test --locked --lib health_feedback_and_tick_skip_a_concurrent_update
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The full library passes 1,406 tests with two ignored. Strict Clippy passes.
Logs are /private/tmp/native-store-health-owner-{red,green,all,clippy}.log.
TiDB must synchronize source using its maintained script. This is maintenance
of existing ports, not complete internal/locate acceptance. No live cluster or
sysbench/TPC-C/TPC-H/YCSB performance measurement is claimed.

## Atomic feedback publication follow-up, 2026-10-02

The previous receipt's statement that active-feedback admission defers under
the feedback mutex was incorrect. Go reads feedback presence, score and last
update time atomically before requesting feedback. Only writes and decay skip
a contended update lock. TiDB tracked this remaining gap as T04.

Keep the same shared native health object, but replace the mutex-protected
metadata with AtomicBool and ArcSwapOption<Instant>, retaining one Mutex<()>
for writes. arc-swap 1.9.2 provides safe timestamp publication/reclamation; no
custom unsafe pointer or wall-clock conversion is introduced. Follow Go's
pre-lock and in-lock timestamp checks, unchanged-score refresh and decay
recheck. The native health owner now surrounds the asynchronous callback with
client-score update and subsequent decay/slow-flag publication, matching Go's
tick order. TiDB must still synchronize this native source through its script.

Three regressions fail on the prior production implementation and pass with
the repair: overdue admission observes false instead of true, the cache sends
zero instead of one RPC under contention, and the callback sees client score
zero instead of one. Successful feedback prevents decay; callback errors and
unreachable stores allow decay; contended writes skip and resume after unlock.
All bounded contention tests release their held lock before joining workers.

Commands run from the native repository:

    cargo test --locked --lib health_feedback_and_tick_skip_a_concurrent_update
    cargo test --locked --lib health_tick_requests_feedback_during_concurrent_update
    cargo test --locked --lib source_go_region_request3_TestTiKVRecoveredFromDown
    cargo test --locked --lib locate::tests
    cargo test --locked --lib region_cache::test
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --all-targets -- -D warnings
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The owner suite passes 53 cases, cache suite 89, and full library 1,524 with two
existing ignored cases. Strict library Clippy passes. The broader all-target
Clippy check reports two pre-existing redundant closures in
src/pd/circuitbreaker_tests.rs:369,371; no lint suppression or unrelated edit
was added. Native Cargo.lock is ignored; Cargo resolved the new dependency
and the validation commands use that local lock. TiDB retains a tracked lock.

The same pinned original Go TestRegionCache subtests TestTiKVSideSlowScore,
TestStoreHealthStatus and TestRegionCacheHandleHealthStatus pass with Go1.25.14
and -race in a disposable module copy, enabling failpoints and disabling them
on exit. An added disposable held-writer oracle also passes under -race.
Logs are /private/tmp/native-health-publication-*.log and
/private/tmp/go-health-publication*.log. TiDB's
rust/docs/health-feedback-publication-execplan.md retains exact source-oracle
commands and downstream validation. This is existing-owner maintenance, not
complete internal/locate acceptance. Full routing/runtime ownership, distributed
failover, other platforms, ThreadSanitizer and throughput benchmarks are not
verified here; timestamp publication allocates as Go's time pointer does.
