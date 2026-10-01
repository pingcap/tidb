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
