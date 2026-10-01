# Replace the private region breaker with PD's shared package

This living ExecPlan follows TiDB PLANS.md. The atomic acceptance unit is all of
pinned PD pkg/circuitbreaker. Region-cache and grpcutil integration are distinguished
from accepting their complete parent packages.

## Purpose and context


TiDB Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins PD
v0.0.0-20260805103528-afa43111d149 and client-go
v2.0.8-0.20260928031501-8edb23f6c7ee. Both branches were refreshed and clean:
native e3e8de80f2791ed725b6f06c25dfe4ed339ce564, TiDB
c527656ca391d2a6224582379c86e7435b0d78e6. The package has one production file,
one original test file, ten original cases and shared testutil support. Inventory
all artifacts, platform variants, build inputs and callers in pd-circuitbreaker-package.json.

The existing region_cache.rs embeds another settings type and private state
machine. Its saturation changes source threshold arithmetic, panics lose result
accounting, and metrics/context helpers are absent. Implement the whole shared
owner, then remove that duplicate. Keep the existing public settings name as a
type alias. Go's region-meta instance explicitly uses a 30-second window; the
shared package's AlwaysClosedSettings uses ten seconds and Settings{} is zero.
Preserve those different source construction choices without another settings owner.

## Progress


- [x] Refresh source pins and read complete original package and existing callers.
- [x] Reproduce the old threshold arithmetic error and retain a red log.
- [x] Implement every declaration, lifecycle and original test; migrate and remove the private owner.
- [x] Validate original Go race/goleak, boundary/concurrency/panic/context/metrics cases and native gates.
- [ ] Publish native, sync TiDB, validate adapters/lint, commit with actual hook and freshly build before push.

## Design and milestones


Add src/pd/circuitbreaker.rs with Settings, Overloading, StateType, opaque State,
CircuitBreaker, mutable AlwaysClosedSettings and context helpers. Settings::default
represents Go's zero-valued Settings. ALWAYS_CLOSED_SETTINGS is a synchronized
native static; reading it explicitly obtains Go's named disabled configuration.
Use signed i64 nanoseconds for both duration fields, preserving Go negative and
extreme values. Compare elapsed monotonic nanoseconds against the signed interval
instead of adding platform-limited durations to Instant. Duration fields in the
old public alias consequently require migration from Duration to signed nanoseconds. Context helpers reuse
TraceContext's typed values and preserve nil/missing/wrong-type and Arc identity.
Expose the package as pd_circuitbreaker, reusing existing Error::CircuitBreakerOpen.

Preserve strict expiration, request-time transitions, wrapping u32 counts/products,
current settings at evaluation, exact half-open success equality, bounded probe
admission and late results updating their admitted state only. Keep locks short;
no network wait or user callback runs under the state lock. The settings mutator
runs under its lock as in Go. Recover poisoned native guards after unwinding, as
Go's deferred unlock does not permanently poison the breaker.

Bind four counters through the existing PD metrics consumer lifecycle, sanitizing
only spaces and hyphens. A failed non-overload operation counts success and error;
an overload counts overload and optionally error. Open rejection counts fast-fail
only. Unwinding counts overload without error and preserves the original panic.
The async API is the native execution adaptation: an admitted future dropped
without a result is a cancelled RPC (non-overload plus error), so half-open
admission cannot leak. Never-polled futures are not admitted. No automatic timeout,
background task or new retry policy is added.

Migrate all existing region-meta calls to this owner and remove the old state,
settings implementation and transition code. Keep the upstream client-go explicit
region-meta settings and disabled bypass. The existing logical PD-call wrapper is
still the integration boundary until complete grpcutil/per-RPC ownership is
implemented; do not describe this as accepted transport/discovery ownership.
Retain source overload classification and move existing tests to the shared owner.

## Validation and acceptance


Reproduce the old minimum-QPS multiplication saturation with two-second windows
and MinQPSForOpen=2^31: Go's u32 product is zero, so one overload opens the next
window; the old Rust implementation refuses to open. The replacement must pass
that test as well as all ten original source cases and their helper semantics.
Cover exact boundaries, zero probe count, setting changes, old-window results,
concurrent admissions, cancellation, panic identity, metric rebinding and context.

Native-root commands:

    cargo test --locked --lib pd::circuitbreaker -- --test-threads=1
    cargo test --locked --lib region_cache::test:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Copy byte-identical upstream circuitbreaker, errs, metrics, resource-group metrics,
testutil and original go.mod/go.sum into a scratch module. Neither circuitbreaker
nor its testutil imports use failpoints, so no instrumentation is needed. Run:

    go test -mod=readonly -race ./pkg/circuitbreaker -count=1

After native master publication, TiDB-root commands:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-pd-client --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared PD circuit breaker'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/module/import/Bazel files change. Preserve incoming commits and inspect
changed scopes before publication. Never bypass hooks or hand-edit generated
protocol output. Repeated tests/generation are safe; stop on failed gates and fix
the cause before committing or pushing.

## Surprises & Discoveries


Go decides overload separately from the returned error. Panic accounting reports
no error because the deferred variable is still nil. Half-open zero count is not
validated, and pending probes do not expire. Follow these behaviors explicitly.

## Decision Log


2026-10-01: complete the shared package and remove the private cache state machine.
A threshold-only patch would retain missing metrics, context and panic ownership.
Native synchronization and cancellation adaptation preserve safe Rust lifetime
behavior without adding Go-absent workers or retry/timeout policy.

## Outcomes & Retrospective


All 18 focused native tests and unchanged original Go race/goleak tests pass.
The two additional Go boundary oracle cases pass, independently confirming
signed duration, wrapping, panic and cancellation expectations. The red threshold
regression failed at runtime before replacement. Full native library validation passes: 1,504 passed and two existing ignored.
Strict Clippy, all-target compilation, formatting and diff checks pass. Native
publication/TiDB integration pending. Full grpcutil, root/TSO/discovery and cache
packages remain open. No Linux, live PD or workload benchmark acceptance is implied.

To reproduce the extra boundary oracle, copy
`doc/pd-circuitbreaker-oracle/source_oracle_test.go.txt` to the scratch module
as `pkg/circuitbreaker/native_oracle_test.go`, then rerun the same Go command.
The original ten tests and TestMain remain byte-identical.

The owner mutex serializes request transitions, settings changes and result
accounting as in Go. A generation handle retains late completion identity;
its internal guard never replaces the owner lifecycle. Clock sampling occurs
after acquiring the owner lock. No transport wait holds either lock.
