# Shared PD circuit-breaker ownership

## Source and package boundary


Fresh TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb` pins PD
`v0.0.0-20260805103528-afa43111d149` and client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`. Native baseline is
`e3e8de80f2791ed725b6f06c25dfe4ed339ce564`; TiDB baseline is
`c527656ca391d2a6224582379c86e7435b0d78e6`. Both branches and master were
refreshed before editing and before publication; no incoming changes were found.

The complete `pkg/circuitbreaker` package has one production file, one original
test file, 14 production functions and ten original test cases. It has no doc.go,
fixtures, generated inputs, platform variants or package-local build files.
The [inventory](../../../third_party/tikv-client-rs/doc/pd-circuitbreaker-package.json)
hashes all package artifacts, dependency/build inputs, original test support
including Linux/non-Linux helpers, and source callers. The
[native ExecPlan](../../../third_party/tikv-client-rs/doc/pd-circuitbreaker-owner.md)
records the design and independent source-oracle reproduction.

This accepts the shared leaf owner and removes the displaced cache implementation.
It does not accept the complete grpcutil, PD root/TSO/discovery, client-go locate
or mock PD packages. In particular, the existing wrapper gates a logical PD call;
Go's grpcutil interceptor gates each physical unary RPC using a context-carried
breaker. Complete interceptor/transport ownership is the next dependency boundary.
P03/P06/P07 and the 85-tracked / 77-unresolved / eight-repaired register remain open
and unchanged. No missing parent service is enabled by this package.

## Changes and compatibility


Removed the cache-local settings implementation, breaker state, transitions and
completion handler. All seven existing call sites now execute through the shared
owner. The existing public settings name is an alias, and the process-wide
region-meta instance retains Go's explicit threshold-zero, 30-second window,
minimum ten QPS, ten-second cooldown and one probe. Disabled calls bypass breaker
accounting as in the Go interceptor.

The shared owner implements strict request-time expiry, wrapping uint32 counters
and products, mutable settings without resetting admitted generations, exact
half-open success equality, bounded concurrent probes, and late results updating
only their admitting generation. Settings, admissions and result updates share
one owner lock; the current clock is sampled under that lock. User execution and
network waits hold no owner lock. Rust Arc/guards retain old generations safely,
and poison recovery preserves usability after an unwinding settings mutator.

Both durations use signed i64 nanoseconds, preserving negative and extreme Go
values. Expiration compares signed elapsed time instead of adding a potentially
unrepresentable duration to a platform Instant. This intentionally changes the
public alias's fields from unsigned Duration to signed nanoseconds. Its Default
is Go's zero Settings; the mutable named AlwaysClosedSettings is distinct and uses
a ten-second window. All in-repo callers migrate; external Rust callers using
Duration-valued fields or the old nonzero Default need to migrate.

Four counters bind through the existing shared metrics consumer and rebind when
PD labels initialize. Name sanitization and independent success/error/overload/
fast-fail classifications follow Go. Synchronous and asynchronous unwinding
preserve the panic payload and account overload without an error sample. A
polled async operation dropped without a result completes as a non-overloaded
canceled RPC; this is Rust's lifetime adaptation of Go's canceled-RPC return.
Unpolled futures consume no probe. No retry policy, worker or timeout is added.
Context helpers reuse TraceContext and retain nil/missing/wrong-type semantics
and shared breaker identity. Error::CircuitBreakerOpen retains its typed native
identity; full PD errs-package acceptance is not claimed.

Native changes are src/pd/circuitbreaker.rs and its tests, src/pd/mod.rs,
src/lib.rs, src/region_cache.rs, the existing error's documentation and package
receipts/oracle. TiDB changes are the maintained vendor sync, sync log, this
receipt and full-picture audit/plan links. Cargo dependencies, lockfiles and
protocol inputs remain unchanged. The sync script reapplied all four patches and
regenerated protocol files; their contents remained unchanged. All nine changed
native files match the published dependency byte-for-byte.

## Validation


Before replacement, the executed regression
`pd_breaker_minimum_qps_uses_source_u32_wrapping` fails: a two-second window and
minimum QPS 2^31 produce a zero uint32 threshold in Go, so one overload opens the
breaker, while Rust's saturation kept it closed. The original red log is
`/private/tmp/pd-circuitbreaker-red.log`. The migrated regression passes.

All 18 focused native cases pass, including all ten original cases. Extra tests
cover exact expiry, signed duration extremes, wrapping counts/rates, disabling,
zero probes, concurrent admission, old-window completion, error/panic identity,
poison recovery, async cancellation, metrics rebinding and context derivation.
Byte-identical original Go files and original module inputs pass with the race
detector and original goleak TestMain. Two additional committed source-oracle
cases independently confirm signed/wrapping, panic, cancellation and context
behavior. No failpoint instrumentation is required by this package or support.

The full native library passes 1,504 tests with two existing ignored tests. Strict
Clippy, all-target compilation, formatting and diff checks pass. Exact native-root
commands (the full suite also runs the migrated region-cache test):

    cargo test --locked --lib pd_breaker_minimum_qps_uses_source_u32_wrapping -- --test-threads=1
    cargo test --locked --lib pd::circuitbreaker -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Exact original-source command, run first without and then with the committed
oracle input copied to pkg/circuitbreaker/native_oracle_test.go:

    cd /private/tmp/pd-circuitbreaker-go-source
    go test -mod=readonly -race ./pkg/circuitbreaker -count=1

TiDB integration/publication gates, from repository root:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-pd-client --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate shared PD circuit breaker'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/import/module/Bazel change triggers bazel_prepare. Oracle .go.txt files are
formatted with gofmt -s for the repository hook. Five stale, ignored TiDB
incremental-cache directories were removed after checking that no TiDB build was
active; 3,667,392 KiB of disposable cache data were removed. Source and build
outputs were preserved.

## Publication and remaining checks


Native `44afb53ffadfb5a711fa9c459d95fd197a3adb1a` is committed and pushed to
ngaut/client-rust master and synchronized into TiDB. Integration validation passes
28 PD tests (one pre-existing live-PD test ignored), 23 transaction-driver tests,
affected all-target compilation, root lint, source inventories, byte-for-byte
synchronization and diff checks. Existing unrelated unused-import/mutability
warnings are not changed. The actual pre-commit hook gates the commit on its
locked server build; publication runs a fresh post-commit locked server build
immediately before pushing hparser-integration. Optional goword is unavailable in this checkout;
no spelling check from that tool is claimed.

Live PD topology, Linux execution, native ThreadSanitizer, complete parent
lifecycle and sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. No performance
improvement is claimed. The unrelated configured TopN stable-tie failure recorded
in the previous metrics receipt remains open; no configured SQL code or assertion
is changed by this dependency migration.
