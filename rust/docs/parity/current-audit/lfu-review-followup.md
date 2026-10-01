# LFU shutdown review and controlled admission evidence

Review baseline is integration `f94540083fd7c17b8589226feb105f18f2b31703`, pulled
before work, against fetched Go master `93a01d31f6da205ae4bf376825293903a6899fdb`.
All five artifacts in [the LFU inventory](lfu-lifecycle-package.json) still
match master. The complete Go owner, its shared cache interface and parent
integration are the review unit. This follows [the initial repair](lfu-lifecycle-repair.md).

## Review finding repaired

The earlier repair covered primary Get/Put/Del/Wait/Clear, but public
TriggerEvict did not acquire the lifetime guard. SetCapacity released it
between changing the quota and triggering eviction. Both then promoted
State's weak primary reference to a new Arc. Close could clear and drop its
own Arc, return, and leave the cache processor alive through that other Arc.
This contradicted the promised joined shutdown, even though the ordinary
read/write tests passed.

The new regression pauses a public operation immediately after its weak
reference upgrade, starts Close, and observes completion before releasing
the operation. It failed on the published baseline with
`Close returned with a live eviction handle (SetCapacity=false)`. The fixed
test checks both TriggerEvict and SetCapacity, waits for their completion,
and verifies that no primary owner remains after Close.

Both public paths now hold the existing shared lifetime guard for their
entire primary access, including the eviction trigger. No second close flag,
reference counter or polling loop was added. Callback triggers deliberately
do not acquire that guard: Close holds it exclusively while draining the
processor, so taking it from a callback would deadlock. A test-only one-shot
pause makes the previously missed interleaving deterministic; it is absent
from production builds.

## Dependency finding narrowed, still open

The large pressure test remains failing evidence, not a full explanation of
its interleaving. A second, controlled test now isolates primary admission:

1. Reject one oversized item and pause the cache processor in its callback.
2. Submit a different, admissible key while the processor remains paused.
3. Check its primary visibility before admission, then release and Wait.

Go returns `primary_before_admission=false`, then
`primary_after_wait=true`. Stretto exposes the new primary value before the
worker can admit it. The native regression fails on that precise assertion;
the immediately visible fallback table and final admitted identity also pass.
The test releases the worker before asserting so a failure cannot strand it.

The source mechanism is confirmed: Go's Ristretto v0.1.1 SetWithTTL only
updates an already-resident row synchronously; a new row lives in the bounded
Set queue until policy acceptance. Stretto 0.9.0 try_update instead calls
try_insert for new rows before queueing versioned policy work. This affects
frequency observations, callbacks and same-key replacement, beyond a single
oversized-table check. Adding another rejection policy in LFU would leave
that structural difference intact.

Both native C04 probes are explicitly ignored in the ordinary green suite
and remain runnable with `--ignored`. Neither has been made to pass by
asserting Stretto's behavior as correct. The previously noted inaccessible
native policy metrics remain open too. No complete LFU or dependency parity
claim is made.

The pressure translation previously inspected Values rather than Go's Get.
This review changes it to Get for every key, preserving primary-first lookup
and matching the strengthened Go probe in the earlier receipt. It still
fails with a retained 136-byte payload, so the admission finding is not based
on substituting fallback enumeration for Go's read path.

The [complete external inventory](ristretto-v0.1.1-inventory.json) pins the
module sum and every one of its 91 artifacts. The root package has six
production files and six original test/support files, 3,580 lines, 73 tests
and five benchmarks. All six production contracts were reviewed, including
buffer/callback ownership, TinyLFU admission, sampled eviction, read stripes,
sketch counters, 256 store shards, TTL buckets, metrics and shutdown. The
inventory also retains the separate z/sim/contrib packages, original stress
helpers, compressed trace, benchmark figures, CI/lint inputs, runtime assembly,
OS/32-bit/64-bit and jemalloc variants. These are explicit acceptance work;
native coverage of the 73 dependency tests is not yet implemented. No partial
replacement cache has been integrated.

## Validation

The targeted shutdown regression failed before the production change and
passes after it. The owner and parent suites have **33 passing tests and two
ignored dependency failures**. The original ten LFU tests and the pinned
73-test Ristretto package pass under race detection on macOS/arm64. The Go
source has no skipped root tests. Its five benchmarks were inventoried but
not executed. The controlled Go admission probe also passes.

Commands run from the repository root unless scoped:

```sh
# Red on the published baseline, then green after the guard repair:
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib public_eviction_operations_cannot_outlive_close)
# Red before marking the known dependency probe ignored:
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib nonresident_primary_waits_for_admission)
# Pressure reproduction after restoring Go's Get observation path, still red:
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib concurrent_small_capacity -- --ignored)
# Final native owner/consumer gates:
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu -p tidb-stats-handle-cache --lib)
(cd rust && cargo check --locked -p tidb-stats-handle-cache-internal-lfu -p tidb-stats-handle-cache --all-targets)
rustfmt --edition 2021 --check rust/crates/tidb-stats-handle-cache-internal-lfu/src/lib.rs rust/crates/tidb-stats-handle-cache-internal-lfu/src/source_tests.rs
make lint
git diff --check
# In the restored master reference checkout:
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./pkg/statistics/handle/cache/internal/lfu github.com/dgraph-io/ristretto -count=1
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go run /private/tmp/lfu-review-admission.go
/private/tmp/tidb-globalconfig-tools/bazel clean --expunge
```

The Go suite was rerun after Bazel preparation completed; an earlier run that
overlapped that prerequisite is not counted as the final gate. No main-tree
Go/Bazel files or module dependencies changed. Neither tested Go package uses
failpoints. The existing macOS linker LC_DYSYMTAB warning is unchanged.
Temporary evidence is in `/private/tmp/lfu-review-*.log`. Reference outputs
are cleaned and the managed worktree archived after validation.

All-target compilation, root lint, formatting, diff hygiene, every LFU/module
artifact hash and receipt links pass. Native ordinary tests do not include the
two explicit C04 failures; they are reported separately above.

Recreate `/private/tmp/lfu-review-admission.go` with this standalone probe,
and run it from the master reference checkout so it uses TiDB's pinned module:

```go
package main

import (
    "fmt"
    "github.com/dgraph-io/ristretto"
)

func main() {
    entered, release := make(chan struct{}), make(chan struct{})
    c, err := ristretto.NewCache(&ristretto.Config{
        NumCounters: 10, MaxCost: 100, BufferItems: 64, IgnoreInternalCost: true,
        OnReject: func(i *ristretto.Item) {
            if i.Key == 1 { close(entered); <-release }
        },
    })
    if err != nil { panic(err) }
    defer c.Close()
    c.Set(int64(1), "oversized", 136)
    <-entered
    accepted := c.Set(int64(2), "next", 8)
    _, before := c.Get(int64(2))
    close(release)
    c.Wait()
    value, after := c.Get(int64(2))
    fmt.Printf("accepted=%v primary_before_admission=%v primary_after_wait=%v value=%v\n", accepted, before, after, value)
    if !accepted || before || !after || value != "next" { panic("admission contract") }
}
```

The actual pre-commit hook must run the locked server build, followed by a
separate `cd rust && cargo build --locked -p tidb-server` before normal push
to hparser-integration. Publication results are recorded in the response.

Production changes are confined to the LFU lifetime paths in lib.rs; tests,
inventories, receipts/register links and the living ExecPlan record evidence.
Cargo dependencies and client-rust are unchanged. A shared-lock acquisition
now also covers explicit eviction and capacity updates; no performance delta
is claimed. SQL benchmarks, native race sanitizer, cross-platform/build-tag
matrix, external z/sim package suites and complete native Ristretto replacement
remain unverified. C04 stays unresolved; the register remains 74 findings,
seven repaired and 67 unresolved.
