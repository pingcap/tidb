# Statistics LFU lifetime and nil-trigger repair

Follow-up: the [shutdown review](lfu-review-followup.md) found and repaired two
public eviction paths missed by this change. It also adds controlled admission
evidence and the complete external dependency inventory. Counts below are
historical; C04 remains open.

Integration baseline `affe8b392b4957ab84009f4cb44c586ecf677354` was pulled before
review. Go master is `93a01d31f6da205ae4bf376825293903a6899fdb`. The complete
[package inventory](lfu-lifecycle-package.json) covers all five artifacts of
`pkg/statistics/handle/cache/internal/lfu`: three production sources, the
original ten-test file and BUILD.bazel, 782 lines. All blobs match current
master and the older audit. There are no generated inputs, platform/build
variants, doc.go, fixtures, benchmarks or fuzz corpus inside this package.

This is a complete-package review and a repair of the existing native owner.
It is **not package-completion evidence**: the Stretto/Ristretto dependency
boundary remains open as C04, including a newly reproduced failure below.

## Removed ownership differences

Go's fake eviction entries contain nil. Rust allocated an empty statistics
Table and used the key's sign to classify callbacks. That skipped a legitimate
table at ID -256: Go's signed remainder selects shard zero. Its full 136-byte
payload survived rejection. `Option<Arc<Table>>` now represents the nil value;
the callback's outer Option still independently represents a removed item.
The empty-table constructor, negative-key filter and sequential fake-key
counter are removed. Trigger keys use a random nonpositive signed integer as
Go does. Signed shard routing rejects -1 and allows -256; the retained -1
regression had already drifted back to failing on the baseline.

Go's sync.Once makes concurrent Close calls wait for completion. Rust marked
closed before acquiring its primary-cache mutex, allowing another closer to
return early. Every operation also cloned the primary Arc through that
exclusive mutex, so outstanding clones could keep its worker alive after
Close. A shared read guard now covers each primary operation; Close holds an
exclusive guard through clear, drain and the final primary drop/join. Copies
share that same owner. Callbacks use only the weak primary reference and never
take the lifetime guard, avoiding a worker/shutdown lock cycle.

Clear retains the reusable primary owner, clears it before fallback metadata,
and replaces fallback maps to release their storage. Close suppresses cost
callbacks before clearing, preserving Go's existing nonzero Cost after close.
Post-close Put still publishes fallback metadata and returns false; a later
Clear removes it, while repeated Close has no new effect, matching Go.

Each callback recovers independently and logs error/stack fields. The combined
Stretto callback still invokes onExit after recovered rejection or eviction,
as Ristretto's wrapper does. Host-memory adjustment errors retain their error
field, test mode enables native metrics, and clear/wait errors are no longer
silently discarded. Primary-first lookup, shared table identity, fallback
publication before Set, and copy/drop/accounting order remain intact.

## Dependency failure discovered by the full source review

All ten original Go tests pass under the race detector. Native cases map every
test and the checkTable support function; nine original cases execute in the
passing native suite. The small-capacity concurrent case retains all five
writers, five readers, 1,000 passes and 50 IDs. A deterministic warmup replaces
Go's one-second writer head start. It failed twice during this review:
after Wait, a retained Rust table still had a full 136-byte payload. The case
remains checked in with an explicit C04 ignore, runnable with `--ignored`.
It is failing evidence, not an accepted test or a reason to weaken eviction.

An initial extra assertion demanded Cost == 0 after the pressure workload.
That is not a Go contract: a temporary Go probe reported costs 49,959,294,
52,042,746 and 52,479,000, with all 50 retained table payloads evicted in all
three runs. The original test does not assert zero cost here. The native
reproduction therefore checks retained payloads without inventing that cost
requirement. The temporary Go probe was removed afterward.

Source comparison identifies a structural dependency difference: Stretto
0.9.0 eagerly inserts new keys before policy admission and processes versioned
replacement events; Ristretto v0.1.1 buffers nonresident values until admission.
This does not certify the precise interleaving behind every pressure failure.
The cached Stretto archive matches Cargo.lock's SHA-256, and every extracted
file matches that archive; this is not an unrecorded local dependency edit.
Do not work around this with oversized-item rejection or additional eviction
policy in the TiDB wrapper. The complete external package requires its own
inventory, implementation/compatibility decision and acceptance gates.

Stretto's synchronous API also does not expose the policy CostAdded/CostEvicted
metrics used by six source tests. Those exact assertions ran in Go, while the
native suite checks the corresponding visible costs/tables. No duplicate
metric counters were invented to make this gap appear closed. The original
high-concurrency case uses 32 native workers for all 2,000 operations rather
than allocating 2,000 OS threads; it keeps independent writers and readers.

## Validation and publication

Before the production repair, the targeted native suite failed for all three
confirmed defects: invalid signed shard selection, early Close and the real
negative-ID eviction. The sandbox also denied the zero-quota host-memory
query; that environmental failure disappeared with host access, preserving
Go's query-before-test-override ordering.

After repair, **19 LFU tests and 13 parent-cache tests pass**. One LFU pressure
test is the known failing C04 reproduction described above. Original Go tests
pass with race instrumentation, including the exact policy-metric assertions.
No failpoints occur in this package or its test dependencies. The restored
reference checkout ran Bazel preparation; main-tree changes contain no Go,
Go imports, Bazel targets or Go module changes, so main-tree Bazel preparation
is inapplicable.

Commands run from the repository root unless scoped:

```sh
# Before repair: expected three regression failures with host access.
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu --lib)
# Complete source cases initially exposed the dependency pressure failure.
# After native repair: 32 passing tests, one explicitly unaccepted reproduction.
(cd rust && cargo test --locked -p tidb-stats-handle-cache-internal-lfu -p tidb-stats-handle-cache --lib)
(cd rust && cargo check --locked -p tidb-stats-handle-cache-internal-lfu -p tidb-stats-handle-cache --all-targets)
python3 rust/scripts/build-structural-coverage.py
rustfmt --edition 2021 --check \
  rust/crates/tidb-stats-handle-cache-internal-lfu/src/lib.rs \
  rust/crates/tidb-stats-handle-cache-internal-lfu/src/source_tests.rs
make lint
git diff --check
# At the verified master reference checkout:
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./pkg/statistics/handle/cache/internal/lfu -count=1
# Temporary strengthened pressure probe, restored afterward:
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./pkg/statistics/handle/cache/internal/lfu -run TestLFUCachePutGetWithManyConcurrencyAndSmallConcurrency -count=3 -v
/private/tmp/tidb-globalconfig-tools/bazel clean --expunge
```

The lockfile was regenerated with `cargo check --offline`: only direct edges
to already-resolved rand 0.10.2 and tidb-log were added. Temporary logs use
`/private/tmp/lfu-owner-*.log`, including red, green, stretto-red, check, lint,
go-tests and go-pressure-probe. The Go linker emitted its existing macOS
LC_DYSYMTAB warning without affecting the successful exit status.

All-target compilation, formatting, source-inventory/link checks, 74 unique
finding IDs, `git diff --check` and root `make lint` pass. Lint includes the
shared protocol contract checks and their guard tests. The failing dependency
case remains the explicit exception to native source-suite acceptance.

Publication requires the real hook via `TERM=xterm git -c core.hooksPath=hooks
commit`, its `cd rust && cargo build --locked -p tidb-server`, and a separate
fresh locked server build immediately before normal push to
hparser-integration. The publication response records results. Clean the
reference Bazel output and archive the managed reference worktree afterward.

## Scope and risks

Changed production files are the LFU crate's manifest and lib.rs, plus its
generated lock edges. Source cases, this receipt/inventory, historical receipt
links, structural register/coverage and the living ExecPlan record acceptance
limits. The parent cache's implementation and client-rust pin are unchanged.

Rust now permits concurrent primary readers without an exclusive mutex or
per-operation primary Arc increment. It also eliminates allocated fake Tables;
the lifetime gate and random-number generation have costs. No sysbench, TPC-C,
TPC-H or YCSB throughput improvement is claimed or measured. No Rust race
sanitizer, cross-platform run or complete external-module acceptance was
performed. Existing statistics/table/logger dependencies retain their own
acceptance boundaries. The register is now 74 findings, seven repaired and
**67 unresolved**, including C04's remaining admission/metrics contracts.
