# Follow Go's cache ownership across packages

This is a design review, not a completed transcreation or a runtime repair.
Integration `fd1e1fe378dc1d40f2d6eac5ab08689acb7e777e` and Go master
`93a01d31f6da205ae4bf376825293903a6899fdb` were freshly pulled/fetched on
2026-10-01. Go master selects `github.com/dgraph-io/ristretto v0.1.1`.
The native client pin is unchanged; this dependency belongs to TiDB's cache
owners, not the transaction client.

## Ownership before implementation

The repair unit is the complete shared dependency and its consumer contracts.
Replacing one container, or matching an eviction example, does not establish
the surrounding package's design. Trace construction, configuration, access,
publication, callbacks and shutdown through the actual production callers.
Use Go master objects, not just this branch's older Go working tree.

There are **four production importers** of Ristretto on the pinned master.
The fourth, inference, was missing from the preceding three-consumer view
because `pkg/inference` does not exist in the integration checkout. Its absence
in Rust is already recorded in the [inference boundary receipt](../../../testport/receipts/inference.md).
It is not a newly repaired package or an additional duplicate implementation.

| Go owner and lifetime | Current Rust owner | Required consumer contract |
| --- | --- | --- |
| `pkg/bindinfo/binding_cache.go`; shared binding updater maintained by `pkg/domain/domain.go::globalBindHandleWorkerLoop` | `tidb-session/src/binding_cache.rs`, `tidb-server/src/cluster_binding_seam.rs`; FIFO store and periodically replaced immutable cache images | Keep one live cache across incremental reloads; preserve the digest index, binding references, parsing and matching, capacity changes, callback logging, metrics, Set/Wait, watermark, GC/usage scheduling and shutdown. B01 and B02 must be reviewed together. |
| `pkg/statistics/handle/cache/internal/lfu`; statistics cache lifetime | `tidb-stats-handle-cache-internal-lfu`, parent `tidb-stats-handle-cache`; Stretto plus metadata fallback | Preserve primary-first Get, immediate fallback publication, retained table identities, CopyAs/DropEvicted, callback cost accounting, nil trigger payloads, load-phase waits and joined shutdown. C04 remains open. |
| `pkg/store/copr/coprocessor_cache.go`; `Store.NewStore`/`Store.Close` | `tidb-distsql/src/copr_cache.rs`, `tidb-exec/src/real_tikv_read.rs`; FIFO store with a required, hardcoded enabled instance | Use effective TiKV configuration, including disabled cache; preserve request/response admission, key bytes and collision checks, timestamp/region validation, paging ranges, shared payloads, eviction metrics and store-owned close. C03 includes the caller, not only the container. |
| `pkg/inference/sqlembed.go`; Domain init/close via `pkg/domain/inference.go` and `pkg/inference/domainadaptor` | No accepted Rust inference runtime; existing variables and ignored expression tests are seeds | Share results and in-flight calls per Domain. Preserve key construction and configuration version, option snapshots, cloned result vectors, independent caller cancellation, provider cancellation after the last waiter, batching, Set/Wait and joined shutdown. Implement the complete owner/dependencies before enabling SQL support. |

The shared part is the **implementation**, not a single global cache instance.
Binding, statistics, coprocessor and inference caches must retain independent
budgets, policies configured by their owners, callbacks and lifetimes. Their
different configuration is significant:

| Consumer | Counters / maximum cost | Internal item cost / metrics / callbacks |
| --- | --- | --- |
| Binding | 1,000,000 counters; binding memory quota; deferred `Binding.size()` cost function | Ignore internal cost; policy metrics enabled; OnReject and OnEvict log unless closed. |
| Statistics | `max(min(cost / 128, 1,000,000), 10)` counters; adjusted memory quota | Internal cost ignored and policy metrics enabled only in Go test mode; OnReject, OnEvict and OnExit update the fallback/accounting. |
| Coprocessor | `max(capacity / max_result_size * 2, 10) * 10` counters; configured byte capacity | Internal cost included; default policy metrics disabled; OnEvict increments the coprocessor eviction counter. |
| Inference | 100,000 counters; 10,000 entries, cost 1 per vector | Ignore internal cost; default policy metrics disabled; no application eviction callback. |

All four use read-buffer capacity 64. Keeping the wrappers' cost functions
while silently changing internal charges, metrics or callback points would
still change admission and observable accounting.

## Shared dependency to implement

Use a native Rust implementation of the complete pinned Ristretto root package
at `rust/crates/tidb-ristretto`, below the consumer crates and without SQL or
transaction dependencies. This path is a design target; no such crate is
implemented by this review. Preserve the six source responsibilities
(`cache.go`, `policy.go`, `ring.go`, `sketch.go`, `store.go`, `ttl.go`) and their
public behavior. Do not replace only the LFU policy while retaining an
incompatible write processor.

The root contract includes new-key publication only after admission, immediate
resident replacement, one bounded 32,768-item write queue, dropped-write
semantics, ordered deletion and Wait markers, sampled LFU eviction and TinyLFU
admission, lossy read-frequency buffering, TTL, primary/conflict hashing,
deferred cost evaluation, capacity updates, policy metrics, and exact callback
sequencing through replacement, rejection, eviction, Clear and Close.
Clear is externally quiesced in Go; safe Rust ownership must enforce that
lifetime without claiming an additional concurrent-clear contract.

The current [91-artifact module inventory](ristretto-v0.1.1-inventory.json)
retains every test/support/build/platform artifact, including the separate
`z`, simulation and contribution packages. The root package contains six
production files, six test files, 73 original tests and five benchmarks.
Map runtime helpers and platform variants to native implementations explicitly;
an unused Go allocator or assembly file is not permission to omit it from the
integration decision. Root-package acceptance does not automatically accept
the separate `z` or simulation packages. Native layout, ownership and hashing
must be justified against the source contract; matching Go's GC or machine
instructions is not the objective.

A Stretto version bump or a TiDB wrapper cannot establish this contract.
The [paused-worker and concurrent-pressure failures](lfu-review-followup.md)
already demonstrate two differences. Source review also found a striped write
buffer instead of Go's single bounded queue, callbacks on dropped new writes,
version-based handling of pending replacement, and different Clear callback
semantics. These belong to one shared dependency review. Adding wrapper-level
eviction or accounting compensation would spread its responsibilities among
the consumers again.

Use Rust `Arc`, locks, atomics and joined workers to preserve reference identity
and lifetime. Invoke callbacks outside store/policy locks where Go does so;
statistics callbacks can enqueue another eviction trigger. A new broad lock
around every operation could deadlock those callbacks and serialize reads.
The shared cache must not acquire consumer locks or depend on consumer types.

## Retirement and integration order

First implement and validate the complete Ristretto root package, including
dependency decisions and original test/benchmark mappings. Keep the two C04
reproductions failing and explicitly unaccepted until the replacement makes
them pass. Do not enable a partial replacement in production or silently mark
an ignored test accepted. Core acceptance is a prerequisite for consumer
acceptance, not a substitute for it.

Then migrate the complete five-artifact statistics LFU package and its parent
callers. Remove the Stretto dependency and its API adaptation, retain the Go
fallback, and rerun every original LFU case including the pressure case and
policy metric assertions. Preserve the recently repaired public-operation
lifetime guard until the new shared cache's ownership proves it redundant.

For bindings, migrate the complete `pkg/bindinfo` owner and necessary Domain
callers together. Remove `CostLruStore`, its insertion queue/local accounting,
and the narrowed `BindingStore` contract. Replace full-image cache rebuilding
with the source updater's live shared cache and watermark. Merely putting a
Ristretto instance inside each new image would discard access history on every
reload. Keep the digest bi-map: Go permits digests to outlive an evicted entry,
and matching skips misses. Restore original probabilistic test assertions;
the Rust test that requires the oldest insertion to be evicted currently
certifies the wrong policy. GC and usage persistence are consumer work, not
features of the shared cache.

For coprocessor work, migrate the complete `pkg/store/copr` owner and its store
construction/close paths. Remove the private HashMap, insertion-order queue
and budget enforcement after shared storage owns them. Carry nullable cache
configuration through `ProductionReadSessionFactory`; remove its hardcoded
enabled-cache requirement. Keep key construction, result admission and
response validation in the coprocessor owner. Include both enabled and
disabled configured-server paths in validation.

Inference is a fourth dependent package with an existing explicit boundary.
The shared dependency must support its cache contract now; that does not
authorize advertising a partial EMBED_TEXT runtime. Its complete package,
provider/batcher dependencies, Domain/expression/vector integration and tests
form a separate acceptance milestone. No external provider calls or credentials
are needed for this design review.

Go's other cache implementations remain distinct. Planner plan caches use
their own LRU/instance ownership, and required binding/statistics side indexes
are not duplicate Ristretto stores. Remove an implementation only after the
corresponding Go responsibility has a validated replacement and all production
callers use it. Apply that rule equally to the planner's shared candidate
lifecycle and the session/transaction/client boundary in the wider audit.

## Evidence, scope and validation

The source scan was run after `git pull --ff-only origin hparser-integration`
and `git fetch origin master`:

    git grep -n 'github.com/dgraph-io/ristretto' origin/master -- '*.go' go.mod

Exclude `_test.go` matches when counting production importers. Those test
matches are worker leak allowances, not additional cache owners. Read the
constructors and production integration paths listed above; matching an import
string alone does not establish their lifetimes.

All 98 artifacts under the four Go consumer directories still match the
blobs in `package-coverage.json`. Direct package inventories are 18 artifacts
for bindinfo, five for LFU, 20 for copr and three for inference; their subpackages
retain independent acceptance boundaries. There are no `doc.go` files in
these four owners. This membership recheck does not mean all source in those
packages has been semantically re-reviewed or accepted in this turn.

This change only records design and source evidence. Validation is link/blob
checking, `git diff --check`, root lint and the mandatory locked server builds
for changes under `rust/`. No new runtime regression result is claimed. The
prior 73 Ristretto and ten LFU Go race-test passes remain prior evidence, not
tests executed again here.

Executed from the repository root:

    python3 /private/tmp/tidb-shared-cache-review-check.py
    git diff --check
    make lint

All passed. The local review checker compares the four production importers,
98 consumer blobs, all 91 dependency artifact SHA-256 hashes, new review links
and 74 unique finding IDs. Its source is retained at the command path. The
first restricted lint attempt could not resolve proxy.golang.org; the authorized
rerun with dependency/cache access passed. Its output is retained in
`/private/tmp/tidb-shared-cache-review-lint.log`.

Publication must additionally execute the actual hook and a separate fresh
build before pushing; the publication response records their result:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'docs: align shared cache repair with all Go consumers'
    (cd rust && cargo build --locked -p tidb-server)
    git push origin HEAD:hparser-integration

Implementation acceptance must run the original package suites, translated
original tests and regression cases, including queue saturation, pending
replacement/delete ordering, callback reentrancy, Clear/Close, TTL and hash
collisions. Run tests through production constructors as well as cache methods.
Do not demand a fixed probabilistic eviction victim or zero LFU cost after
dropped writes where Go does not guarantee it. Preserve the source contract
rather than creating a stronger assertion and changing implementation to fit it.

Measure allocation, lock contention, throughput and latency after correctness
passes. Use the five source dependency benchmarks and repeatable SQL workload
baselines for sysbench, TPC-C, TPC-H and YCSB with the same cache settings,
dataset, concurrency and warm-up. This review establishes no benchmark delta,
cross-platform runtime result or complete repository parity. B01, B02, C03,
C04 and the inference boundary remain open.
