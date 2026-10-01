# Shared range-tree progress records

Baseline: integration `df7bd1aefb5b598af8a00ffdf07b6300eed92c58`, Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Integration was pulled and master
fetched before review. All seven source artifacts still match the complete
[package inventory](rtree-protocol-package.json): two production files, four
original test/support/fuzz/benchmark files and BUILD.bazel. No package doc,
generated input/output, build/platform variant or external fixture is present.
This follows the earlier protocol repair of the same `br/pkg/rtree` owner.

## Removed ownership mismatch

Go stores `*ProgressRange`, returns that same pointer from `FindContained`,
and keeps it alive after deletion when a caller retains it. Rust stored a
value, exposed an exclusive borrow, and cloned the progress/result tree to
keep the translated callback tests working after deletion. That clone silently
became a second coverage owner. The regression reproduced stale `[a,d)` coverage
through a retained copy after the actual tree had advanced to `[c,d)`.

Insertion, lookup and deferred deletion now carry the same
`Arc<Mutex<ProgressRange>>`. The mutex expresses Rust's exclusive mutation
requirement while allowing handles to outlive a tree operation. Guards must
be released before calling tree methods; the ordering key cannot change while
inserted, just as Go's B-tree comparator requires. Deep `Clone` implementations
on ProgressRange and RangeTree are removed. Value copies of Range/RangeStats
remain where Go explicitly copies those containers.

Both original callback tests now retain one handle from their first lookup
through all partial updates, completion and late responses. The detached
snapshot workaround is gone. A replacement inserted at the same key has its
own identity; a late response to the old handle cannot update the replacement,
reinsert a completed record or publish a second checksum.

The completion walk keeps handles rather than copied start-key/physical-ID
records, so it reads PhysicalID after callbacks as Go does. Metadata delivery
and callbacks run without a record lock. Delivery retains only file-reference
batches, preserving the generated payloads without copying them. The
writer/callback extraction, temporary no-op replacement and manual restoration
are also removed: Rust's disjoint field borrows permit their existing owners
to remain in place, including on an error return.

## Validation

The regression failed before the repair with actual `[a,d)` versus expected
`[c,d)` on the retained record. After the fix, all **40 active Rust tests** pass.
Two unchanged update/merge workload tests remain ignored in this run; both
were explicitly exercised by the preceding protocol commit, and this change
does not modify those algorithms. The all-target check compiles their bodies.

New coverage verifies shared insertion/lookup identity, duplicate insertion,
containment errors, updates through separate handles, unlocked sink/callback
access, post-callback physical-ID selection, completion, same-key replacement,
late responses and reference release. Existing failure coverage still checks
that a later metadata-send failure retains every range and publishes no
checksum, although earlier callbacks have run and run again on retry, matching
Go. The original eight Go tests, fuzz seed and TestMain/goleak harness pass
under the race detector; their metadata fixture uses actual temporary local
object storage. Both classic and V2 10,000-range merge cases still pass.

Commands from the TiDB repository root unless a directory is stated:

```sh
# Before the production repair; expected failure:
(cd rust && cargo test --locked -p tidb-br --lib retained_progress)
# After the repair:
(cd rust && cargo test --locked -p tidb-br --lib)
(cd rust && cargo check --locked -p tidb-br --all-targets)
python3 rust/scripts/build-structural-coverage.py
rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-br/src/rtree.rs \
  rust/crates/tidb-br/src/rtree/rtree.rs
make lint
git diff --check
# At /Users/qiliu/.codex/worktrees/rtree-go-reference/tidb, verified master:
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./br/pkg/rtree -count=1
/private/tmp/tidb-globalconfig-tools/bazel clean --expunge
```

Rust all-target compilation, formatting, diff checks, source-inventory checks
and `make lint` pass. Lint includes the complete shared protocol checks and
their five guard tests. Its first sandboxed attempt could not resolve the Go
module proxy; the rerun with network access completed successfully.

There are no failpoint calls or dependencies in this Go package, so no
failpoint instrumentation is required. The restored reference checkout passed
Bazel preparation. Main-tree Bazel preparation is inapplicable: only Rust and
its audit artifacts changed. The macOS Go linker emitted the existing
LC_DYSYMTAB warning, but tests exited successfully. Existing utility warnings
are unrelated to this repair. Temporary evidence is saved in
`/private/tmp/rtree-ownership-{red,green,check,lint}.log` and
`/private/tmp/rtree-ownership-go-{bazel,tests,clean}.log`.

Publication must use `TERM=xterm git -c core.hooksPath=hooks commit`, which
runs `cd rust && cargo build --locked -p tidb-server`, then a separate fresh
locked server build immediately before a normal push to hparser-integration.
The publication response records those results. The reference build outputs
are cleaned and its managed checkout is archived after validation.

## Scope and risks

Changed production files are `rust/crates/tidb-br/src/rtree/rtree.rs` and its
module exports/documentation in `rtree.rs`. Every progress caller is in this
package and migrated atomically; restore-utils does not call this progress API.
The remaining changes update this receipt, the package inventory, structural
register, generated coverage summary and living ExecPlan.

The public Rust helper API changes from an owned record/borrowed lookup to
shared handles. There is no production user of this progress API outside the
helper package in this workspace. Retaining a record now costs an Arc increment
instead of cloning its result tree. Locks and temporary file-reference batches
are new costs; no throughput claim is made. No sysbench, TPC-C, TPC-H, YCSB,
large BR progress benchmark, sustained fuzz campaign or cross-platform run was
performed.

This closes the retained-progress limitation recorded by P05. MetaSink's
object-storage integration, metautil ChecksumStats boundary, local API-V2
helper, summary/diagnostic integration and live BRIE (E07) remain explicit
unaccepted dependency/lifecycle work. Ordinary range lookup and checksum-map
access retain Rust borrow contracts; this receipt does not certify arbitrary
Go B-tree pointer operations or mutation of keys during traversal. It does not
claim complete BR application or repository parity. The register remains
73 tracked findings, seven repaired and **66 unresolved**.
