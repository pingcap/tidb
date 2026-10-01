# Range-tree protocol boundary repair

Baseline: integration `566163c58cb5ad4f72cd127ebfc421ca83b50273` and freshly
fetched Go master `93a01d31f6da205ae4bf376825293903a6899fdb`. The integration
pull was already current. Every artifact in `br/pkg/rtree` is identical between
these revisions. The [package inventory](rtree-protocol-package.json) records
all seven artifacts and their blobs: two production files, four original
test/support/benchmark/fuzz files, and BUILD.bazel. There is no package doc,
generated input/output, platform variant or external fixture in this owner.

## Removed boundaries

Go's range trees have a concrete `[]*backuppb.File` payload. Rust's generic
RangeFile adapter permitted either shared files or arbitrary narrowed/cloned
values; the original-case tests used a four-field TestFile. The prior P04
repair supplied shared generated files to restore, but this lower-level
boundary still had a second representation policy.

RangeFile, both forwarding implementations, generic payload parameters and
TestFile are removed. Range/RangeStats, their trees, progress/checksum collection
and MetaSink now carry `Arc<tidb_proto::backup::File>` directly. Tree/container
clones and metadata delivery preserve the complete generated file and its
identity. The original Go file fixtures now include their source key bounds.
The sole production sibling, restore_utils, consumes the concrete types.

The two missing-range APIs now return generated `kvrpcpb.KeyRange`, matching
Go's RPC boundary. Go also defines a separate local KeyRange with containment,
intersection and logging methods; that local type remains. No second schema,
conversion wrapper or field list was added. Existing shared native source gates
already cover the complete backup and kvrpcpb packages. Native client, schema,
generation, Cargo manifests and lockfile are unchanged.

Changed production files are `tidb-br/src/rtree/rtree.rs`, its module/crate
documentation and both restore consumers (`merge.rs`, `rewrite_rule.rs`). The
other changes record source coverage and correct historical boundary claims.

## Validation

Before the fix, `missing_ranges_are_generated_rpc_messages` failed with two
E0308 errors: both APIs returned `rtree::KeyRange` where the caller required
the generated `kvrpcpb::KeyRange`. The same test passes after replacement.

The complete crate passes **38 active tests**. All eight original Go test
functions remain mapped, including both classic and V2 10,000-range merge
cases, all overlap/intersection cases and callbacks/checksum behavior. The
new generated-file test covers tree clone, merged output, metadata delivery,
full payload identity and the checksum-disabled mode. On a second-send failure,
Go retains every range and publishes no checksum, but has already called back
for the first successful range. Retry therefore calls back again; this ordering
is explicitly verified, rather than silently making the operation atomic.

Both default-ignored workload tests were run: the previously empty
BenchmarkRangeTreeUpdate counterpart now executes 100,000 updates and checks
coverage, and restore's five merge workloads still pass through 100,000 files.
The original Go benchmark was also run with 100,000 iterations. These are
functional workload checks, not a comparative performance measurement.

Exact commands from the repository root unless otherwise indicated:

```sh
# Expected red, before replacement:
(cd rust && cargo test --locked -p tidb-br --lib missing_ranges_are_generated_rpc_messages)
# Green, including a rerun after final fixture cleanup:
(cd rust && cargo test --locked -p tidb-br --lib)
(cd rust && cargo test --locked -p tidb-br --lib benchmark_ -- --ignored)
(cd rust && cargo check --locked -p tidb-br --all-targets)
python3 rust/scripts/build-structural-coverage.py
make lint
rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-br/src/lib.rs \
  rust/crates/tidb-br/src/rtree.rs \
  rust/crates/tidb-br/src/rtree/rtree.rs \
  rust/crates/tidb-br/src/restore_utils/merge.rs \
  rust/crates/tidb-br/src/restore_utils/rewrite_rule.rs
git diff --check
# In /Users/qiliu/.codex/worktrees/rtree-go-reference/tidb at recorded master:
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./br/pkg/rtree -count=1
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -p 2 -tags=intest,deadlock ./br/pkg/rtree -run '^$' -bench '^BenchmarkRangeTreeUpdate$' -benchtime=100000x -count=1
/private/tmp/tidb-globalconfig-tools/bazel clean --expunge
```

Original Go tests pass with the race detector, the actual local-storage
MetaWriter fixture, original fuzz seed and TestMain/goleak harness. No failpoint
calls or dependency occur in this package. Reference-checkout Bazel preparation
passed; main-tree preparation is inapplicable because no Go/Bazel inputs changed.
The macOS linker emitted an LC_DYSYMTAB warning; the test exited successfully.
All-target Rust compilation and root lint pass, including complete shared
protocol source checks and their five existing guard tests. Temporary logs are
`/private/tmp/rtree-protocol-{red,green,workloads,check,lint}.log` and
`/private/tmp/rtree-go-{bazel,tests,workload,clean}.log`. Reference build outputs
are cleaned and the managed checkout archived after use.

Publication requires `TERM=xterm git -c core.hooksPath=hooks commit` to run
the locked server build, followed by a separate
`(cd rust && cargo build --locked -p tidb-server)` immediately before a normal
push to `hparser-integration`. Results belong to the publication response.

## Scope and remaining risks

P05's file/RPC boundary is repaired after review of the complete package, with
all callers migrated together. This narrows the public Rust helper API to Go's
actual payload contract and removes Eq bounds not provided by generated
messages. There is no external production caller of tidb-br in this workspace.

The helper package remains unconnected to live BRIE (E07). MetaSink still
represents an unimplemented Rust object-storage integration, and ChecksumStats
remains its explicit metautil boundary. Progress-record lookup uses native
mutable borrowing; the historical callback tests retain detached snapshots
instead of implementing all of Go's progress-handle aliasing. Local API-V2
decoding, process-wide summary collection and diagnostics require their own
dependency/lifecycle review. None is certified by this repair. The Go fuzz seed
ran, but no sustained fuzz campaign or native mutation harness was added.

No live cluster/object-store, cross-platform, sysbench, TPC-C, TPC-H or YCSB
performance validation was run. Shared files prevent payload copies at this
boundary; no throughput improvement is claimed. The register now contains
73 known findings, seven repaired and **66 unresolved**, not exhaustive proof
of repository parity.
