# Restore-utils protocol ownership repair

Follow-up: the [range-tree package repair](rtree-protocol-repair.md) removes the
generic RangeFile boundary described below. Restore now consumes the concrete
shared-file Range/RangeStats types; this receipt preserves the earlier evidence.

Reviewed Go master `93a01d31f6da205ae4bf376825293903a6899fdb` and integration
`aa7b8d864d8568b0546dc580f3cd56357fac9bc0` after a fast-forward pull and master
fetch. All eight `br/pkg/restore/utils` artifacts are identical between these
revisions. The [package inventory](restore-utils-protocol-package.json) records
their Git blobs, all 16 original tests, five benchmarks, support code, build
mapping, dependency decisions and integration limits. The package has no
additional generated, platform, fixture or fuzz artifacts.

## Removal and source design

The handwritten seven-field `File` discarded SHA-256, version bounds, table
metadata, size and cipher IV at the public helper boundary. Merely completing
the shared BR schema had not repaired these consumers. `proto.rs` and both of
its duplicate types are now deleted. The test-local `DataFileInfo` projection
is also removed; original PITR cases use the complete generated type.

`tidb-proto` re-exports native `import_sstpb` alongside the existing `backup`
package. Restore uses their generated `File`, `DataFileInfo` and `RewriteRule`
directly. No generated source or native client source changed. The existing
source drift gate now records `import_sstpb` too: ten shared packages and all
139 artifacts of master's kvproto pin, including generation inputs/outputs.
There is no new list of projected fields to keep synchronized.

Go retains `*backuppb.File` in both grouping and output ranges. Rust now uses
`Arc<File>` through that entire path: range-container cloning retains the same
payload instead of repeatedly deep-copying protobuf metadata. Only outer range
bounds are rewritten. The existing generic `RangeFile` statistics interface
delegates to the generated object, including `crc64xor`.

Rule lookup now borrows the selected generated rule. Go's explicit `Clone`
still maps to a deep copy that resets the three outer timestamp fields;
`Equal` keeps Go's explicit five-field rule comparison. The existing raw/encoded
key, error, ID mapping, filtering and merge cases remain intact. The merge
fixture's row handles now use Go's plain `codec.EncodeInt` framing; index keys
retain datum framing. Its old empty benchmark placeholder now executes all
five source workloads, from 100 to 100,000 files.

## Evidence and exact commands

Before the production replacement, the complete-file regression failed to
compile: the API expected `[proto::File]` instead of generated shared files,
and its output could not satisfy pointer-identity checks. After replacement,
the same regression verifies all generated metadata, shared identity, and
rewritten outer bounds across a merge. A second regression verifies generated
PITR input, borrowed rule identity, independent explicit cloning, timestamps
and equality policy. Original PITR fixtures also exercise the generated owner.

Commands from the repository root unless a working directory is stated:

```sh
# Expected failure before the replacement:
(cd rust && cargo test --offline -p tidb-br --lib merged_ranges_preserve_complete_shared_backup_files)
# After the replacement:
(cd rust && cargo test --locked -p tidb-br --lib)
(cd rust && cargo test --locked -p tidb-br --lib restore_utils::merge::tests::benchmark_merge_ranges -- --ignored --exact)
(cd rust && cargo check --locked -p tidb-br -p tidb-proto --all-targets)
python3 rust/scripts/check-shared-client-proto.py --write --go-ref origin/master
python3 rust/scripts/build-structural-coverage.py
make lint
rustfmt --edition 2021 --config skip_children=true --check \
  rust/crates/tidb-br/src/lib.rs \
  rust/crates/tidb-br/src/rtree.rs \
  rust/crates/tidb-br/src/rtree/rtree.rs \
  rust/crates/tidb-br/src/restore_utils.rs \
  rust/crates/tidb-br/src/restore_utils/merge.rs \
  rust/crates/tidb-br/src/restore_utils/rewrite_rule.rs
git diff --check
```

The complete Rust crate suite passes **36 tests**, with two default-ignored
workload entries. Restore's entry was explicitly run and passes all five sizes
(33.24 seconds total); the unrelated rtree placeholder was not run. All targets
of `tidb-br` and `tidb-proto` compile. `make lint` passes, including complete
TiPB/etcd/native source guards, five source-guard unit tests and dashboard lint.
No main-tree Go or Bazel input changed, so main-tree Bazel preparation is not
required. The final fixture cleanup was followed by another complete Rust
crate test run and all-target check.

The original Go tests ran in a disposable checkout at the recorded master:

```sh
# Working directory: /Users/qiliu/.codex/worktrees/restore-utils-go-reference/tidb
PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare
GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -race -p 2 -tags=intest,deadlock ./br/pkg/restore/utils -count=1
# Remove disposable reference build outputs after validation:
/private/tmp/tidb-globalconfig-tools/bazel clean --expunge
```

Bazel preparation and all original Go tests passed, including the race case.
No failpoint calls/dependency occur in this package, so no failpoint mutation
was needed. The macOS linker emitted an LC_DYSYMTAB warning; the race test
process exited successfully. The reference worktree is archived after cleanup.
Logs from this run are `/private/tmp/restore-utils-{red,green,workloads,check}.log`,
`restore-utils-go-{bazel,tests}.log` and `restore-utils-lint.log`.

Publication must run the actual hook with
`TERM=xterm git -c core.hooksPath=hooks commit`, then a separate
`(cd rust && cargo build --locked -p tidb-server)` before a normal push to
`hparser-integration`. Final results are recorded in the publication response.

## Acceptance boundary and risks

P04's narrowed protocol consumer is repaired across the whole reviewed
`br/pkg/restore/utils` package. Its helper API now requires shared generated
files and returns borrowed rules, a deliberate source-compatible ownership
change at a crate with no external production callers. Existing Rust borrow
rules prohibit mutation while a lookup result is borrowed; unsafe Go-style
concurrent mutation is not introduced.

This is not acceptance of live backup/restore execution: BRIE dispatch remains
absent (E07). Logging and Debug-versus-Go error rendering are not certified by
this repair. `metautil`, rtree/spans, the complete external packages and parent
crate retain their own acceptance obligations. No live TiKV/object-store,
cross-platform, sysbench, TPC-C, TPC-H or YCSB benchmark was run. Shared ownership
eliminates the new large-payload copies structurally; no throughput or latency
improvement is claimed. The register remains open with **66 unresolved findings**.
