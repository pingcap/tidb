# `pkg/util/extsort` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly five artifacts, all read in full. The local Go package
is byte-identical to the pin.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 43 | `bcac1cf1e0c6fc1d833f68f715549639f7187aa9` | `tidb-util::extsort` and its existing native filesystem, JSON, logging, synchronization, and temporary-directory dependencies |
| `disk_sorter.go` | 1,415 | `9819e55cb8bba15f7180a0acf94db290902ffe7d` | complete disk-run writer/reader pool, merge iterator, recovery state, compaction selection/splitting/execution, and buffered writer |
| `disk_sorter_test.go` | 973 | `7f3b38a71ba6b71b30a0956f4c00d0fe28876566` | all 16 source test identities together with their exact tables, scales, and concurrency loops |
| `external_sorter.go` | 87 | `65ea6cebf3e6d50f1182c2ad8aced41798765a06` | complete sorter, writer, and iterator contracts |
| `external_sorter_test.go` | 149 | `62a98f53374272c8204f3aee6b4df6f796dba844` | shared 1,000-key and 10,000-key/10-writer source test bodies |

There is no package doc, fixture, testdata, benchmark, generated source,
platform variant, README, or ownership artifact. Bazel's short/flaky/four-
shard scheduling metadata has no Cargo runtime behavior to port.

## Rust ownership and parity result

`rust/crates/tidb-util/src/extsort/` owns the complete package. The public
sorter, writer, and iterator traits retain the source state machine: concurrent
writers are admitted only before sorting starts; flush is reusable; the disk
writer's actual `Close` remains reusable; sorting is idempotent; iterators are
refused before the atomic sorted state; iteration supports seek/first/next/
last and strictly advances over unique bytewise keys; close either preserves
or removes external storage as selected by the caller.

Writer runs use the source filenames (`%06d.sst`, `.tmp`, and `sorted`),
directory mode, monotonic file allocation, temp-file rename, empty-run bounds,
one-megabyte KV histogram buckets, exact JSON field names/null shape, and
recovery cleanup. An empty path receives an implementation-private ephemeral
filesystem root, corresponding to Go's in-memory VFS. Pebble's Go-only SST
objects are not a cross-process TiDB protocol; the native run encoding keeps
the same atomic lifecycle and uses a sparse four-KiB block index plus one
buffered file handle per iterator. It therefore retains external-memory
behavior, concurrent reads, bounded index amplification, seeks, and recovery
without introducing a second database engine or loading every key/value into
memory.

The reader pool shares run metadata with exact reference removal and double-
release panic behavior. The merge iterator opens only files that can contain
the current minimum, removes duplicate keys across runs, reuses seeks, and
closes every opened iterator. Sorting ports the overlap sweep, compaction-file
selection, non-overlapping/depth grouping, histogram-sized range construction,
concurrency limit, shared file references, cancellation checks every 1,000
records, partial-output recovery, input deletion, and stable sorted-file
replacement. Reopening reads run metadata concurrently like the source
errgroup. The writer retains target buffer thresholds, over-sized record
admission, source ordering and empty-key dedup edge behavior, and one run per
flush.

Exactly `TestDiskSorterCommon`, `TestDiskSorterCommonParallel`,
`TestDiskSorterReopen`, `TestKVStatsCollector`, `TestMakeFilename`,
`TestParseFilename`, `TestSSTWriter`, `TestSSTWriterEmpty`,
`TestSSTWriterError`, `TestSSTReaderPool`, `TestSSTReaderPoolParallel`,
`TestSSTIter`, `TestMergingIter`, `TestPickCompactionFiles`,
`TestSplitCompactionFiles`, and `TestBuildCompactions` remain as snake-case
Rust identities. There is no supplemental test, benchmark, alternate in-memory
sorter, duplicate owner, or source-absent policy.

## WIP validation

Profile: WIP. This completes one prerequisite package in an ongoing
repository-wide parity audit; it is not a Ready or repository-completion claim.

Inventory checks from the repository root:

```text
git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 pkg/util/extsort
git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/extsort
rg -n 'failpoint\.|testfailpoint\.|failpoint' pkg/util/extsort
```

The complete source baseline passed from the repository root:

```text
go test -run '^(TestDiskSorterCommon|TestDiskSorterCommonParallel|TestDiskSorterReopen|TestKVStatsCollector|TestMakeFilename|TestParseFilename|TestSSTWriter|TestSSTWriterEmpty|TestSSTWriterError|TestSSTReaderPool|TestSSTReaderPoolParallel|TestSSTIter|TestMergingIter|TestPickCompactionFiles|TestSplitCompactionFiles|TestBuildCompactions)$' -tags=intest,deadlock ./pkg/util/extsort -count=1
```

Passed from `rust/`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check --quiet --offline -p tidb-util
cargo test --quiet --offline -p tidb-util 'extsort::disk_sorter::tests' --lib -- --test-threads=1
cargo clippy --quiet --offline -p tidb-util --lib --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings
git diff --check
```

The package has no failpoint use or dependency. No Go, Bazel, module, or
generated artifact changed, so `make bazel_prepare` is not required. The full
515-test `tidb-util` sweep was attempted: 512 passed, two existing tests were
ignored, and the unrelated `cgmon` test failed because this host rejects
`sysctl -n hw.memsize` with `Operation not permitted`. The same `cgmon` test
fails in isolation and no cgmon file changed. Cross-platform execution,
process-kill fault injection, and the Ready-profile `make lint` were not run.
Cargo emitted only the existing `tidb-model` `unused_mut` and vendored
TiKV-client `private_bounds` warnings.

## Risk

- Correctness: all five artifacts, 16 tests, recovery states, merge paths, and
  compaction algorithms are mapped; both source and Rust suites pass.
- Compatibility: filenames, marker semantics, metadata, byte ordering, and
  reopen behavior are preserved. The private run encoding is native rather
  than exporting Pebble's Go object representation; no package API promises
  interchange of partially written sorter directories between implementations.
- Performance: writers honor the source buffer threshold, iterator metadata is
  sparse by four-KiB blocks, reads are buffered and per-iterator, overlapping
  runs are opened lazily, and compactions honor the source concurrency/size/
  depth controls.
