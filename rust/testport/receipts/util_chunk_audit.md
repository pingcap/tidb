# `pkg/util/chunk` — complete package audit and `UsedMemoryUsage` parity

Pinned Go source: `origin/master` at
`db35d47066648fe73abce6318d53fc625df51490`.

## Complete inventory

The package has exactly 30 artifacts and 11,342 Go lines. Every production
file, test, support harness, and build row was read from the pinned tree; the
earlier source-shaped transcreation receipts `b015`, `b016`, and `b017` cover
the complete Go test-function mapping and the Rust owner/test surface.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 81 | library/test targets and source rows |
| `alloc.go` | 335 | chunk allocator and reuse hooks |
| `alloc_test.go` | 361 | allocator/reuse/concurrency tests |
| `chunk.go` | 772 | chunk storage, row operations, memory accounting |
| `chunk_in_disk.go` | 395 | spilled chunk representation |
| `chunk_in_disk_test.go` | 106 | spilled-chunk tests |
| `chunk_test.go` | 1,212 | chunk behavior and memory tests |
| `chunk_util.go` | 410 | chunk utility functions |
| `chunk_util_test.go` | 261 | utility tests |
| `codec.go` | 341 | chunk encoding/decoding |
| `codec_test.go` | 191 | codec tests |
| `column.go` | 949 | fixed/variable column storage |
| `column_test.go` | 1,045 | column tests |
| `compare.go` | 265 | row/column comparison |
| `iterator.go` | 488 | chunk iterators |
| `iterator_test.go` | 198 | iterator tests |
| `list.go` | 190 | chunk list |
| `list_test.go` | 179 | list tests |
| `main_test.go` | 56 | testkit bootstrap |
| `mutrow.go` | 415 | mutable row |
| `mutrow_test.go` | 228 | mutable-row tests |
| `pool.go` | 125 | chunk pool |
| `pool_test.go` | 110 | pool tests |
| `row.go` | 264 | row view/accessors |
| `row_container.go` | 691 | memory-tracked row container and spill |
| `row_container_reader.go` | 170 | spilled-row reader |
| `row_container_test.go` | 603 | row-container/spill tests |
| `row_in_disk.go` | 420 | on-disk row representation |
| `row_in_disk_test.go` | 481 | on-disk row tests |

There is no `doc.go`, generated or platform-specific source, fixture tree,
benchmark, example, or additional harness in this package. Go master differs
from `origin/hparser-integration` only in `Chunk.UsedMemoryUsage` and its
three assertions in `TestChunkMemoryUsage`; `BUILD.bazel` is unchanged.

## Rust ownership and implementation

`rust/crates/tidb-chunk` owns the package (the crate is intentionally separate
from the `tidb-util` dependency root). The complete owner modules, source-test
ports, spill support, and consumer call-site inventory are recorded in
receipts `b015`, `b016`, and `b017`; no unreviewed Rust-only implementation was
left in the path touched here.

Go's new method reports currently occupied bytes, while `MemoryUsage` reports
retained allocation. Rust now mirrors that distinction:

- `Column::used_memory_usage` sums the Go column payload constant (112 bytes)
  plus null-bitmap, offset, data, and element-buffer *lengths*.
- `Chunk::used_memory_usage` sums those per-column values and is public for the
  source-shaped chunk API.
- `go_test_chunk_memory_usage` proves used bytes start below retained
  allocation, grow when rows are appended, and return to the initial value on
  `Reset` while retained allocation remains unchanged.

No Rust consumer currently needs this informational method, so no speculative
memory-accounting integration was added.

## Validation

Profile: Ready for this package batch; the repository-wide audit is still
continuing.

- `go test ./pkg/util/chunk -count=1` — the two existing spill failpoint tests
  (`TestPanicWhenSpillToDisk`, `TestPanicDuringSortedRowContainerSpill`) fail
  because the failpoint does not fire in this environment; the new memory
  behavior is not involved.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-chunk chunk::go_test_chunk_memory_usage --lib` — passed (1).
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-executor --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `make lint` — passed.
- `git diff --check` — passed.
- A full `tidb-chunk` lib/nextest sweep was attempted: the clean environment
  reports the pre-existing temporary spill-path and row-container timing
  failures (35/279 lib tests; 40/325 nextest tests). The focused regression
  passes; those unrelated failures remain explicitly unverified here.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: low; the new method follows Go's length-versus-capacity
  accounting and preserves the existing 112-byte per-column term.
- Compatibility: additive public Rust API only; no existing call path changes.
- Performance: one linear pass over columns and their current buffer lengths;
  no allocations and no change to retained-allocation accounting.
