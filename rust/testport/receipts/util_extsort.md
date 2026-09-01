# `pkg/util/extsort` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from the earlier extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All five Go-master artifacts were read in full before making the boundary
decision:

| Artifact | Lines | SHA-256 | Inventory |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 43 | `87a1041d5950ea88918003d0d59785b459c8ca533ed7546c699dea4f4ee1a5b6` | public library and flaky short test targets; Pebble, generic, syncutil, logging, and errgroup deps |
| `disk_sorter.go` | 1,415 | `6f18350fd7348b1a2770aa929a11fe6b001f26d1bfccf23e0da60af4dffdb3bc` | Pebble SST writer/reader pool, metadata and KV histogram collection, merging iterator, compaction planner/runner, crash recovery, and `DiskSorter` lifecycle |
| `external_sorter.go` | 87 | `1e4bd7262a076564a33487eebc601cc4277a1771f1f3be479bad0f804273af6c` | `ExternalSorter`, `Writer`, and `Iterator` interfaces and their context/lifecycle contracts |
| `disk_sorter_test.go` | 973 | `786d1543ac5caf79ac5b9d0627561449ff4642ce072c2ac68c02573099846794` | 16 source tests covering common/parallel/reopen sorting, stats, filenames, SST writer/pool/iterator, merge, and compaction planning |
| `external_sorter_test.go` | 149 | `947548b70a6f3d1e33d4c6d37064efb04aea7b0b3106241051d486f44f21fef9` | shared common and parallel suites plus deterministic random KV fixture generator |

There is no `doc.go`, platform-specific source, generated output, fixture or
`testdata` tree, benchmark, fuzz target, example, nested package, or test
harness beyond the two source test files. The package has 2,667 Go lines and
the source has no current-master delta.

## Go behavior and consumers

`DiskSorter` writes sorted, deduplicated key/value pairs to Pebble SST files,
persists a `sorted` marker, recovers pending `.sst` files after a reopen, and
compacts overlapping files using collected KV-size histograms. Its iterators
support seek/first/next/last, merge duplicate keys, and reference-count SST
readers. Writers are reusable, copy caller bytes into a bounded buffer, and
flush atomically named temporary files. Go consumers are the Lightning
importer duplicate detector and duplicate-resolution tests under
`lightning/pkg/importer` and `pkg/lightning/duplicate`.

## Rust ownership and decision

The Rust workspace has no `pebble`, SSTable, or external key/value sorter
crate, no Lightning importer owner, and no dependency-closed `ExternalSorter`
trait. `tidb-executor::sort_partition`, `sort`, and
`parallel_sort_spill_helper` sort SQL chunks for query execution; they use a
different row/chunk spill protocol and are not substitutes for this utility's
Pebble-backed key/value sorter. `tidb-br` has restore-time range merging but
does not implement this API or its duplicate detector consumers.

Adding a small Rust sorter, adapter, or test-only Pebble substitute would be a
Rust-only behavior surface and would not satisfy the package-atomic Go
contract. No production Rust change or focused regression test is therefore
claimed for this package; the complete package remains explicitly unclaimed
until the Lightning importer and Pebble/SST ownership can move together.

## Validation

Profile: WIP for the continuing repository audit; this package has no Rust
code change.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/extsort -count=1` — passed (16 source tests).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/extsort` — empty; source is unchanged at Go master.
- Rust search of all crates and Cargo manifests — no Pebble/SSTable/ExternalSorter owner or Lightning importer consumer.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Rust
owner tests and `make lint` are not applicable to this no-change, explicitly
unclaimed boundary; full Lightning integration and Pebble crash-recovery tests
were not run.

## Risks and unverified scope

- Correctness: the Go package itself passes its complete source suite; no Rust
  implementation exists to compare against.
- Compatibility: a future port must preserve Pebble file format, crash marker,
  reader reference counts, compaction histogram boundaries, and duplicate-key
  semantics at the Lightning caller boundary.
- Performance: the Go package's concurrent compaction and reader cache are
  unrepresented in Rust; no performance claim is made.
- Not verified locally: Lightning importer/duplicate-detector integration,
  Pebble crash/reopen scenarios beyond the package unit suite, and any live
  external-sort workload.
