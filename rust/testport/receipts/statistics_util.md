# Complete `pkg/statistics/util` package receipt

Status: package behavior complete against the pinned Go source. This is a WIP
package claim inside the larger ongoing Go-to-Rust parity effort, not a Ready
claim for the repository.

## Pinned inventory

Behavioral source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

| Artifact | Lines | Blob |
| --- | ---: | --- |
| `pkg/statistics/util/json_objects.go` | 82 | `497ebd60e85badb44dfe2f3527a894e3e717146a` |
| `pkg/statistics/util/BUILD.bazel` | 9 | `a2a342b24bff8c5ae4ffd43badacf47496d41e3b` |

That is the complete package tree. It has no `doc.go`, package-local tests,
test support, fixtures, testdata, benchmarks, generated sources, or platform
variants. Its only external production dependency is the pinned tipb module
whose generated `Histogram`, `CMSketch`, and `FMSketch` messages are embedded
in `JSONColumn`.

## Go-to-Rust mapping

| Go contract | Rust owner | Decision |
| --- | --- | --- |
| `TiDBGlobalStats == "global"` | `tidb_stats::TIDB_GLOBAL_STATS` | Direct constant |
| `JSONTable` fields and JSON tags | `tidb_stats::JsonTable` | Direct shared dump/load model; `Option` retains Go nil versus present values and `BTreeMap` matches `encoding/json` key ordering |
| `(*JSONTable).Sort` | `JsonTable::sort` | `sort_unstable_by_key`, matching `slices.SortFunc`'s unstable ordering and nil-element panic |
| `JSONColumn` fields and JSON tags | `tidb_stats::JsonColumn` plus the JSON forms of the three pinned tipb messages | Direct object/protobuf boundary |
| `(*JSONColumn).TotalMemoryUsage` | `JsonColumn::total_memory_usage` | Exact sum of generated protobuf `Size()` rules, including mandatory zero scalars and present empty byte slices |
| `JSONPredicateColumn` | `tidb_stats::JsonPredicateColumn` | Direct optional timestamps and integer ID |
| Bazel library target | `tidb-stats` module exported through `src/lib.rs`; downstream dump/load owners import it | Native build mapping |

The model is consumed by ordinary LOAD STATS, statistics dumping, history, and
cluster persistence paths. No executor-local duplicate JSON table remains.

## Parity corrections

The prior Rust representation had three source mismatches:

1. It used stable sorting even though Go uses unstable `slices.SortFunc`.
2. It applied `skip_serializing_if` to protobuf scalar fields whose generated
   Go JSON tags do not contain `omitempty`, and its manual `Size()` equivalent
   likewise omitted those fields when zero. The pinned generated methods
   always count `Histogram.ndv`, `Bucket.count/repeats`, `FMSketch.mask`,
   `CMSketchTopN.count`, and `CMSketch.default_value`.
3. It exposed `JsonPredicateColumn::new` and a four-test integration file that
   have no Go package counterparts and no production callers.

The production behavior is corrected and both source-absent surfaces are
removed. The repository bug-fix regression is placed in the existing Rust
mapping of Go `pkg/statistics/handle/storage/dump_test.go::TestJSONTableToBlocks`
rather than recreating a test suite in this source-test-free leaf.

## Validation

WIP commands, run from `rust/` unless noted otherwise:

    cargo fmt --all -- --check
    cargo test --offline -p tidb-executor --lib json_table_blocks_round_trip -- --nocapture
    cargo check --offline -p tidb-stats -p tidb-executor
    git diff --check

The Bazel preparation gate is not required: no Go/Bazel/module artifact is
changed. Ready validation and broad lint remain deferred because the global
parity goal is still active.

## Risk

The main compatibility risk is statistics dump JSON and cache memory
accounting. Both now follow the pinned Go tags and generated protobuf size
methods. No platform-specific behavior exists in this package.
