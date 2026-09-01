# `pkg/util/hint` — Go-master parity boundary receipt

Status: audited, but unclaimed as a package-complete transcreation. The Go
hint package is a cross-cutting optimizer/binding owner; Rust currently splits
its syntax, binding, SEM filtering, and planner consumers across several
crates. The missing query-block handler and full statement/plan-hint pipeline
cannot be repaired safely as a partial leaf.

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The only source
delta after the extraction point is `8c38aa4e6a`, which changes the
`bindableChecker` visitor from replacing `Accept` to non-replacing `ast.Walk`
signatures without changing its table-count/subquery predicate.

## Complete Go inventory

The package has exactly four tracked artifacts and 2,128 Go lines:

| Artifact | Lines | Inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 25 | one public library target over all three production files |
| `hint.go` | 1,275 | statement/plan hint constants, parsing, matching, restoration, and warning aggregation |
| `hint_processor.go` | 466 | hint collection/binding traversal, SQL parsing, query-block classification, and history completeness checks |
| `hint_query_block.go` | 362 | query-block/view hint state, offset resolution, warnings, and generated QB names |

There are no Go tests, test harness, fixture/testdata tree, generated or
platform-specific variants, benchmarks, fuzz targets, examples, or additional
build artifacts. The three production files contain 81 function/method
declarations; all were enumerated after reading the files in full.

## Rust ownership and comparison

Rust coverage is distributed across `tidb-ast` hint nodes and restoration,
`tidb-parser/src/select/hint.rs` (syntax parsing),
`tidb-session/src/binding.rs` (basic hint collection for bindings),
`tidb-planner/src/plan_builder.rs` and `plan_builder/from.rs` (selected index,
join, storage, and warning paths), and `tidb-util/src/sem_v2/restricted_hint.rs`
(SEM filtering). There is no single Rust `HintsSet`, complete `ParseStmtHints`
or `ParsePlanHints`, `QBHintHandler`, view-hint state machine, or
`CheckBindingFromHistoryComplete` owner. Existing consumers intentionally use
smaller native seams, and several planner hint integration tests remain
explicit `go-parity-gap` cases.

Because the package is not dependency-closed, no Rust-only adapter was
removed and no speculative second hint pipeline was added. The Go-master
visitor signature migration is already represented by Rust's non-replacing
visitor shape; this receipt records the exact boundary rather than claiming a
partial implementation as package parity.

## Validation

Profile: WIP for a continuing boundary audit; no production fix was made, so a
package-complete Ready claim is intentionally not made.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/hint -count=1` — package compilation passed against Go master (there are no Go test files).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-parser --test all parser_hint -- --test-threads=1` — parser hint source tests passed.
- `rustup run nightly-2026-08-22 rustfmt --edition 2021 --check rust/crates/tidb-parser/src/select/hint.rs rust/crates/tidb-planner/src/plan_builder.rs rust/crates/tidb-planner/src/plan_builder/from.rs rust/crates/tidb-session/src/binding.rs` — passed for the inspected owners.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

- Correctness risk is concentrated in the unported query-block/view state
  machine and full hint-to-optimizer pipeline, including warning order.
- Compatibility risk spans parser, session binding, planner, SEM, and
  executor statement-context consumers; a leaf-only change could diverge from
  Go's shared hint state.
- Performance is unchanged because this audit added no production path.
