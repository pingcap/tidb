# Complete `pkg/util/hint` package receipt

Status: package behavior complete against the pinned Go source. This is a WIP
package claim inside the ongoing repository parity goal, not a Ready claim for
the repository.

## Pinned inventory

Behavioral source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

| Artifact | Lines | Blob |
| --- | ---: | --- |
| `pkg/util/hint/hint.go` | 1,275 | `6812e31fb768f5014bf6455310a823f8cec414f4` |
| `pkg/util/hint/hint_processor.go` | 466 | `85957d00a0a90e4a58e31fdb3acfb6533e9d1f0c` |
| `pkg/util/hint/hint_query_block.go` | 362 | `0262e1b53e6a329eb6a688be30de3aeb586a2bb0` |
| `pkg/util/hint/BUILD.bazel` | 25 | `2fed52261e4984e2d7574c7736dd73c047ecff2b` |

There is no `doc.go`, package test, test support, fixture, testdata, benchmark,
generated source, or platform variant at the pin.

## Go-to-Rust mapping

| Go contract | Rust owner | Decision |
| --- | --- | --- |
| `StmtHints`, `TaskMapNeedBackUp`, deliberate `Clone` omission of hypothetical indexes | `tidb_hint::StmtHints` | Exact field state and clone behavior |
| `ParseStmtHints`, restricted-hint filtering and checker registration | `statement.rs` | Exact duplicate resolution, offsets, warning codes/messages, SET_VAR and HYPO_INDEX handling |
| `IndexJoinHints`, `PlanHints`, `HintedTable`, `HintedIndex` and match methods | `plan.rs` | Direct native representations; match mutation and returned-copy behavior retained |
| `ParsePlanHints` | `plan.rs::parse_plan_hints` | All pinned join, aggregation, index, storage, CTE, LEADING and subquery branches retained |
| restore, de-duplicate, and unmatched-warning helpers | `plan.rs` and `processor.rs` | Exact ordering, lower-case restore, first-occurrence retention, and warning text |
| `HintsSet`, `CollectHint`, `BindHint`, `ParseHintsSet` | `processor.rs` | AST traversal order, insert/select statement-hint rules, binding completeness, and parse normalization retained |
| `QBHintHandler`, `QBHintBuildState`, `GenerateQBName` | `query_block.rs` | Query-block offsets, view paths, unknown/duplicate warnings, and unused-view warnings retained |
| planner-owned view consumption of handler state | `ViewHintContext`, `matching_view_hints`, `for_view_body` | Native boundary for the pinned planner consumer; it does not add a hint behavior |
| Bazel library target | workspace crate `tidb-hint` | Native package owner with AST, parser, model, and datatype dependencies |

Go's `CIStr` retains original and lower-case forms while Rust's parsed `Hint`
stores the parser's canonical uppercase name. Name comparisons that consume Go
lower-case hint constants therefore use case-insensitive comparison in the
Rust owner; this preserves the Go parser/consumer behavior rather than adding
a new accepted SQL spelling.

## Removed non-source surface

The new crate initially carried four internal tests. The pinned package has no
test artifact, so those tests were removed. Source-backed parser, planner,
binding, session, and executor tests remain with the Go packages whose
observable behavior they transcreate.

The old split hint parsing/matching code in planner and session consumers was
removed or redirected to this canonical owner. Cached and fresh statements now
consume the same parsed `StmtHints`/`PlanHints` state; there is no cache-only
hint executor path.

## Consumer integration

- `tidb-parser` emits every hint payload required by the package, including
  warning-preserving parse entrypoints.
- `tidb-planner` consumes query-block, view, index, join, aggregation, CTE, and
  READ_FROM_STORAGE state on its ordinary plan-building path.
- `tidb-session` applies statement hints, bindings, prepared/non-prepared cache
  state, resource groups, and isolation-read engines through the same statement
  context used for fresh planning.
- `tidb-executor` and `tidb-server` propagate catalog TiFlash replica metadata
  into that ordinary planning path.

TiFlash columnar-index path identity and MPP-enforcement warnings belong to the
separate pinned planner package and remain explicit work there; they are not
implemented or claimed by `pkg/util/hint`.

## Validation

WIP commands run from the repository root:

    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    cargo check --locked --manifest-path rust/Cargo.toml -p tidb-hint -p tidb-parser -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-hint -- --list
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-parser --test all parser_hint_source -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib read_from_storage -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib test_index_hints_respect_tikv_isolation_like_go -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-session --lib a_binding_moves_the_access_path_and_leaves_the_rows_alone -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-session --lib a_comment_index_hint_constrains_the_access_path -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-session --lib isolation_read_engines_reach_the_statement_context -- --nocapture
    cargo test --locked --manifest-path rust/Cargo.toml -p tidb-executor --test all create_table_like_copies_tiflash_replica_settings_clearing_availability -- --nocapture
    git diff --check

The first attempted parser command named a nonexistent standalone test target;
the crate's manifest maps source files through the `all` target, and the
corrected command above passed both parser hint tests. Bazel preparation is not
required because no Go, Bazel, or module source changed.

## Risk

The principal compatibility boundary is shared mutable match state: Go mutates
`Matched` fields through slice values, while Rust performs the same mutation
through exclusive slices. Focused planner and session tests prove that the
state reaches ordinary planning, binding, and warning consumers. The package
has no platform-specific behavior.

## Rust-only return-contract alignment (2026-09-06)

The complete four-artifact Go inventory above remains the authority for this
package. The complete `tidb-hint` owner (five source modules, 2,579 lines,
four inline tests) and its direct planner/session consumers were rechecked
before this follow-up. Go permits discarding the results of five `Hinted*`
matching/string methods, three plan-hint restoration helpers, and three
unmatched-hint helpers, plus seven query-block state/offset/view helpers. Rust
had imposed 18 Rust-only `#[must_use]` diagnostics on those direct source
APIs; the annotations were removed without changing parsing, matching,
restoration, warning, or query-block state behavior.

`plan::tests::go_plan_api_returns_may_be_ignored_like_go` and
`query_block::tests::go_query_block_api_returns_may_be_ignored_like_go`
discard all 18 returns under `#[deny(unused_must_use)]`. Before the source
edit, the focused compile failed with exactly 18 `unused_must_use` diagnostics;
after the edit both regressions pass. No Go source, Bazel metadata, Cargo
dependency, generated input, fixture, or platform variant changed, so
`make bazel_prepare` was not required.

Ready evidence for this package-scoped follow-up:

```text
OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-hint --lib go_ --offline --locked
# PASS; 2 tests

OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
  -p tidb-hint --offline --locked --no-fail-fast
# PASS; 4 tests

OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-hint --all-targets --offline --locked
# PASS

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# PASS
git diff --check
# PASS
```
