# `pkg/util/mviewutil` — new-package parity receipt (Go-master)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). The package is NEW
on master (created by the materialized-view DDL commit `94a9cbedab`): there
was no prior Rust owner and no earlier pin.

## Complete Go inventory

The package contains exactly 2 tracked artifacts and 156 lines at the
comparison commit. It has no `doc.go`, test file, fixture/testdata directory,
generated source, benchmark, or `//go:build` platform variants.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 2 | (bazel metadata; no Rust counterpart required) |
| `util.go` | 154 | `bfaeb57720` (tree blob at `94a9cbedab`) |

`util.go` exports five functions: `CheckMaterializedViewSelect`,
`FindVisibleIndexWithPrefixCoveringColumns`,
`FindVisibleIndexesWithPrefixCoveringColumns`,
`HasIndexWithPrefixCoveringColumns`, and the unexported
`findIndexesWithPrefixCoveringColumns` + `indexPrefixCoversColumns` pair.

## Rust owner and implementation

New module `rust/crates/tidb-util/src/mviewutil.rs` (the crate already
depends on `tidb-model`, `tidb-mysql`, `tidb-parser` and `tidb-error`;
`tidb-ast` was added as a dependency — acyclic, `tidb-ast` does not depend on
`tidb-util`):

- `CheckMaterializedViewSelect` → `check_materialized_view_select(&QueryStmt)`.
  Go's `*ast.SelectStmt` assertion becomes the `QueryStmt::Select` arm
  (`SetOpr` returns `Ok`, mirroring Go's fall-through). Refusals cover WITH,
  locking clauses (`SelectLock` — the Rust carrier only stores a lock when
  `FOR UPDATE`/`FOR SHARE`/`LOCK IN SHARE MODE` was written, so a non-`None`
  lock is exactly Go's `LockType != SelectLockNone`), `SELECT INTO`
  (`into_outfile`, Go's `SelectIntoOpt` superset), `AS OF` and `TABLESAMPLE`
  on the single table reference; multi-table joins, derived tables and
  absent FROM return `Ok`. Errors are Go's `dbterror.ErrGeneralUnsupportedDDL`
  identity (DDL class, 8200, `Unsupported %s`) formatted through
  `TerrorError::fast_generate` with Go's exact detail strings.
- The index-layout helpers →
  `find_visible_index_with_prefix_covering_columns` (returns `Option<String>`
  for Go's `(string, bool)`),
  `find_visible_indexes_with_prefix_covering_columns`,
  `has_index_with_prefix_covering_columns`, and the shared
  `find_indexes_with_prefix_covering_columns` /
  `index_prefix_covers_columns`. All preserve Go's order (the `PRIMARY`
  handle branch before the index scan), the
  `excludedIndexName != strings.ToLower(mysql.PrimaryKeyName)` guard,
  state/invisible filtering under `requireVisiblePublic`, prefix-length
  rejection, first-duplicate-column rejection, lowercase name matching, and
  the nil-table/empty-group-by early returns. `IndexInfo`/`IndexColumn`
  nil entries are skipped via `iter_handles`, matching Go's `idx == nil`
  guard.

## Known gaps recorded (not fixed in this batch)

None inside the package: every Go function and branch has a Rust counterpart
and a regression test. Consumers of this package (the `pkg/ddl`
materialized-view core and refresh worker) are queued separately.

## Regression tests

`mviewutil::tests` (5 running tests, SQL fixtures parsed through
`tidb_parser::parse` so the AST shapes are the real parser output):

- `check_select_refuses_unsupported_clauses` — WITH, `FOR UPDATE`,
  `LOCK IN SHARE MODE`, `INTO OUTFILE`, `AS OF TIMESTAMP`, `TABLESAMPLE`,
  each asserting the 8200 code and the full `Unsupported <detail>` message;
- `check_select_accepts_supported_shapes` — plain SELECT, absent FROM,
  comma join, derived table, set operation;
- `find_visible_index_with_prefix_covering_columns_prefers_primary` — Go's
  PK-handle branch reporting `PRIMARY`;
- `find_visible_indexes_filters_state_visibility_prefix_and_exclusion` —
  seven candidate layouts covering the visible/public, prefix-length, short,
  wrong-column and duplicate-column rejections plus the exclusion argument;
- `empty_group_by_and_nil_table_return_no_layouts` — early returns and the
  excluded-`PRIMARY` guard.

Fail-before evidence: the package is new on master — the module and its tests
do not exist in the pre-batch tree (no Rust owner carried any of this
behavior), so the tests bind to symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-util --no-fail-fast
# 560/560 passed (full owner suite, including the 5 new regressions)
cargo +nightly-2026-08-22 check --offline -p tidb-util
# 0 errors (after the Cargo.lock addition of tidb-ast -> tidb-util)
```

No Go source changed in this batch.
