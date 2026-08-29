# `pkg/planner/core/resolve` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `resolve.go` — `TableNameW`, `NodeW`, and the pointer-identity-keyed resolve
  context;
- `result.go` — the complete `ResultField` value;
- `BUILD.bazel` — one production library and no package test target.

There is no `doc.go`, README, test, fixture, benchmark, generated/platform
variant, or package-local harness. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-resolve` owns the complete package. `NodeW::new`,
`with_context`, `clone_with_new_node`, and `resolve_context` retain one shared
context exactly as Go's wrapper does. `Context` implements construction,
addition, individual lookup, and complete-map access. `TableNameW` and
`ResultField` carry every Go field with shared model pointers.

Go keys the context by `*ast.TableName`, not by a table's text. Rust AST values
can move, so `tidb-ast::TableIdentity` uses a shared zero-sized allocation as
the stable equivalent of that Go pointer. A cloned reference retains identity
like a copied Go pointer; a separately parsed or constructed occurrence gets
a distinct identity. AST semantic equality deliberately ignores this storage
identity, as Go AST comparison does. This preserves self-join and repeated-name
lookups without a name-based workaround or unsafe stack addresses.

The ordinary query planner now enters through `NodeW`, shares its context
through recursive query blocks, and records each resolved physical or memory
table. The executor catalog supplies shared `DBInfo` and `TableInfo` objects,
including columns and indexes, rather than the previous absent-metadata
narrowing. The stale planner documentation calling Go's context "unsound" and
dropping it was removed. Broader statement-family coverage remains owned by
their incomplete planner/executor packages and is not claimed here.

## Validation

Profile: WIP; this completes one small Go package within the continuing
repository audit, not the full planner or repository.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 --
  pkg/planner/core/resolve` — passed; the complete Go package matches the pin.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test
  -tags=intest,deadlock -count=1 ./pkg/planner/core/resolve` — passed; the Go
  package has no test files.
- `cargo check --offline -q -p tidb-ast -p tidb-parser -p tidb-resolve
  -p tidb-planner -p tidb-executor -p tidb-expr` — passed.
- `cargo test --offline -q -p tidb-planner --lib plan_builder::tests` —
  passed, 22 tests.
- `cargo test --offline -q -p tidb-ast --lib tests_dml_package_source` —
  passed, 1 test.
- scoped production-file `cargo fmt` and `git diff --check` — passed. Large
  pre-existing source-test files retain their existing formatting; only their
  required `TableRef` initializer field changed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: identity is occurrence-based rather than name-based, and model
  pointers are shared across occurrences from the same catalog snapshot.
- Compatibility: the identity field is ignored by AST equality and restore,
  so parser output and canonical SQL remain unchanged.
- Performance: identity allocation is one zero-sized `Arc` per parsed table
  occurrence; context lookup remains expected constant time, matching Go's
  pointer-keyed map.
