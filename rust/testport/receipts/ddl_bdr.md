# `pkg/ddl/bdr` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 30 | `5aea26bb54f3f776563687160796b0db8cad4310` | `tidb-model` production and unit-test carrier over its existing AST/datatype dependencies |
| `bdr.go` | 132 | `358a955057ad20dae7fbdba27596730c7d50236f` | `tidb_model::ddl_bdr` policy functions; action classes remain owned by the complete `pkg/meta/model` carrier |
| `bdr_test.go` | 578 | `6c38c32a63e1fbe4fd39e95c53860fa1f57316d9` | `ddl_bdr::tests` plus `bdr::tests` for the complete action-class dependency |

There is no package doc, test harness, benchmark, fixture, generated
source/input, build/platform variant, or ownership artifact in the pinned
directory.

## Behavior and integration decision

The existing implementation matches all three source policies. Add-column
admission discounts COMMENT and GENERATED exactly once and allows only Go's
nullable/defaulted shapes on the primary role. Modify-column admission first
requires `FieldType.Equal`, then permits only DEFAULT or DEFAULT+COMMENT.
General admission uses the process-shared `ActionBDRMap`, denies unclassified
actions for managed roles, applies the primary/secondary class rules, and
reads the first typed `ModifyIndexArgs` entry for the unique-index exception,
including Go's panic boundary for a missing first entry.

The common DDL submit path consumes `is_denied` for ordinary jobs and
multi-schema subjobs after decoding the same typed arguments. No alternate BDR
map or statement-local policy remains. The stale executor documentary marker
that still listed `bdr_test.go` as a missing carrier was removed.

## WIP validation

Run from `rust/`:

```text
cargo test --locked -q -p tidb-model --lib ddl_bdr::tests
cargo test --locked -q -p tidb-model --lib bdr::tests
```

The policy suite passes 10 tests and the complete shared classification suite
passes 13 tests.
