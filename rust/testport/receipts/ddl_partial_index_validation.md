# `pkg/ddl` partial-index shape/type validation

## Go authority

`pkg/ddl/index.go:4050-4250` defines `CheckAndBuildIndexConditionString` and
`checkIndexCondition`. Go accepts only `IS [NOT] NULL` over a visible,
non-generated column, or one of `=`, `!=`, `>`, `<`, `>=`, `<=` with one column
and one literal. Literal families must match the column: integer/bit/year (and
enum/set), floating-point/decimal, string/temporal/enum/set, or binary. NULL,
unknown/generated columns, unsupported operators/shapes, primary-key WHERE
clauses, and partial indexes on partitioned tables return errno 8200.

## Rust change

The shared `validate_partial_index_condition` validator runs before predicate
compilation in CREATE TABLE, CREATE INDEX, and ALTER TABLE ADD INDEX. It
classifies the parsed literal (`Int`/`Bool`, decimal/float, string, hex/bit,
NULL), checks the resolved visible column and generated-column flag, enforces
the Go type families, and emits `DdlCoded { errno: 8200 }` with Go's message
prefix. Partition and primary-key guards use the same coded error helper.

## Focused regression

`ddl_integration_reorg_backfill_source::partial_index_condition_validation_matches_go`
covers accepted/rejected integer, floating, binary, temporal, enum, NULL,
unknown-column, primary-key, CREATE INDEX, and ALTER INDEX cases. The existing
`fk_alter_meta_and_privilege_source::partial_index_safety_rules_match_go`
continues to cover safe/unsafe FK predicates and exact 1451/1452/1553 behavior.

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all partial_index_ -- --nocapture
# 2 passed; 1 ignored; 0 failed
```

No Go, generated, platform, Bazel, or build-artifact file changed. The full Go
literal/type cross-product and reorg job state remain explicit follow-up
boundaries; partial-index key-offset maintenance and 8272 condition-column
protection are covered by the focused affect-columns carrier.
