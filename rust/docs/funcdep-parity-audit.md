# funcdep parity audit (pkg/planner/funcdep)

Audit date: 2026-09-07. Go ref: origin/master a85e0fd5df
(`pkg/planner/funcdep/fd_graph.go` 1285 lines, `doc.go`, and the two
test files). Rust: `rust/crates/tidb-funcdep` (`fd_graph.rs` 1390
lines, `null_reject.rs` for `pkg/planner/util.IsNullRejected`).

## Function map

Every Go function resolves to a Rust counterpart with the same body
semantics: `ClosureOfStrict`/`closureOfStrict` (public wrapper),
`ClosureOfLax`, `ClosureOfEquivalence`, `InClosure`, `ReduceCols`,
`AddStrictFunctionalDependency`, `AddLaxFunctionalDependency`,
`AddNCFunctionalDependency`/`add_conditional`, `addFunctionalDependency`,
`fdEdge.implies`, `addEquivalence`/`add_equivalence_closure`,
`AddEquivalence`, `AddEquivalenceUnion`, `AddConstants`,
`removeColumnsFromSide`, `isConstant`, `isEquivalence`,
`removeColumnsToSide`, `ConstantCols`, `EquivalenceCols`,
`MakeNotNull`, `MakeNullable`, `MakeCartesianProduct`,
`MakeOuterJoin`, `FindPrimaryKey`/`primary_key`, `AllCols`,
`AddFrom`, `MaxOneRow`, `ProjectCols`, `makeEquivMap`, `String`,
`RegisterUniqueID`, `IsHashCodeRegistered`, and the package function
`FindCommonEquivClasses`. The Go size impressions from line counts
dissolve on reading: `MakeCartesianProduct`'s 150-line range is mostly
its doc comment, and `ProjectCols`'s in-place `cnt` compaction is
restructured into retained/substituted vectors with the same
semantics, including the nc-edge tail (intersect-condition keep,
constant/equiv shrink with empty drops).

## Findings and repair

One real divergence, found in `MakeOuterJoin` and repaired this audit:

- The Rust tail carried two invented lines --
  `not_null_cols.union_with(&filter.not_null_cols)` followed by
  `difference_with(inner_cols)` -- maintaining `NotNullCols` across the
  outer join. Go's `MakeOuterJoin` (a85e0fd5df) does not touch
  `NotNullCols`, and its caller (`logicalop/logical_join.go:1003`)
  does not either: `MakeNullable` exists in Go but has no production
  caller. Removed. The trait tests
  (`outer_join_without_a_filter_keeps_both_keys_as_a_combined_key`,
  `outer_join_keeps_inner_key_and_builds_the_combined_key`) already
  asserted Go's behavior (`!not_null_cols.has(inner)`) for a different
  reason -- the inner column is never unioned into the outer set,
  because `make_outer_join` does not `AddFrom` the inner set -- so the
  assertions stand unchanged.
- The same tail merged `HashCodeToUniqueID` with a plain overwrite;
  Go's merge logs a warning and keeps the FIRST registration
  (`AddFrom` already merged Rust-side with `or_insert`). Repaired to
  keep-first and pinned by
  `outer_join_keeps_the_first_unique_id_for_a_duplicate_hash`.

## Semantics spot-verified line-by-line

`MakeOuterJoin` (the inner-edge strictness downgrade through
`NotNullCols`, the filter's NC constants, the rule-331 combined
strict FD with its skip option, the both-sides-strict-key coverage
over the copied sets, the per-column lax cross product from right to
left, the conditional equivalence over `innerCols`, the primary-key
union) and `ProjectCols` (constant-column capture, strict-closure
to-side widening, the determinant/equivalence intersection feeding
`makeEquivMap`, the to-side keep conditions -- subset of constants or
of `NotNullCols` -- and the from-side substitution) both match Go
statement for statement.
