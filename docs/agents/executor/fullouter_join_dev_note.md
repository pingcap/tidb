# TiDB FULL OUTER JOIN Development Notes

## Goal

Track the design decisions and staged implementation plan for adding SQL standard
`FULL OUTER JOIN` support in TiDB.

Tracking issue: https://github.com/pingcap/tidb/issues/69998

The feature is developed in four steps:

1. Add syntax, AST restore, and a feature gate. Keep execution unsupported.
2. Add root planner support while keeping execution unsupported.
3. Add root HashJoin v1 executor support.
4. Add TiFlash MPP shuffle join support after root semantics are stable.

## Scope

Initial target:

- `FULL OUTER JOIN ... ON ...`
- Volcano planner path
- root HashJoin v1 execution
- feature-gated by `tidb_enable_full_outer_join`, default `OFF`

Initial non-goals:

- `FULL OUTER JOIN ... USING (...)`
- `NATURAL FULL OUTER JOIN`
- Cascades planner support
- root IndexJoin, MergeJoin, or HashJoin v2 support
- TiFlash MPP broadcast join support
- non-MPP coprocessor join pushdown

## Decision Log

### Syntax

Only `FULL OUTER JOIN` is introduced as full join syntax.

`FULL JOIN` is not introduced as shorthand. `FULL` is an unreserved keyword in
the current grammar and can be used as an alias or identifier. Keeping
`FULL JOIN` out of scope avoids changing MySQL-compatible parsing behavior such
as:

```sql
SELECT * FROM t1 full JOIN t2 ON t1.a = t2.a;
```

This should continue to parse as `t1 AS full JOIN t2`, not as a full outer join.

### Feature Gate

The feature is guarded by `tidb_enable_full_outer_join`.

The default is `OFF` because the feature changes planner and executor behavior
across several join paths. The gate allows the implementation to land in staged
PRs without making the syntax accidentally executable before root semantics are
complete.

In Step 1, `FULL OUTER JOIN` still returns `ErrNotSupportedYet` even if the
variable is set to `ON`. This is intentional: Step 1 only makes the syntax
recognizable and prevents silent fallback to another join type.

In Step 2, planner-only support allows optimizer tests and planner APIs to
exercise the full outer join path when the variable is `ON`, but user SQL
execution must still fail with `ErrNotSupportedYet("FULL OUTER JOIN")`. This
includes `EXPLAIN`, because TiDB's explain executor still builds the target
executor for partition-pruning metadata. The HashJoin v1 executor guard is
removed only in Step 3.

### No Silent Fallback

After parser support is added, `ast.FullJoin` must not fall through to the
default inner join path in `PlanBuilder`.

Until planner and executor support are fully wired, the safe behavior is to fail
fast with `ErrNotSupportedYet("FULL OUTER JOIN")`.

### Nullability

Full outer join preserves both input sides. Therefore both sides can produce
NULL-extended rows, and output columns from both children must be treated as
nullable.

This differs from left and right outer joins, where only one side is null
extended.

### ON Condition Handling

Full outer join cannot reuse all one-sided outer join assumptions.

For full join:

- `EqualConditions` are join-key conditions.
- one-side `ON` predicates must stay in join semantics unless a later rule can
  prove a transformation is safe.
- optimizer rules that rely on a single preserved side need explicit full join
  guards.

The main correctness risk is accidentally pushing a preserved-side predicate
below the join and filtering rows before NULL extension.

### Join Key Semantics

Root HashJoin v1 should reuse existing join-key null-safe equality handling:

- `=` does not match NULL join keys.
- `<=>` allows `NULL <=> NULL` to match.
- The existing `IsNullEQ` mechanism should represent per-key `<=>` behavior.

No additional full-join-only join-key comparison rule should be introduced
unless existing `IsNullEQ` handling is insufficient.

### Root Execution Strategy

The first executable implementation should be root HashJoin v1 only.

This keeps the first semantic implementation focused on correctness:

- matched rows
- left-only unmatched rows
- right-only unmatched rows
- one-side `ON` filters
- `=` and `<=>` join keys
- spill behavior

Root IndexJoin, MergeJoin, and HashJoin v2 should remain unsupported until they
are designed and tested separately.

### TiFlash MPP Strategy

TiFlash MPP support should be a follow-up after root semantics are stable.

The intended first TiFlash scope is shuffle HashJoin only:

- allow full outer join MPP shuffle join
- do not allow full outer join MPP broadcast join
- do not advertise one-side hash partitioning above a full outer join
- keep `<=>` / `NullEQ` join-key pushdown unsupported until that is handled as a
  separate compatibility item

## Four-Step Development Plan

### Step 1: Syntax and Gate

Status: done in the first PR.

Implementation scope:

- Add parser token/grammar support for `FULL OUTER JOIN`.
- Add AST join type and restore output.
- Keep `FULL JOIN` as alias-compatible syntax, not full join shorthand.
- Add `tidb_enable_full_outer_join`, default `OFF`.
- Add a `PlanBuilder` fail-fast guard so `FULL OUTER JOIN` returns
  `ErrNotSupportedYet` instead of becoming inner join.
- Add parser, sysvar, and planner-gate tests.

User-visible behavior after Step 1:

- `FULL OUTER JOIN` is recognized by the parser.
- `FULL JOIN` remains parsed as alias-compatible syntax.
- Executing `FULL OUTER JOIN` still returns unsupported.

### Step 2: Root Planner Support

Status: planned.

Implementation scope:

- Add logical join type `FullOuterJoin`.
- Wire `PlanBuilder` to build full outer join only when
  `tidb_enable_full_outer_join=ON`.
- Fix full join output nullability.
- Guard predicate pushdown, outer join simplification, join reorder, runtime
  filters, and physical plan enumeration.
- Restrict physical plan support to root HashJoin v1.
- Add a temporary executor build guard so normal execution still returns
  `ErrNotSupportedYet("FULL OUTER JOIN")`.
- Add planner coverage for logical and physical plan behavior.

Expected user-visible behavior after Step 2:

- Planner tests can produce root full outer join plans when the feature gate is
  enabled.
- Executing or explaining `FULL OUTER JOIN ... ON ...` still returns unsupported.
- Unsupported forms and unsupported planner paths still fail fast.

### Step 3: Root HashJoin v1 Executor

Status: planned.

Implementation scope:

- Implement HashJoin v1 full outer join matched and unmatched row behavior.
- Remove the temporary executor build guard from Step 2.
- Add executor and integration coverage.

Expected user-visible behavior after Step 3:

- `FULL OUTER JOIN ... ON ...` works on root HashJoin v1 when the feature gate is
  enabled.
- Unsupported forms and unsupported execution paths still fail fast.

### Step 4: TiFlash MPP Shuffle Join

Status: planned.

Implementation scope:

- Allow TiFlash MPP shuffle HashJoin for full outer join.
- Encode full outer join in TiPB.
- Keep MPP broadcast full outer join unsupported.
- Use a safe output partition property for full outer join.
- Add MPP planner tests.

Expected user-visible behavior after Step 4:

- Full outer join can be planned as TiFlash MPP shuffle join when applicable.
- Broadcast join and `NullEQ` join-key pushdown remain separate follow-up items.

## Progress Tracker

| Step | Scope | Status |
| --- | --- | --- |
| Step 1 | Parser, AST restore, sysvar gate, planner fail-fast guard | Done |
| Step 2 | Root planner semantics and temporary executor unsupported guard | Planned |
| Step 3 | Root HashJoin v1 executor support | Planned |
| Step 4 | TiFlash MPP shuffle full outer join pushdown | Planned |

## Review Checklist

- `FULL JOIN` does not become shorthand for full outer join.
- `FULL OUTER JOIN` never silently degrades to inner join.
- The feature gate defaults to `OFF`.
- Both input sides are nullable after full join semantics are enabled.
- One-side `ON` predicates are not pushed below the join unsafely.
- Root execution starts with HashJoin v1 only.
- Unsupported paths fail fast until explicitly implemented.
- TiFlash MPP full outer join is introduced only after root behavior is covered.
