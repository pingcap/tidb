# Planner Package Issues

## God Files

| File | Lines | Issue |
|------|-------|-------|
| `core/logical_plan_builder.go` | 7,362 | 80+ imports, 50+ builder methods |
| `core/planbuilder.go` | 6,518 | AST-to-plan coupled with DDL/DML/privilege |
| `core/find_best_task.go` | 3,030 | Dense optimization algorithm |
| `core/rule/rule_partition_processor.go` | 2,143 | Partition pruning + union expansion mixed |
| `core/operator/logicalop/logical_join.go` | 2,172 | 6 condition fields, property tracking, schema mgmt |

## Two Parallel Cost Models

**Files**:
- `core/plan_cost_ver1.go` (1,234 lines)
- `core/plan_cost_ver2.go` (1,300 lines)

**Problem**: Both active simultaneously, switched by config. Neither trusted enough to
deprecate the other.

**Ver1 hack** (line 61-64):
```go
selfCost = getCardinality(p.Children()[0], costFlag) * cpuFactor
if p.FromDataSource {
    selfCost = 0 // for compatibility, see issue #36243
}
```

**Fix**: Validate ver2 accuracy, deprecate ver1.

## Type Assertion Heavy Design

**Total**: 1,845 type assertions across planner package.

**Hotspot**: `exhaust_physical_plans()` (lines 56-100) has 18 cases:
```go
switch x := lp.(type) {
case *logicalop.LogicalCTE:     ops, _, err = physicalop.ExhaustPhysicalPlans4LogicalCTE(x, prop)
case *logicalop.LogicalSort:    ops, _, err = physicalop.ExhaustPhysicalPlans4LogicalSort(x, prop)
// ... 16 more cases ...
default: panic("unreachable")
}
```

**Fix**: Add `ExhaustPhysicalPlans()` as interface method on LogicalPlan.

## Algorithm Complexity Issues

### O(n^2) Candidate Comparison
**File**: `core/find_best_task.go:829`
- `compareCandidates()` does histogram lookups in nested loops.
- `findBestTableIndex()` compares every candidate against all others.
- No pruning of dominated candidates early.

### Selectivity Bitmask Limitation
**File**: `cardinality/selectivity.go:67-70`
- Uses int64 as bitmask for expression tracking.
- Falls back to pseudo-selectivity for >63 predicates.
- Known issue with TODO comment.

### Missing Memoization
- `cardinality.Selectivity()` called per candidate path without caching.
- Same expression sets recalculated from scratch multiple times.

## Statistics Access Patterns

**File**: `core/find_best_task.go`
- Lines 2226, 2346, 2457, 2740: Multiple direct accesses to `ds.TableStats.HistColl`
- `statsTbl.HistColl.Pseudo` accessed multiple times without pre-computation.
- No stats caching within planning session.
- Index stats not cached per table.

## Missing Object Pools

**Known gap** (plan_cache_param.go:178):
```go
// TODO: add a sync.Pool for type.FieldType and expression.Constant here.
```

These are among the most frequently allocated objects in the planner.

**Existing pools** (good examples to follow):
- `paramReplacerPool` (plan_cache_param.go:32)
- `paramRestorerPool` (plan_cache_param.go:37)
- `planCacheHasherPool` (plan_cache_utils.go:609)

## Rule Ordering Issues

**File**: `core/rule/rule_init.go:30`
- Rules execute in sequence with implicit dependencies.
- Example: Partition processor must run after predicate pushdown (documented in comments).
- No explicit dependency declaration.
- Changing rule order breaks invariants silently.

## Plan Node Struct Bloat

### LogicalJoin (logical_join.go:47-99)
- 6 condition fields: EqualConditions, NAEQConditions, LeftConditions,
  RightConditions, OtherConditions, DefaultValues
- Property tracking: LeftProperties, RightProperties
- Schema management: FullSchema, FullNames
- Cardinality: EqualCondOutCnt
- Join leaf identification for constant propagation

### DataSource (logical_datasource.go - 825 lines)
- Table/index access path management
- Partition pruning
- Statistics loading
- Too many concerns for a single node
