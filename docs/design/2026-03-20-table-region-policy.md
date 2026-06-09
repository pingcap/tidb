# Proposal: Table Region Boundary Policy via `ALTER TABLE ... ATTRIBUTES`

## 1. Summary

This proposal introduces a **table-level region boundary policy** for TiDB by reusing the existing
`ALTER TABLE ... ATTRIBUTES` / `ALTER TABLE ... PARTITION ... ATTRIBUTES` DDL path.

The target behavior is intentionally narrow:

- Keep the existing global `split-table` behavior unchanged.
- Keep the existing TiDB active split logic unchanged.
- Do **not** introduce automatic multi-region splitting for a table.
- Only guarantee that a table's key range will eventually **not share a Region with other tables**.

The proposal standardizes one reserved table attribute key:

- `region_policy=exclusive`

When this attribute is applied to a table or partition, TiDB will use the existing PD Region Label Rule path
so that PD can eventually split on the table/partition key-range boundaries and prevent future cross-boundary merge.

This is an **opt-in**, **eventually consistent**, **low-intrusion** design.

---

## 2. Background

Today TiDB already supports:

- `ALTER TABLE t ATTRIBUTES="k=v,..."`
- `ALTER TABLE t PARTITION p ATTRIBUTES="k=v,..."`

Internally, TiDB converts these attributes into a **PD Region Label Rule** with `key-range` data.
The rule range is built from the physical table or partition ID:

- `start_key = GenTablePrefix(id)`
- `end_key   = GenTablePrefix(id+1)`

The important consequence is:

> once a table-level or partition-level label rule exists, PD can treat the rule boundaries as split boundaries
> and can also prevent merge across those boundaries.

That means TiDB already has most of the machinery needed for the desired behavior.

What is missing is not a new split algorithm. What is missing is a **clear, explicit, documented policy key**
that users can rely on to request table-level boundary isolation.

---

## 3. Problem Statement

When `split-table=false`, most tables should continue to behave as they do today:

- no TiDB-side active split on table creation
- low Region count by default
- adjacent tables may initially share a Region

However, some tables need special handling:

- their Region range must not include keys from other tables
- they do not need automatic multi-region splitting
- they do not need any change to the global `split-table` switch

The desired capability is therefore:

> keep the default shared behavior for most tables, but allow selected tables or partitions to opt in to
> an independent key-range boundary using a DDL-level policy.

---

## 4. Goals

### 4.1 In scope

1. Provide a documented, explicit SQL-level way to mark a table or partition as boundary-isolated.
2. Reuse the existing `ALTER TABLE ... ATTRIBUTES` framework.
3. Reuse the existing PD label rule lifecycle logic.
4. Preserve current `split-table` behavior.
5. Preserve current Region split implementation in TiDB.
6. Guarantee **eventual** table/partition boundary isolation from other tables.

### 4.2 Out of scope

1. Automatic creation-time split on `CREATE TABLE`.
2. Automatic multi-region splitting within the table.
3. Scatter / wait-scatter semantics.
4. Any new placement rule mechanism.
5. Any change to the global `split-table` option.
6. Strong synchronous guarantee that the table is isolated before the DDL returns.

---

## 5. User-Facing SQL Design

## 5.1 Reserved attribute key

This proposal reserves the following key for region-boundary policy:

- `region_policy`

Supported value in v1:

- `exclusive`

So the canonical SQL is:

```sql
ALTER TABLE t ATTRIBUTES="region_policy=exclusive";
```

For a partition:

```sql
ALTER TABLE t PARTITION p0 ATTRIBUTES="region_policy=exclusive";
```

## 5.2 Disable / revert

In v1, disable is done by removing the table or partition attribute rule entirely:

```sql
ALTER TABLE t ATTRIBUTES=DEFAULT;
ALTER TABLE t PARTITION p0 ATTRIBUTES=DEFAULT;
```

### Why not support `region_policy=shared` in v1?

Because under the current TiDB implementation, a non-empty `ATTRIBUTES` set creates a PD label rule,
and the existence of that rule itself forms a boundary.

So an explicit `shared` rule would be misleading: a rule with `region_policy=shared` would still create
a boundary simply because the rule exists.

For that reason, v1 supports:

- `region_policy=exclusive` to enable
- `ATTRIBUTES=DEFAULT` to disable

and intentionally does **not** support:

- `region_policy=shared`

---

## 5.3 Examples

### Enable on a normal table

```sql
ALTER TABLE orders ATTRIBUTES="region_policy=exclusive";
```

### Enable on a partition only

```sql
ALTER TABLE orders PARTITION p202601 ATTRIBUTES="region_policy=exclusive";
```

### Clear on a table

```sql
ALTER TABLE orders ATTRIBUTES=DEFAULT;
```

### Clear on a partition

```sql
ALTER TABLE orders PARTITION p202601 ATTRIBUTES=DEFAULT;
```

---

## 6. Semantics

## 6.1 Table-level semantics

`ALTER TABLE t ATTRIBUTES="region_policy=exclusive"` means:

- TiDB creates or updates the existing **table-level PD label rule** for `t`.
- The rule covers the full physical key range of the table.
- If `t` is partitioned, the table-level rule continues to cover:
  - the logical table ID range
  - all current partition physical ID ranges
- PD will eventually split any Region that crosses those boundaries.
- PD will not merge across those boundaries later.

The resulting effect is:

> the Regions that contain table `t` will eventually contain only keys belonging to `t`
> (and its partitions, if this is a table-level rule on a partitioned table).

## 6.2 Partition-level semantics

`ALTER TABLE t PARTITION p ATTRIBUTES="region_policy=exclusive"` means:

- TiDB creates or updates the existing **partition-level PD label rule** for partition `p`.
- PD will eventually isolate the physical range of that partition from other tables/partitions.

This is useful when only certain partitions are hot or operationally sensitive.

## 6.3 Eventual consistency

This proposal does **not** change TiDB's active split path.

Therefore DDL success means:

- the label rule has been accepted and persisted through the normal DDL path
- PD can use it as a split/merge boundary

It does **not** mean:

- the split has already completed when the DDL returns

The isolation guarantee is eventual, not synchronous.

---

## 7. Why `ATTRIBUTES` Is the Right Entry Point

Using `ALTER TABLE ... ATTRIBUTES` is preferable to introducing a new mechanism because:

1. TiDB already has SQL syntax for it.
2. TiDB already converts it into PD label rules.
3. TiDB already has lifecycle handling for rename/truncate/recover/drop/partition changes.
4. `information_schema.attributes` already exposes these rules.
5. The desired behavior is fundamentally about **key-range boundaries**, which is exactly what label rules model.

This proposal therefore standardizes a new **well-defined policy key** on top of the existing mechanism,
rather than building a parallel control path.

---

## 8. Current Code Paths That Will Be Reused

The proposal intentionally reuses the following existing paths.

### 8.1 Table attributes

- `pkg/ddl/executor.go`: `AlterTableAttributes`
- `pkg/ddl/table.go`: `onAlterTableAttributes`

Current behavior already:

1. parses the attributes
2. builds a `label.Rule`
3. calls `rule.Reset(...)`
4. writes the rule to PD via `infosync.PutLabelRule(...)`
5. deletes it on `ATTRIBUTES=DEFAULT`

### 8.2 Partition attributes

- `pkg/ddl/executor.go`: `AlterTablePartitionAttributes`
- `pkg/ddl/table.go`: `onAlterTablePartitionAttributes`

### 8.3 Rule lifecycle reuse

Existing label rule lifecycle handling already covers:

- `rename table`
- `truncate table`
- `recover table`
- `drop table` (deleted later by GC worker)
- `drop schema`
- `add/drop/truncate/exchange partition`

Key existing helpers include:

- `pkg/ddl/table.go`: `getOldLabelRules(...)`
- `pkg/ddl/table.go`: `updateLabelRules(...)`
- `pkg/ddl/partition.go`: `alterTableLabelRule(...)`
- `pkg/ddl/partition.go`: `dropLabelRules(...)`
- `pkg/store/gcworker/gc_worker.go`: delayed GC cleanup for dropped tables

Because of this, the proposal requires **very little lifecycle-specific new logic**.

---

## 9. Proposed Implementation

## 9.1 High-level implementation strategy

The implementation should be deliberately small:

1. Reuse the current `ALTER TABLE ... ATTRIBUTES` DDL path.
2. Introduce a reserved key and validation rule for `region_policy`.
3. Keep the existing label-rule creation and cleanup mechanism unchanged.
4. Keep the existing split-table / active split code unchanged.
5. Document the operational semantics clearly.

In short:

> v1 is primarily a **contract and validation change**, not a new scheduler or split algorithm.

---

## 9.2 Reserved key definition

Add the following constants in the label package, for example in `pkg/ddl/label/attributes.go`:

```go
const (
    regionPolicyKey       = "region_policy"
    regionPolicyExclusive = "exclusive"
)
```

### Validation rules

For v1:

- allow `region_policy=exclusive`
- reject any other value for `region_policy`
- reject duplicated `region_policy`
- recommend case-insensitive value normalization to lower case during validation

Examples:

- valid: `region_policy=exclusive`
- invalid: `region_policy=shared`
- invalid: `region_policy=random`
- invalid: `region_policy=`

---

## 9.3 DDL handling rules

### Table-level DDL

When executing:

```sql
ALTER TABLE t ATTRIBUTES="region_policy=exclusive"
```

TiDB should continue to follow the existing path:

1. parse the attribute string
2. build labels
3. validate `region_policy`
4. call `getIDs(...)`
5. call `rule.Reset(codec, db, table, "", ids...)`
6. persist the rule using the existing PD infosync path

No new DDL job type is required.

### Partition-level DDL

When executing:

```sql
ALTER TABLE t PARTITION p ATTRIBUTES="region_policy=exclusive"
```

TiDB should continue to:

1. resolve partition ID
2. build a partition-level label rule
3. persist it through the current path

No new DDL job type is required.

---

## 9.4 No changes to split logic

The following areas should remain unchanged in v1:

- `split-table` config handling
- `EnableSplitTableRegion`
- `preSplitAndScatter(...)`
- `splitTableRegion(...)`
- `splitPartitionTableRegion(...)`
- active `SplitRegions(...)` usage on table attributes DDL

This is intentional.

The proposal depends only on:

- PD label rule boundaries
- PD's asynchronous split and merge-boundary behavior

This ensures low implementation risk and preserves current system behavior for all other tables.

---

## 10. Detailed Code Changes

## 10.1 `pkg/ddl/label/attributes.go`

### Change
Add reserved-key constants and a dedicated validation helper.

### Proposed helper

A helper like:

```go
func ValidateRegionPolicy(labels []pd.RegionLabel) error
```

Suggested behavior:

- scan labels for `region_policy`
- if absent: return nil
- if present more than once: return error
- if value is not `exclusive`: return error

### Why this file

This is the natural place because it already defines label-related reserved keys such as:

- `db`
- `table`
- `partition`
- `keyspace`

and already owns attribute-level compatibility checks.

---

## 10.2 `pkg/ddl/label/rule.go`

### Change
No behavioral rewrite is needed.

Possible small changes:

- call the new validation helper from the attribute application flow or from the caller
- add comments clarifying that a table-level rule is the boundary mechanism used by `region_policy=exclusive`

### Why no larger change is needed

Because `Rule.Reset(...)` already produces exactly the right key-range model:

- table rule => full table physical range
- partition rule => full partition physical range

That is already sufficient for the v1 goal.

---

## 10.3 `pkg/ddl/executor.go`

### Change
In both:

- `AlterTableAttributes`
- `AlterTablePartitionAttributes`

insert explicit validation after `ApplyAttributesSpec(...)` and before submitting the DDL job.

Pseudo-flow:

```go
rule := label.NewRule()
err = rule.ApplyAttributesSpec(spec.AttributesSpec)
if err != nil { ... }
if err := label.ValidateRegionPolicy(rule.Labels); err != nil { ... }
```

### Why here

This keeps invalid policy values from entering the DDL queue and provides immediate user feedback.

---

## 10.4 `pkg/ddl/table.go`

### Change
No control-flow redesign is needed.

The following current logic should remain the execution backbone:

- `onAlterTableAttributes(...)`
- `onAlterTablePartitionAttributes(...)`

Behavior remains:

- non-empty labels => `infosync.PutLabelRule(...)`
- `ATTRIBUTES=DEFAULT` => delete rule by patch

### Additional note

No special branch is needed for `region_policy=exclusive` in v1.

Once validated, it can flow through the same label-rule persistence logic as any other label.
The important point is that it is now a **documented reserved key** with a defined operational meaning.

---

## 10.5 `pkg/ddl/partition.go`

### Change
No functional redesign is needed.

Existing logic should continue to manage the rule lifecycle for partition operations, including:

- add partition
- drop partition
- truncate partition
- exchange partition
- rollback paths

### Expected reused behavior

- if a table-level rule already exists, add-partition extends its covered ranges
- partition-level rules are removed when a partition is dropped
- truncate partition rewrites the rule to the new physical partition ID
- exchange partition swaps or patches the relevant rules

This is already good enough for the proposed policy.

---

## 10.6 `pkg/executor/infoschema_reader.go`

### Change
Likely no code change needed.

Because `information_schema.attributes` already reads rules from PD, the reserved attribute will automatically
show up there once stored in the rule.

That means users can inspect the policy using existing observability.

---

## 10.7 Documentation

### Required documentation additions

Document the following explicitly:

1. `region_policy=exclusive` is a reserved attribute key.
2. It means the table or partition should eventually have an independent Region boundary.
3. The effect is eventual, not synchronous.
4. `ATTRIBUTES=DEFAULT` removes the policy.
5. `region_policy=shared` is not supported in v1.
6. This feature does not trigger TiDB-side active split and does not create multiple Regions automatically.

---

## 11. Operational Behavior

## 11.1 What users should expect

After:

```sql
ALTER TABLE t ATTRIBUTES="region_policy=exclusive";
```

users should expect:

- the DDL succeeds quickly
- `information_schema.attributes` shows the new attribute
- PD eventually observes and applies boundary split/merge behavior
- after convergence, no Region containing keys from `t` should also contain keys from neighboring tables

## 11.2 What users should **not** expect

Users should not expect:

- immediate split completion before DDL returns
- active TiDB split invocation
- automatic scatter
- multiple Regions inside the table range

---

## 12. Compatibility and Limitations

## 12.1 Compatibility

This proposal is intentionally backward-compatible with the current DDL and label-rule infrastructure.

It does not require:

- new SQL syntax
- new PD APIs
- new DDL job types
- new table metadata fields
- changes to the current split-table switch

## 12.2 Important limitation of the current single-rule model

Today TiDB effectively manages one table-level label rule per table object ID namespace.
Therefore, table-level attributes share one rule lifecycle.

This has an important consequence:

> v1 does not support independently clearing `region_policy=exclusive` while retaining other table-level labels
> without also reconsidering how all table-level attributes are modeled.

Practically, this means:

- `ATTRIBUTES=DEFAULT` clears the whole table-level rule
- if a user wants to keep other labels, they must understand that those labels share the same rule object
- any non-empty table-level rule still creates a boundary because the rule exists

This proposal accepts that limitation for v1.

## 12.3 Existing generic attributes still create boundaries

This proposal does **not** change the existing behavior that any non-empty table-level `ATTRIBUTES` creates a label rule.

Therefore:

- the new reserved key makes the behavior explicit and documented
- but it does not alter the pre-existing side effect that other non-empty table attributes also imply a boundary

Operationally, users should treat `region_policy=exclusive` as the canonical key for this feature.

---

## 13. Lifecycle Behavior

One major benefit of this design is that most lifecycle handling already exists.

## 13.1 Rename table

The existing `getOldLabelRules(...)` + `updateLabelRules(...)` flow already migrates label rules across rename.
The region policy will therefore move with the rule.

## 13.2 Truncate table

The existing truncate flow already rewrites label rules to the new table ID.
The region policy therefore survives truncate correctly.

## 13.3 Recover / flashback-like recovery

The existing recovery path already restores and rewrites label rules.
The region policy therefore naturally participates in recovery.

## 13.4 Drop table

Drop-table cleanup is already delayed until GC.
This behavior should remain unchanged.

This is desirable because it avoids prematurely deleting the boundary metadata before the dropped data range is actually GC-processed.

## 13.5 Drop schema

Drop schema already deletes all table and partition label rules under the schema.
No new logic is required.

## 13.6 Partition changes

Existing partition lifecycle code already handles label-rule maintenance.
This is sufficient for the proposed policy.

---

## 14. Test Plan

## 14.1 Unit tests

Add unit tests around reserved-key validation.

Suggested cases:

1. `region_policy=exclusive` => valid
2. duplicate `region_policy` => invalid
3. `region_policy=shared` => invalid
4. `region_policy=random` => invalid
5. unrelated attributes without `region_policy` => unchanged behavior

## 14.2 Integration tests

Extend SQL integration tests, for example near existing attribute tests.

Suggested cases:

1. normal table:
   - `ALTER TABLE t ATTRIBUTES="region_policy=exclusive"`
   - verify one row appears in `information_schema.attributes`
2. partitioned table, table-level policy:
   - verify table-level rule exists
3. partitioned table, partition-level policy:
   - verify partition-level rule exists
4. `ATTRIBUTES=DEFAULT` clears the rule
5. `DROP DATABASE` removes all rules
6. invalid value returns an error
7. rename/truncate/recover preserve the policy rule behavior

## 14.3 Optional PD-aware end-to-end tests

If a PD-integrated test environment is available, add eventual-convergence verification:

- apply `region_policy=exclusive`
- wait for PD boundary processing
- confirm no Region spans both the target table and a neighboring table

This is optional for v1 but valuable.

---

## 15. Alternatives Considered

## 15.1 Change `split-table`

Rejected because:

- `split-table` is a global default switch
- the requirement is table-level opt-in
- the desired feature is boundary isolation, not generic active splitting

## 15.2 Add a new DDL syntax

Rejected for v1 because:

- current `ATTRIBUTES` syntax already exists
- current label rule pipeline already exists
- lifecycle reuse would be lost or duplicated unnecessarily

## 15.3 Use placement rules instead of label rules

Rejected because placement rules primarily model replica placement and store constraints.
The core requirement here is a **key-range boundary** requirement, which is much more naturally represented by label rules.

---

## 16. Rollout Plan

### Phase 1

Implement only:

- reserved key definition
- validation
- documentation
- tests

No changes to:

- active split code
- `split-table`
- create-table flow

### Phase 2 (optional, future)

If needed in the future, this design can be extended with:

- stronger observability
- create-time support
- optional active split fast-path
- optional automatic multi-region split policy

None of those are required for v1.

---

## 17. Final Recommendation

The recommended v1 design is:

1. Reuse existing `ALTER TABLE ... ATTRIBUTES` and `ALTER TABLE ... PARTITION ... ATTRIBUTES`.
2. Reserve the attribute key `region_policy`.
3. Support only one value in v1:
   - `exclusive`
4. Use `ATTRIBUTES=DEFAULT` to clear the policy.
5. Keep the existing TiDB split logic unchanged.
6. Rely on the existing PD label-rule boundary behavior for eventual isolation.

In one sentence:

> Standardize `region_policy=exclusive` as a documented reserved attribute that reuses the existing
> table/partition label-rule lifecycle to provide eventual table-boundary isolation, without changing TiDB's
> current active split behavior.

