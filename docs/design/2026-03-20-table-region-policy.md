# Design: Table Region Policy via `ALTER TABLE ... ATTRIBUTES`

## Introduction

This change standardizes one reserved table attribute:

- `region_policy=exclusive`

It is implemented on top of the existing `ALTER TABLE ... ATTRIBUTES` and PD label rule path.
The goal is narrow: let a table or partition opt in to an independent key-range boundary.
It does not change `split-table`, pre-split behavior, or introduce automatic multi-region split.

## User Interface

Enable on a table:

```sql
ALTER TABLE t ATTRIBUTES="region_policy=exclusive";
```

Enable on a partition:

```sql
ALTER TABLE t PARTITION p0 ATTRIBUTES="region_policy=exclusive";
```

Disable:

```sql
ALTER TABLE t ATTRIBUTES=DEFAULT;
ALTER TABLE t PARTITION p0 ATTRIBUTES=DEFAULT;
```

Rules:

- only `exclusive` is supported in v1
- value matching is case-insensitive and stored as lowercase
- duplicate `region_policy` is rejected
- `region_policy=shared` and any other value are rejected
- disabling uses `ATTRIBUTES=DEFAULT`; v1 does not support clearing only `region_policy` while keeping other attributes

## Semantics

- Table-level DDL reuses the existing table label rule path.
- Partition-level DDL reuses the existing partition label rule path.
- A table rule covers the table ID and, for partitioned tables, all current partition IDs.
- A partition rule covers only the target partition ID.
- Successful DDL means TiDB has persisted the PD label rule; boundary isolation is eventual and depends on PD's existing split/merge handling.
- `information_schema.attributes` exposes the user-visible attribute string, for example `"region_policy=exclusive"`.

## Implementation

- `pkg/ddl/label/attributes.go`
  - reserves `region_policy`
  - normalizes `EXCLUSIVE` to `exclusive`
  - rejects duplicate or invalid values
- `pkg/ddl/label/rule.go`
  - `ApplyAttributesSpec` validates `region_policy`
  - `Reset(...)` builds key-range label rules from table / partition IDs
- `pkg/ddl/executor.go`
  - `AlterTableAttributes`
  - `AlterTablePartitionAttributes`
  - both keep the existing DDL entry points
- `pkg/ddl/table.go`
  - non-empty labels are written with `infosync.PutLabelRule(...)`
  - `ATTRIBUTES=DEFAULT` deletes the rule by patching PD

This feature is therefore a validation and contract change on top of the existing attributes framework,
not a new split scheduler or a new DDL mechanism.

## Compatibility and Limits

- no new SQL syntax
- no new DDL job type
- no change to `split-table` or active split logic
- no synchronous guarantee that the table is already isolated when DDL returns
- clearing the policy through `ATTRIBUTES=DEFAULT` clears the whole attribute rule for that table or partition

## Test Design

Unit tests cover:

- valid `region_policy=exclusive`
- case-insensitive normalization
- duplicate `region_policy`
- invalid values such as `shared` and `random`

SQL tests cover:

- table-level and partition-level `ALTER ... ATTRIBUTES`
- `information_schema.attributes`
- `ATTRIBUTES=DEFAULT` cleanup
