# MV Index Fast Admin Check Refactoring

## Problem

The current fast admin check (`FastCheckTableExec`) has separate code paths for normal indexes and multi-valued (MV) indexes. The MV-specific logic is interleaved in `handleTask`, making the code hard to maintain and extend. Key issues:

1. `handleTask` branches on `idxInfo.MVIndex` to choose between 4 different SQL builder functions
2. MV indexes use `SUM(handle_crc * element_crc)` while normal indexes use `BIT_XOR(CRC32(...))` — two different checksum models
3. A custom builtin function `JSON_SUM_CRC32` was added to the expression layer solely for this use case (and cannot be pushed down to TiKV)
4. SQL templates use heavy `fmt.Sprintf` with `%s` placeholders, reducing readability

## Design: Phase A — Strategy Abstraction

### Core Abstraction: `indexCheckBuilder`

Extract the difference between MV and normal index checking into a builder interface:

```go
// indexCheckBuilder abstracts SQL generation and row parsing for fast admin check.
// handleTask depends only on this interface — it never checks idxInfo.MVIndex.
type indexCheckBuilder interface {
    // handleChecksum returns the SQL expression used for bucketing.
    // e.g. "CRC32(MD5(CONCAT_WS(0x2, `id`)))"
    handleChecksum() string

    // buildChecksumQuery returns complete SQL for the checksum comparison phase.
    // The builder handles all internal formatting — caller only passes logical parameters.
    buildChecksumQuery(groupByKey, whereKey string) (tableSQL, indexSQL string)

    // buildCheckRowQuery returns complete SQL for the detail row comparison phase.
    buildCheckRowQuery(groupByKey string) (tableSQL, indexSQL string)

    // getRecords parses query results into records with checksums for row comparison.
    // Each builder knows its own column layout (e.g. MV indexes map array columns to TypeJSON).
    getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error)
}
```

Construction:

```go
func newIndexCheckBuilder(tblName string, handleCols []string, pkTypes []*types.FieldType,
    idxInfo *model.IndexInfo, tblInfo *model.TableInfo,
) indexCheckBuilder {
    if idxInfo.MVIndex {
        return &mvCheckBuilder{...}
    }
    return &normalCheckBuilder{...}
}
```

### `handleTask` Refactoring

The only line that is index-type-aware becomes `newIndexCheckBuilder(...)`. Everything else uses the interface:

```go
builder := newIndexCheckBuilder(tblName, handleCols, pkTypes, idxInfo, tblInfo)
md5Handle := builder.handleChecksum()

// Checksum phase loop — identical for MV and normal
for tableRowCntToCheck > LookupCheckThreshold || checkTimes == 0 {
    tableQuery, indexQuery := builder.buildChecksumQuery(groupByKey, whereKey)
    // ... compare checksums, narrow down bucket ...
}

// Detail phase — also identical
if meetError {
    tableQuery, indexQuery := builder.buildCheckRowQuery(groupByKey)
    idxRecords, _ := builder.getRecords(ctx, se, indexQuery)
    tblRecords, _ := builder.getRecords(ctx, se, tableQuery)
    // ... compare handle by handle ...
}
```

Unchanged parts:
- Checksum phase bucketed binary search logic (~80 lines)
- Detail phase handle comparison logic (~50 lines)
- `getGroupChecksum`, `queryToRow`, `getReporter`, `initSessCtx`, and other utility functions

### Builder Implementations

**`normalCheckBuilder`**: wraps existing `buildChecksumSQLForNormalIndex` and `buildCheckRowSQLForNormalIndex` logic. No behavior change.

- `handleChecksum()` → `crc32FromCols(handleCols)`
- `buildChecksumQuery(groupByKey, whereKey)` → `BIT_XOR(CRC32(MD5(CONCAT_WS(handle, idx_cols))))` grouped by bucket
- `buildCheckRowQuery(groupByKey)` → fetches individual rows with handle + index columns + per-row checksum
- `getRecords()` → calls shared `getRecordWithChecksum` with standard `indexColTypes`

**`mvCheckBuilder`**: wraps existing `buildChecksumSQLForMVIndex` and `buildCheckRowSQLForMVIndex` logic. No behavior change.

- `handleChecksum()` → `crc32FromCols(handleCols + non-array index cols)`
- `buildChecksumQuery(groupByKey, whereKey)` → `SUM(handle_crc * JSON_SUM_CRC32(array_expr))` on table side, `SUM(handle_crc * CRC32(hidden_col))` on index side
- `buildCheckRowQuery(groupByKey)` → table side fetches JSON arrays directly; index side uses subquery + `JSON_ARRAYAGG` + `GROUP BY handle`
- `getRecords()` → calls shared `getRecordWithChecksum` with array columns mapped to `TypeJSON`

The existing 4 package-level `buildXxxForMVIndex` / `buildXxxForNormalIndex` functions become builder private methods.

## Scope

### In scope (Phase A)
- Introduce `indexCheckBuilder` interface + two implementations
- Refactor `handleTask` to be index-type-agnostic
- Remove `if idxInfo.MVIndex` branches from `handleTask`
- Remove debug TODO code (line 802-803 in current check_table_index.go)
- Internalize the 4 `buildXxx` package-level functions into builder methods

### Out of scope (future phases)
- **Phase C**: Unify aggregation to BIT_XOR, replace `JSON_SUM_CRC32` with a new `JSON_ARRAY_XOR` function. Migration path: add `mvXorCheckBuilder` implementing the same `indexCheckBuilder` interface, swap in `newIndexCheckBuilder`, delete old `mvCheckBuilder` and `JSON_SUM_CRC32`.
- **Type support**: Remove the type restriction in `indexSupportFastCheck` (builder.go TODO)
- **Master improvements**: Quick pass global checksum, session vars propagation, `verifyIndexSideQuery` with `intest.AssertFunc`, `execdetails.ContextWithInitializedExecDetails` — orthogonal to this refactoring, can be cherry-picked separately.

## Files Changed

| File | Change |
|------|--------|
| `pkg/executor/check_table_index.go` | Add `indexCheckBuilder` interface, `normalCheckBuilder`, `mvCheckBuilder`; refactor `handleTask`; internalize build functions |
| `pkg/executor/builder.go` | `indexSupportFastCheck` unchanged |
| `pkg/executor/test/admintest/admin_test.go` | Existing tests cover correctness; no new tests needed for pure refactoring |
