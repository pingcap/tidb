# MV Index Fast Admin Check Refactoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `handleTask` in fast admin check to use a strategy pattern (`indexCheckBuilder`), eliminating all `if idxInfo.MVIndex` branches.

**Architecture:** Introduce an `indexCheckBuilder` interface with two implementations (`normalCheckBuilder`, `mvCheckBuilder`). Each builder encapsulates SQL generation and row parsing. `handleTask` becomes index-type-agnostic by depending only on the interface.

**Tech Stack:** Go, TiDB executor framework, internal SQL execution

**Spec:** `docs/superpowers/specs/2026-03-18-mv-index-fast-check-refactor-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `pkg/executor/check_table_index.go` | `indexCheckBuilder` interface, `normalCheckBuilder`, `mvCheckBuilder`, refactored `handleTask`, shared utilities |

This is a single-file refactoring. All changes stay within `check_table_index.go`. No new files needed.

---

### Task 1: Add `indexCheckBuilder` interface and `normalCheckBuilder`

**Files:**
- Modify: `pkg/executor/check_table_index.go`

This task adds the interface and the normal index builder. The builder wraps the existing `buildChecksumSQLForNormalIndex`, `buildCheckRowSQLForNormalIndex` and `getRecordWithChecksum` logic into struct methods. No behavior change.

- [ ] **Step 1: Add the `indexCheckBuilder` interface and `normalCheckBuilder` struct**

Add after the `extractHandleColumnsAndType` function (line 354). The interface and struct:

```go
// indexCheckBuilder abstracts SQL generation and row parsing for fast admin check.
// handleTask depends only on this interface — it never checks idxInfo.MVIndex.
type indexCheckBuilder interface {
	// handleChecksum returns the SQL expression used for bucketing.
	handleChecksum() string

	// buildChecksumQuery returns complete SQL for the checksum comparison phase.
	buildChecksumQuery(groupByKey, whereKey string) (tableSQL, indexSQL string)

	// buildCheckRowQuery returns complete SQL for the detail row comparison phase.
	buildCheckRowQuery(groupByKey string) (tableSQL, indexSQL string)

	// getRecords parses query results into records with checksums for row comparison.
	getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error)
}

type normalCheckBuilder struct {
	tblName    string
	handleCols []string
	pkTypes    []*types.FieldType
	idxInfo    *model.IndexInfo
	tblInfo    *model.TableInfo
}
```

- [ ] **Step 2: Implement `normalCheckBuilder` methods**

Move logic from the existing package-level functions into struct methods. The `checksumCols` computation (currently duplicated between `buildChecksumSQLForNormalIndex` and `buildCheckRowSQLForNormalIndex`) should be computed once in a helper method.

```go
func (b *normalCheckBuilder) checksumCols() []string {
	cols := append([]string{}, b.handleCols...)
	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		if tblCol.IsVirtualGenerated() && tblCol.Hidden {
			cols = append(cols, tblCol.GeneratedExprString)
		} else {
			cols = append(cols, ColumnName(col.Name.O))
		}
	}
	return cols
}

func (b *normalCheckBuilder) handleChecksum() string {
	return crc32FromCols(b.handleCols)
}

func (b *normalCheckBuilder) buildChecksumQuery(groupByKey, whereKey string) (string, string) {
	checksumExpr := crc32FromCols(b.checksumCols())
	idxName := ColumnName(b.idxInfo.Name.String())

	tableSQL := fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX() WHERE %s = 0 GROUP BY %s",
		checksumExpr, groupByKey, b.tblName, whereKey, groupByKey,
	)
	indexSQL := fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX(%s) WHERE %s = 0 GROUP BY %s",
		checksumExpr, groupByKey, b.tblName, idxName, whereKey, groupByKey,
	)
	return tableSQL, indexSQL
}

func (b *normalCheckBuilder) buildCheckRowQuery(groupByKey string) (string, string) {
	idxName := ColumnName(b.idxInfo.Name.String())
	cols := b.checksumCols()
	colsStr := strings.Join(cols, ", ")
	handleStr := strings.Join(b.handleCols, ", ")
	checksumExpr := fmt.Sprintf("CRC32(MD5(CONCAT_WS(0x2, %s)))", colsStr)

	tableSQL := fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ %s, %s FROM %s USE INDEX() WHERE %s = 0 ORDER BY %s",
		b.tblName, colsStr, checksumExpr, b.tblName, groupByKey, handleStr,
	)
	indexSQL := fmt.Sprintf(
		"SELECT %s, %s FROM %s USE INDEX(%s) WHERE %s = 0 ORDER BY %s",
		colsStr, checksumExpr, b.tblName, idxName, groupByKey, handleStr,
	)
	return tableSQL, indexSQL
}

func (b *normalCheckBuilder) indexColTypes() []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(b.idxInfo.Columns))
	for _, col := range b.idxInfo.Columns {
		colTypes = append(colTypes, &b.tblInfo.Columns[col.Offset].FieldType)
	}
	return colTypes
}

func (b *normalCheckBuilder) getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error) {
	return getRecordWithChecksum(ctx, se, sql, b.tblInfo.IsCommonHandle, b.pkTypes, b.indexColTypes())
}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb && go build ./pkg/executor/...`
Expected: Build succeeds (new code is added, nothing removed yet)

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/check_table_index.go
git commit -m "executor: add indexCheckBuilder interface and normalCheckBuilder"
```

---

### Task 2: Add `mvCheckBuilder`

**Files:**
- Modify: `pkg/executor/check_table_index.go`

Wrap existing `buildChecksumSQLForMVIndex`, `buildCheckRowSQLForMVIndex` logic into `mvCheckBuilder` methods.

- [ ] **Step 1: Add `mvCheckBuilder` struct and methods**

```go
type mvCheckBuilder struct {
	tblName    string
	handleCols []string
	pkTypes    []*types.FieldType
	idxInfo    *model.IndexInfo
	tblInfo    *model.TableInfo
}
```

Move `buildChecksumSQLForMVIndex` logic into `mvCheckBuilder.handleChecksum()` and `mvCheckBuilder.buildChecksumQuery()`. Move `buildCheckRowSQLForMVIndex` logic into `mvCheckBuilder.buildCheckRowQuery()`. Key points:

- `handleChecksum()`: returns `crc32FromCols(handleCols + non-array index cols)` — extracted from the md5Handle computation in current `buildChecksumSQLForMVIndex`.
- `buildChecksumQuery(groupByKey, whereKey)`: produces the full SQL by embedding groupByKey/whereKey directly, no `%s` placeholders.
- `buildCheckRowQuery(groupByKey)`: same — embed groupByKey directly.
- `getRecords()`: calls `getRecordWithChecksum` with array columns mapped to `TypeJSON`.

```go
func (b *mvCheckBuilder) handleChecksum() string {
	crc32Cols := append([]string{}, b.handleCols...)
	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		if len(ExtractCastArrayExpr(tblCol)) == 0 {
			crc32Cols = append(crc32Cols, ColumnName(col.Name.O))
		}
	}
	return crc32FromCols(crc32Cols)
}

func (b *mvCheckBuilder) indexColTypes() []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(b.idxInfo.Columns))
	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		if len(ExtractCastArrayExpr(tblCol)) > 0 {
			colTypes = append(colTypes, types.NewFieldType(mysql.TypeJSON))
		} else {
			colTypes = append(colTypes, &tblCol.FieldType)
		}
	}
	return colTypes
}

func (b *mvCheckBuilder) getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error) {
	return getRecordWithChecksum(ctx, se, sql, b.tblInfo.IsCommonHandle, b.pkTypes, b.indexColTypes())
}
```

For `buildChecksumQuery` and `buildCheckRowQuery`, port the logic from existing `buildChecksumSQLForMVIndex` and `buildCheckRowSQLForMVIndex` respectively, replacing the `%s` placeholders with the actual `groupByKey`/`whereKey` parameters.

- [ ] **Step 2: Add `newIndexCheckBuilder` constructor**

```go
func newIndexCheckBuilder(tblName string, handleCols []string, pkTypes []*types.FieldType,
	idxInfo *model.IndexInfo, tblInfo *model.TableInfo,
) indexCheckBuilder {
	if idxInfo.MVIndex {
		return &mvCheckBuilder{
			tblName: tblName, handleCols: handleCols, pkTypes: pkTypes,
			idxInfo: idxInfo, tblInfo: tblInfo,
		}
	}
	return &normalCheckBuilder{
		tblName: tblName, handleCols: handleCols, pkTypes: pkTypes,
		idxInfo: idxInfo, tblInfo: tblInfo,
	}
}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb && go build ./pkg/executor/...`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/check_table_index.go
git commit -m "executor: add mvCheckBuilder and newIndexCheckBuilder constructor"
```

---

### Task 3: Refactor `handleTask` to use `indexCheckBuilder`

**Files:**
- Modify: `pkg/executor/check_table_index.go:666-878` (the `handleTask` method)

Replace the `if idxInfo.MVIndex` branch and all `fmt.Sprintf` template formatting with builder calls.

- [ ] **Step 1: Rewrite `handleTask` setup section**

Replace lines 676-710 (variable declarations + the `if idxInfo.MVIndex` branch) with:

```go
func (w *checkIndexWorker) handleTask(task checkIndexTask) error {
	defer w.e.wg.Done()

	ctx := kv.WithInternalSourceType(w.e.contextCtx, kv.InternalTxnAdmin)
	se, restoreCtx, err := w.initSessCtx()
	if err != nil {
		return err
	}
	defer restoreCtx()

	idxInfo := w.indexInfos[task.indexOffset]
	tblInfo := w.table.Meta()
	tblName := TableName(w.e.dbName, tblInfo.Name.String())
	handleCols, pkTypes := extractHandleColumnsAndType(tblInfo)

	builder := newIndexCheckBuilder(tblName, handleCols, pkTypes, idxInfo, tblInfo)
	md5Handle := builder.handleChecksum()

	// ... rest unchanged from here (begin txn, etc.)
```

This removes:
- The `indexColTypes` manual construction (lines 692-700) — now inside each builder
- The `if idxInfo.MVIndex` branch (lines 702-710)
- The `tableChecksumSQL/indexChecksumSQL/tableCheckSQL/indexCheckSQL/tableQuery/indexQuery` variable declarations (lines 676-685)

- [ ] **Step 2: Rewrite checksum phase to use builder**

Replace lines 738-740:
```go
// OLD:
tableQuery = fmt.Sprintf(tableChecksumSQL, groupByKey, whereKey, groupByKey)
indexQuery = fmt.Sprintf(indexChecksumSQL, groupByKey, whereKey, groupByKey)
verifyIndexSideQuery(ctx, se, indexQuery)
```

With:
```go
// NEW:
tableQuery, indexQuery := builder.buildChecksumQuery(groupByKey, whereKey)
verifyIndexSideQuery(ctx, se, indexQuery)
```

- [ ] **Step 3: Rewrite detail phase to use builder**

Replace lines 817-833:
```go
// OLD:
groupByKey := fmt.Sprintf(...)
tableQuery = fmt.Sprintf(tableCheckSQL, groupByKey)
indexQuery = fmt.Sprintf(indexCheckSQL, groupByKey)
verifyIndexSideQuery(ctx, se, indexQuery)
// ...
if idxRecords, err = getRecordWithChecksum(ctx, se, indexQuery, tblInfo.IsCommonHandle, pkTypes, indexColTypes); err != nil {
if tblRecords, err = getRecordWithChecksum(ctx, se, tableQuery, tblInfo.IsCommonHandle, pkTypes, indexColTypes); err != nil {
```

With:
```go
// NEW:
groupByKey := fmt.Sprintf(...)
tableQuery, indexQuery := builder.buildCheckRowQuery(groupByKey)
verifyIndexSideQuery(ctx, se, indexQuery)
// ...
if idxRecords, err = builder.getRecords(ctx, se, indexQuery); err != nil {
if tblRecords, err = builder.getRecords(ctx, se, tableQuery); err != nil {
```

- [ ] **Step 4: Remove debug TODO code**

Delete lines 802-804:
```go
// TODO(joechenrh): remove me after testing.
// meetError = true
// currentOffset = int(tableChecksum[0].bucket)
```

- [ ] **Step 5: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb && go build ./pkg/executor/...`
Expected: Build succeeds

- [ ] **Step 6: Commit**

```bash
git add pkg/executor/check_table_index.go
git commit -m "executor: refactor handleTask to use indexCheckBuilder interface"
```

---

### Task 4: Remove old package-level build functions

**Files:**
- Modify: `pkg/executor/check_table_index.go`

Now that `handleTask` uses the builder, the old standalone functions are dead code.

- [ ] **Step 1: Delete the 4 old functions**

Delete these functions (they are now replaced by builder methods):
- `buildChecksumSQLForMVIndex` (lines 430-474)
- `buildCheckRowSQLForMVIndex` (lines 500-568)
- `buildChecksumSQLForNormalIndex` (lines 570-595)
- `buildCheckRowSQLForNormalIndex` (lines 597-624)

Also delete the `CheckTableFastBucketSize` exported var from the `var` block (line 83) if it was replaced by `CheckTableFastBucketSize` used within builders. Check if tests reference it — if they do (`executor.CheckTableFastBucketSize = 4`), keep it exported.

- [ ] **Step 2: Clean up unused imports**

After deleting the functions, check if any imports became unused (e.g., `expression` if only used by the deleted MV function). The `expression` import is used by `mvCheckBuilder` methods so it likely stays.

- [ ] **Step 3: Verify compilation**

Run: `cd /mnt/data/joechenrh/pingcap/tidb && go build ./pkg/executor/...`
Expected: Build succeeds with no dead code

- [ ] **Step 4: Commit**

```bash
git add pkg/executor/check_table_index.go
git commit -m "executor: remove old package-level SQL build functions"
```

---

### Task 5: Run existing tests to verify correctness

**Files:**
- Test: `pkg/executor/test/admintest/admin_test.go`

This is a pure refactoring — existing tests should pass without changes.

- [ ] **Step 1: Run normal index fast check tests**

```bash
cd /mnt/data/joechenrh/pingcap/tidb && go test ./pkg/executor/test/admintest/ -run "TestAdminCheckTableFailed|TestCheckFailReport|TestAdminCheckWithSnapshot|TestAdminCheckTableWithSnapshot" -v -count=1 -timeout 300s
```

Expected: All PASS. These tests exercise the normal index fast check path (`normalCheckBuilder`).

- [ ] **Step 2: Run MV index tests**

```bash
cd /mnt/data/joechenrh/pingcap/tidb && go test ./pkg/executor/test/admintest/ -run "TestAdminCheckMVIndex|TestExtractCastArrayExpr|TestAdminCheckTableWithMultiValuedIndex" -v -count=1 -timeout 300s
```

Expected: All PASS. These tests exercise the MV index fast check path (`mvCheckBuilder`).

- [ ] **Step 3: Run global index tests**

```bash
cd /mnt/data/joechenrh/pingcap/tidb && go test ./pkg/executor/test/admintest/ -run "TestAdminCheckGlobalIndex" -v -count=1 -timeout 300s
```

Expected: All PASS.

- [ ] **Step 4: Run the full admin test suite**

```bash
cd /mnt/data/joechenrh/pingcap/tidb && go test ./pkg/executor/test/admintest/ -v -count=1 -timeout 600s
```

Expected: All 27 tests PASS.
