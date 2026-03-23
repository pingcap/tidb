// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/admin"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	exec.BaseExecutor

	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	srcs       []*IndexLookUpExecutor
	done       bool
	exitCh     chan struct{}
	retCh      chan error
	checkIndex bool
}

var _ exec.Executor = &CheckTableExec{}

const (
	// aliasPrefix is the prefix for the alias in check row SQL.
	aliasPrefix = "_c$_"

	maxPartitionTimes = 10
)

var (
	// LookupCheckThreshold is the threshold to do check, exported for test
	LookupCheckThreshold = int64(100)

	// castArrayRegexp is the regexp to extract the expression from `cast(EXPR as TYPE array)`.
	castArrayRegexp = regexp.MustCompile(`(?i)cast\s*\(\s*(.+?)\s+as\s+.+?\s+array\s*\)`)
)

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	for _, src := range e.srcs {
		if err := exec.Open(ctx, src); err != nil {
			return errors.Trace(err)
		}
	}
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckTableExec) Close() error {
	var firstErr error
	close(e.exitCh)
	for _, src := range e.srcs {
		if err := exec.Close(src); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *CheckTableExec) checkTableIndexHandle(ctx context.Context, idxInfo *model.IndexInfo) error {
	// For partition table, there will be multi same index indexLookUpReaders on different partitions.
	for _, src := range e.srcs {
		if src.index.Name.L == idxInfo.Name.L {
			err := e.checkIndexHandle(ctx, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *CheckTableExec) checkIndexHandle(ctx context.Context, src *IndexLookUpExecutor) error {
	cols := src.Schema().Columns
	retFieldTypes := make([]*types.FieldType, len(cols))
	for i := range cols {
		retFieldTypes[i] = cols[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.InitCap(), e.MaxChunkSize())

	var err error
	for {
		err = exec.Next(ctx, src, chk)
		if err != nil {
			e.retCh <- errors.Trace(err)
			break
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return errors.Trace(err)
}

func (e *CheckTableExec) handlePanic(r any) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	idxNames := make([]string, 0, len(e.indexInfos))
	for _, idx := range e.indexInfos {
		if idx.MVIndex || idx.IsColumnarIndex() {
			continue
		}
		idxNames = append(idxNames, idx.Name.O)
	}
	greater, idxOffset, err := admin.CheckIndicesCount(e.Ctx(), e.dbName, e.table.Meta().Name.O, idxNames)
	if err != nil {
		// For admin check index statement, for speed up and compatibility, doesn't do below checks.
		if e.checkIndex {
			return errors.Trace(err)
		}
		if greater == admin.IdxCntGreater {
			err = e.checkTableIndexHandle(ctx, e.indexInfos[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkTableRecord(ctx, idxOffset)
		}
		return errors.Trace(err)
	}

	// The number of table rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjustable. And we can consider the number of records.
	if len(e.srcs) == 1 {
		err = e.checkIndexHandle(ctx, e.srcs[0])
		if err == nil && e.srcs[0].index.MVIndex {
			err = e.checkTableRecord(ctx, 0)
		}
		if err != nil {
			return err
		}
	}
	taskCh := make(chan *IndexLookUpExecutor, len(e.srcs))
	failure := atomicutil.NewBool(false)
	concurrency := min(3, len(e.srcs))
	var wg util.WaitGroupWrapper
	for _, src := range e.srcs {
		taskCh <- src
	}
	for range concurrency {
		wg.RunWithRecover(func() {
			for {
				if fail := failure.Load(); fail {
					return
				}
				select {
				case src := <-taskCh:
					err1 := e.checkIndexHandle(ctx, src)
					if err1 == nil && src.index.MVIndex {
						for offset, idx := range e.indexInfos {
							if idx.ID == src.index.ID {
								err1 = e.checkTableRecord(ctx, offset)
								break
							}
						}
					}
					if err1 != nil {
						failure.Store(true)
						logutil.Logger(ctx).Info("check index handle failed", zap.Error(err1))
						return
					}
				case <-e.exitCh:
					return
				default:
					return
				}
			}
		}, e.handlePanic)
	}

	wg.Wait()
	select {
	case err := <-e.retCh:
		return errors.Trace(err)
	default:
		return nil
	}
}

func (e *CheckTableExec) checkTableRecord(ctx context.Context, idxOffset int) error {
	idxInfo := e.indexInfos[idxOffset]
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	if e.table.Meta().GetPartitionInfo() == nil {
		idx, err := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		if err != nil {
			return err
		}
		return admin.CheckRecordAndIndex(ctx, e.Ctx(), txn, e.table, idx)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx, err := tables.NewIndex(def.ID, e.table.Meta(), idxInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if err := admin.CheckRecordAndIndex(ctx, e.Ctx(), txn, partition, idx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// FastCheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
// It uses a new algorithms to check table data, which is faster than the old one(CheckTableExec).
type FastCheckTableExec struct {
	exec.BaseExecutor

	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	done       bool
	contextCtx context.Context
}

var _ exec.Executor = &FastCheckTableExec{}

// Open implements the Executor Open interface.
func (e *FastCheckTableExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	e.done = false
	e.contextCtx = ctx
	return nil
}

// Next implements the Executor Next interface.
func (e *FastCheckTableExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done || len(e.indexInfos) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	// Here we need check all indexes, includes invisible index
	e.Ctx().GetSessionVars().OptimizerUseInvisibleIndexes = true
	defer func() {
		e.Ctx().GetSessionVars().OptimizerUseInvisibleIndexes = false
	}()

	taskCh := make(chan checkIndexTask, len(e.indexInfos))
	workerPool := workerpool.NewWorkerPool("checkIndex",
		poolutil.CheckTable, 3, e.createWorker)
	workerPool.SetTaskReceiver(taskCh)

	wctx := workerpool.NewContext(ctx)
	workerPool.Start(wctx)

	for i := range e.indexInfos {
		taskCh <- checkIndexTask{indexOffset: i}
	}
	close(taskCh)
	workerPool.Release()

	return wctx.OperatorErr()
}

func (e *FastCheckTableExec) createWorker() workerpool.Worker[checkIndexTask, workerpool.None] {
	return &checkIndexWorker{sctx: e.Ctx(), dbName: e.dbName, table: e.table, indexInfos: e.indexInfos, e: e}
}

func extractHandleColumnsAndType(
	tblMeta *model.TableInfo,
) ([]string, []*types.FieldType) {
	var (
		pkCols  []string
		pkTypes []*types.FieldType
	)

	switch {
	case tblMeta.IsCommonHandle:
		pkColsInfo := tblMeta.GetPrimaryKey().Columns
		for _, colInfo := range pkColsInfo {
			pkCols = append(pkCols, ColumnName(colInfo.Name.O))
			pkTypes = append(pkTypes, &tblMeta.Columns[colInfo.Offset].FieldType)
		}
	case tblMeta.PKIsHandle:
		pkCols = append(pkCols, ColumnName(tblMeta.GetPkName().O))
		pkTypes = append(pkTypes, types.NewFieldType(mysql.TypeLonglong))
	default: // support decoding _tidb_rowid.
		pkCols = append(pkCols, ColumnName(model.ExtraHandleName.O))
		pkTypes = append(pkTypes, types.NewFieldType(mysql.TypeLonglong))
	}

	return pkCols, pkTypes
}

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

// checksumCols builds the columns list used for checksumming.
// This logic is shared between buildChecksumQuery and buildCheckRowQuery.
func (b *normalCheckBuilder) checksumCols() []string {
	cols := make([]string, len(b.handleCols))
	copy(cols, b.handleCols)
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

// handleChecksum returns the CRC32 expression over handle columns, used for bucketing.
func (b *normalCheckBuilder) handleChecksum() string {
	return crc32FromCols(b.handleCols)
}

// buildChecksumQuery returns complete SQL for the checksum comparison phase,
// producing the same SQL as buildChecksumSQLForNormalIndex but with actual values
// instead of %s placeholders.
func (b *normalCheckBuilder) buildChecksumQuery(groupByKey, whereKey string) (string, string) {
	checksumCols := b.checksumCols()
	md5HandleAndIndexCol := crc32FromCols(checksumCols)
	idxName := ColumnName(b.idxInfo.Name.String())

	tableSQL := fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX() WHERE %s = 0 GROUP BY %s",
		md5HandleAndIndexCol, groupByKey, b.tblName, whereKey, groupByKey,
	)
	indexSQL := fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX(%s) WHERE %s = 0 GROUP BY %s",
		md5HandleAndIndexCol, groupByKey, b.tblName, idxName, whereKey, groupByKey,
	)
	return tableSQL, indexSQL
}

// buildCheckRowQuery returns complete SQL for the detail row comparison phase,
// producing the same SQL as buildCheckRowSQLForNormalIndex but with actual values
// instead of %s placeholders.
func (b *normalCheckBuilder) buildCheckRowQuery(groupByKey string) (string, string) {
	checksumCols := b.checksumCols()
	idxName := ColumnName(b.idxInfo.Name.String())

	md5Handle := strings.Join(b.handleCols, ", ")
	checksumColsStr := strings.Join(checksumCols, ", ")
	md5HandleAndIndexCol := fmt.Sprintf("CRC32(MD5(CONCAT_WS(0x2, %s)))", checksumColsStr)

	tableSQL := fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ %s, %s FROM %s USE INDEX() WHERE %s = 0 ORDER BY %s",
		b.tblName, checksumColsStr, md5HandleAndIndexCol, b.tblName, groupByKey, md5Handle,
	)
	indexSQL := fmt.Sprintf(
		"SELECT %s, %s FROM %s USE INDEX(%s) WHERE %s = 0 ORDER BY %s",
		checksumColsStr, md5HandleAndIndexCol, b.tblName, idxName, groupByKey, md5Handle,
	)
	return tableSQL, indexSQL
}

// indexColTypes builds field types for index columns. For normal indexes,
// uses &tblCol.FieldType for each index column.
func (b *normalCheckBuilder) indexColTypes() []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(b.idxInfo.Columns))
	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		colTypes = append(colTypes, &tblCol.FieldType)
	}
	return colTypes
}

// getRecords parses query results into records with checksums for row comparison.
func (b *normalCheckBuilder) getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error) {
	return getRecordWithChecksum(ctx, se, sql, b.tblInfo.IsCommonHandle, b.pkTypes, b.indexColTypes())
}

// mvXorCheckBuilder generates BIT_XOR-based checksum SQL for multi-valued indexes.
// It uses JSON_ARRAY_XOR_CRC32 on the table side and BIT_XOR(CRC32(MD5(...))) on
// the index side, unifying the aggregation method with normalCheckBuilder.
type mvXorCheckBuilder struct {
	tblName    string
	handleCols []string
	pkTypes    []*types.FieldType
	idxInfo    *model.IndexInfo
	tblInfo    *model.TableInfo
}

// handleChecksum returns the CRC32 expression over handle columns only, used for bucketing.
// Non-array index columns are not included here because they are folded into the per-entry CRC.
func (b *mvXorCheckBuilder) handleChecksum() string {
	return crc32FromCols(b.handleCols)
}

// buildChecksumQuery returns complete SQL for the checksum comparison phase.
// Table side: BIT_XOR(JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle, non_array)))
// Index side: BIT_XOR(CRC32(MD5(CONCAT_WS(0x2, handle, non_array, hidden_col))))
func (b *mvXorCheckBuilder) buildChecksumQuery(groupByKey, whereKey string) (string, string) {
	idxName := ColumnName(b.idxInfo.Name.String())

	var (
		tableFilterCol string
		tableArrayExpr string
		prefixCols     = make([]string, len(b.handleCols))
		indexCRC32Cols = make([]string, len(b.handleCols))
	)
	copy(prefixCols, b.handleCols)
	copy(indexCRC32Cols, b.handleCols)

	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		rawExpr := ExtractCastArrayExpr(tblCol)
		generatedExpr := tblCol.GeneratedExprString
		colName := ColumnName(col.Name.O)

		if len(rawExpr) > 0 {
			tableFilterCol = fmt.Sprintf("(JSON_LENGTH(%s) > 0 or JSON_LENGTH(%s) is null)", rawExpr, rawExpr)
			// generatedExpr is "cast(X as TYPE array)"; strip outer "cast" to get "(X as TYPE array)"
			// then build "JSON_ARRAY_XOR_CRC32(X as TYPE array, prefix)".
			// Do NOT lowercase generatedExpr: string literal path keys (e.g. '$.myField') are case-sensitive.
			inner := strings.TrimPrefix(generatedExpr, "cast")
			inner = strings.TrimSuffix(inner, ")")
			prefix := fmt.Sprintf("CONCAT_WS(0x2, %s)", strings.Join(prefixCols, ", "))
			tableArrayExpr = fmt.Sprintf("JSON_ARRAY_XOR_CRC32%s, %s)", inner, prefix)
			indexCRC32Cols = append(indexCRC32Cols, colName)
		} else {
			prefixCols = append(prefixCols, colName)
			indexCRC32Cols = append(indexCRC32Cols, colName)
		}
	}

	indexCRC32Expr := fmt.Sprintf("CRC32(MD5(CONCAT_WS(0x2, %s)))", strings.Join(indexCRC32Cols, ", "))

	tableSQL := fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX() WHERE %s AND %s = 0 GROUP BY %s",
		b.tblName, tableArrayExpr, groupByKey,
		b.tblName, tableFilterCol, whereKey, groupByKey,
	)
	indexSQL := fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX(%s) WHERE %s = 0 GROUP BY %s",
		indexCRC32Expr, groupByKey, b.tblName, idxName, whereKey, groupByKey,
	)
	return tableSQL, indexSQL
}

// buildCheckRowQuery returns complete SQL for the detail row comparison phase.
// Table side: per-row JSON_ARRAY_XOR_CRC32 as checksum, ordered by handle.
// Index side: subquery with GROUP BY handle, BIT_XOR per handle, JSON_ARRAYAGG for array values.
func (b *mvXorCheckBuilder) buildCheckRowQuery(groupByKey string) (string, string) {
	idxName := ColumnName(b.idxInfo.Name.String())

	var (
		tableFilterCol  string
		tableSelectCols = make([]string, len(b.handleCols))
		prefixCols      = make([]string, len(b.handleCols))
		tableArrayExpr  string

		indexSelectCols   = make([]string, len(b.handleCols))
		indexSubqueryCols = make([]string, len(b.handleCols))
		indexCRC32Cols    = make([]string, len(b.handleCols))
	)
	copy(tableSelectCols, b.handleCols)
	copy(prefixCols, b.handleCols)
	copy(indexSelectCols, b.handleCols)
	copy(indexSubqueryCols, b.handleCols)
	copy(indexCRC32Cols, b.handleCols)

	for _, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		generatedExpr := tblCol.GeneratedExprString
		rawExpr := ExtractCastArrayExpr(tblCol)
		colName := ColumnName(col.Name.O)

		if len(rawExpr) > 0 {
			tableFilterCol = fmt.Sprintf("(JSON_LENGTH(%s) > 0 or JSON_LENGTH(%s) is null)", rawExpr, rawExpr)
			tableSelectCols = append(tableSelectCols, rawExpr)

			// Index subquery columns for array column:
			// 1. The raw hidden col for CRC32 in the per-entry checksum (computed in subquery)
			indexCRC32Cols = append(indexCRC32Cols, colName)

			// 2. Cast-back expression for error reporting via JSON_ARRAYAGG.
			// Remove " array" suffix from the type to get a plain CAST expression on the index side.
			// Do NOT lowercase generatedExpr: string literal path keys (e.g. '$.myField') are case-sensitive.
			aliasArray := buildAlias(col.Name, "_array")
			arrayExpr := strings.Replace(generatedExpr, " array", "", 1)
			arrayExpr = strings.Replace(arrayExpr, rawExpr, colName, 1)
			arrayExpr = fmt.Sprintf("(%s) as %s", arrayExpr, aliasArray)
			indexSubqueryCols = append(indexSubqueryCols, arrayExpr)
			indexSelectCols = append(indexSelectCols, fmt.Sprintf("JSON_ARRAYAGG(%s)", aliasArray))
		} else {
			tableSelectCols = append(tableSelectCols, colName)
			prefixCols = append(prefixCols, colName)

			alias := buildAlias(col.Name, "")
			indexSubqueryCols = append(indexSubqueryCols, fmt.Sprintf("%s AS %s", colName, alias))
			indexSelectCols = append(indexSelectCols, fmt.Sprintf("MIN(%s)", alias))
			indexCRC32Cols = append(indexCRC32Cols, colName)
		}
	}

	// Table side checksum: JSON_ARRAY_XOR_CRC32(array_expr, CONCAT_WS(0x2, handle, non_array))
	// Do NOT lowercase the generated expression: string literal path keys (e.g. '$.myField') are case-sensitive.
	inner := strings.TrimPrefix(b.tblInfo.Columns[b.idxInfo.Columns[b.arrayColIdx()].Offset].GeneratedExprString, "cast")
	inner = strings.TrimSuffix(inner, ")")
	prefix := fmt.Sprintf("CONCAT_WS(0x2, %s)", strings.Join(prefixCols, ", "))
	tableArrayExpr = fmt.Sprintf("JSON_ARRAY_XOR_CRC32%s, %s)", inner, prefix)

	// Index side checksum: compute CRC32(MD5(CONCAT_WS(...))) per entry IN the subquery,
	// then BIT_XOR in the outer query. This avoids referencing raw columns in the outer scope.
	entryCRCAlias := buildAlias(ast.NewCIStr("entry"), "_crc")
	entryCRCExpr := fmt.Sprintf("CRC32(MD5(CONCAT_WS(0x2, %s))) as %s", strings.Join(indexCRC32Cols, ", "), entryCRCAlias)
	indexSubqueryCols = append(indexSubqueryCols, entryCRCExpr)
	indexCRC32Expr := fmt.Sprintf("BIT_XOR(%s)", entryCRCAlias)

	handleColsStr := strings.Join(b.handleCols, ", ")

	tableSQL := fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ %s, %s FROM %s USE INDEX() WHERE %s AND %s = 0 ORDER BY %s",
		b.tblName, strings.Join(tableSelectCols, ", "), tableArrayExpr,
		b.tblName, tableFilterCol, groupByKey, handleColsStr,
	)
	indexSQL := fmt.Sprintf(
		"SELECT %s, %s FROM (SELECT %s FROM %s USE INDEX(%s) WHERE %s = 0) tmp GROUP BY %s ORDER BY %s",
		strings.Join(indexSelectCols, ", "), indexCRC32Expr,
		strings.Join(indexSubqueryCols, ", "), b.tblName,
		idxName, groupByKey, handleColsStr, handleColsStr,
	)
	return tableSQL, indexSQL
}

// arrayColIdx returns the index of the array column in idxInfo.Columns.
func (b *mvXorCheckBuilder) arrayColIdx() int {
	for i, col := range b.idxInfo.Columns {
		tblCol := b.tblInfo.Columns[col.Offset]
		if len(ExtractCastArrayExpr(tblCol)) > 0 {
			return i
		}
	}
	return -1
}

// indexColTypes builds field types for MV index columns. Array columns are mapped to TypeJSON.
func (b *mvXorCheckBuilder) indexColTypes() []*types.FieldType {
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

// getRecords parses query results into records with checksums for row comparison.
func (b *mvXorCheckBuilder) getRecords(ctx context.Context, se sessionctx.Context, sql string) ([]*recordWithChecksum, error) {
	return getRecordWithChecksum(ctx, se, sql, b.tblInfo.IsCommonHandle, b.pkTypes, b.indexColTypes())
}

// newIndexCheckBuilder creates the appropriate indexCheckBuilder for the given index.
// For MV indexes, it returns an mvXorCheckBuilder; for normal indexes, a normalCheckBuilder.
func newIndexCheckBuilder(tblName string, handleCols []string, pkTypes []*types.FieldType,
	idxInfo *model.IndexInfo, tblInfo *model.TableInfo,
) indexCheckBuilder {
	if idxInfo.MVIndex {
		return &mvXorCheckBuilder{
			tblName: tblName, handleCols: handleCols, pkTypes: pkTypes,
			idxInfo: idxInfo, tblInfo: tblInfo,
		}
	}
	return &normalCheckBuilder{
		tblName: tblName, handleCols: handleCols, pkTypes: pkTypes,
		idxInfo: idxInfo, tblInfo: tblInfo,
	}
}

type checkIndexWorker struct {
	sctx       sessionctx.Context
	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	e          *FastCheckTableExec
}

func (w *checkIndexWorker) initSessCtx() (sessionctx.Context, func(), error) {
	se, err := w.e.BaseExecutor.GetSysSession()
	if err != nil {
		return nil, nil, err
	}

	sessVars := se.GetSessionVars()
	originOptUseInvisibleIdx := sessVars.OptimizerUseInvisibleIndexes
	originMemQuotaQuery := sessVars.MemQuotaQuery
	originSkipMissingPartitionStats := sessVars.SkipMissingPartitionStats

	sessVars.OptimizerUseInvisibleIndexes = true
	sessVars.MemQuotaQuery = w.sctx.GetSessionVars().MemQuotaQuery
	// Make sure using index scan for global index.
	// See https://github.com/pingcap/tidb/blob/2010151c503c1a0b74d63e52a6019e03afada21d/pkg/planner/core/logical_plan_builder.go#L4506-L4518
	sessVars.SkipMissingPartitionStats = true
	snapshot := w.e.Ctx().GetSessionVars().SnapshotTS
	if snapshot != 0 {
		_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, fmt.Sprintf("set session tidb_snapshot = %d", snapshot))
		if err != nil {
			logutil.BgLogger().Error("fail to set tidb_snapshot", zap.Error(err), zap.Uint64("snapshot ts", snapshot))
		}
	}

	releaseCtx := kv.WithInternalSourceType(w.e.contextCtx, kv.InternalTxnAdmin)
	return se, func() {
		sessVars.OptimizerUseInvisibleIndexes = originOptUseInvisibleIdx
		sessVars.MemQuotaQuery = originMemQuotaQuery
		sessVars.SkipMissingPartitionStats = originSkipMissingPartitionStats
		if snapshot != 0 {
			_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, "set session tidb_snapshot = 0")
			if err != nil {
				logutil.BgLogger().Error("fail to set tidb_snapshot to 0", zap.Error(err))
			}
		}
		w.e.BaseExecutor.ReleaseSysSession(releaseCtx, se)
	}, nil
}

// ExtractCastArrayExpr checks the expression from `cast(EXPR as TYPE array)`
// exported for test.
func ExtractCastArrayExpr(col *model.ColumnInfo) string {
	if col.FieldType.GetType() != mysql.TypeJSON {
		return ""
	}

	matches := castArrayRegexp.FindStringSubmatch(col.GeneratedExprString)
	if len(matches) < 2 {
		return ""
	}

	return strings.TrimSpace(matches[1])
}

func (w *checkIndexWorker) getReporter(indexOffset int) *consistency.Reporter {
	idxInfo := w.indexInfos[indexOffset]
	tblInfo := w.table.Meta()

	return &consistency.Reporter{
		HandleEncode: func(handle kv.Handle) kv.Key {
			return tablecodec.EncodeRecordKey(w.table.RecordPrefix(), handle)
		},
		IndexEncode: func(idxRow *consistency.RecordData) kv.Key {
			var idx table.Index
			for _, v := range w.table.Indices() {
				if strings.EqualFold(v.Meta().Name.String(), idxInfo.Name.O) {
					idx = v
					break
				}
			}
			if idx == nil {
				return nil
			}
			sc := w.sctx.GetSessionVars().StmtCtx
			k, _, err := idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxRow.Values[:len(idx.Meta().Columns)], idxRow.Handle, nil)
			if err != nil {
				return nil
			}
			return k
		},
		Tbl:             tblInfo,
		Idx:             idxInfo,
		EnableRedactLog: w.sctx.GetSessionVars().EnableRedactLog,
		Storage:         w.sctx.GetStore(),
	}
}

// HandleTask implements the Worker interface.
func (w *checkIndexWorker) HandleTask(task checkIndexTask, _ func(workerpool.None)) error {
	return w.handleTask(task)
}

func (w *checkIndexWorker) handleTask(task checkIndexTask) error {

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

	if _, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}

	bucketSize := int(CheckTableFastBucketSize.Load())
	var (
		checkTimes         = 0
		tableRowCntToCheck = int64(0)
		offset             = 0
		mod                = 1

		meetError = false
		ir        = w.getReporter(task.indexOffset)
	)

	for tableRowCntToCheck > LookupCheckThreshold || checkTimes == 0 {
		checkTimes++
		if checkTimes == maxPartitionTimes {
			logutil.BgLogger().Warn("compare checksum by group reaches time limit", zap.Int("times", checkTimes))
			break
		}
		whereKey := fmt.Sprintf("((CAST(%s AS SIGNED) - %d) MOD %d)", md5Handle, offset, mod)
		groupByKey := fmt.Sprintf("((CAST(%s AS SIGNED) - %d) DIV %d MOD %d)", md5Handle, offset, mod, bucketSize)
		if checkTimes == 1 {
			whereKey = "0"
		}

		tableQuery, indexQuery := builder.buildChecksumQuery(groupByKey, whereKey)
		verifyIndexSideQuery(ctx, se, indexQuery)

		logutil.BgLogger().Info(
			"fast check table by group",
			zap.String("table name", tblInfo.Name.String()),
			zap.String("index name", idxInfo.Name.String()),
			zap.Int("times", checkTimes),
			zap.Int("current offset", offset), zap.Int("current mod", mod),
			zap.String("table sql", tableQuery), zap.String("index sql", indexQuery),
		)

		// compute table side checksum.
		tableChecksum, err := getGroupChecksum(ctx, se, tableQuery)
		if err != nil {
			return err
		}

		// compute index side checksum.
		indexChecksum, err := getGroupChecksum(ctx, se, indexQuery)
		if err != nil {
			return err
		}

		currentOffset := 0

		// Every checksum in table side should be the same as the index side.
		for i := range min(len(tableChecksum), len(indexChecksum)) {
			if tableChecksum[i].bucket != indexChecksum[i].bucket || tableChecksum[i].checksum != indexChecksum[i].checksum {
				if tableChecksum[i].bucket <= indexChecksum[i].bucket {
					currentOffset = int(tableChecksum[i].bucket)
					tableRowCntToCheck = tableChecksum[i].count
				} else {
					currentOffset = int(indexChecksum[i].bucket)
					tableRowCntToCheck = indexChecksum[i].count
				}
				meetError = true
				break
			}
		}

		if !meetError {
			if len(tableChecksum) < len(indexChecksum) {
				// Table side has fewer buckets.
				i := len(tableChecksum)
				currentOffset = int(indexChecksum[i].bucket)
				tableRowCntToCheck = indexChecksum[i].count
				meetError = true
			} else if len(indexChecksum) < len(tableChecksum) {
				// Index side has fewer buckets.
				i := len(indexChecksum)
				currentOffset = int(tableChecksum[i].bucket)
				tableRowCntToCheck = tableChecksum[i].count
				meetError = true
			} else if checkTimes != 1 {
				// Both sides have same buckets, which means no error found.
				// But error is detected in previous round, it's unexpected.
				logutil.BgLogger().Error(
					"unexpected result, no error detected in this round, but an error is detected in the previous round",
					zap.Int("times", checkTimes), zap.Int("offset", offset), zap.Int("mod", mod))
			}
		}

		offset += currentOffset * mod
		mod *= bucketSize
	}

	failpoint.Inject("mockMeetErrorInCheckTable", func(val failpoint.Value) {
		// Only set meetError when no error detected.
		if v, ok := val.(bool); ok && !meetError {
			meetError = v
		}
	})

	if meetError {
		groupByKey := fmt.Sprintf("((CAST(%s AS SIGNED) - %d) MOD %d)", md5Handle, offset, mod)
		tableQuery, indexQuery := builder.buildCheckRowQuery(groupByKey)
		verifyIndexSideQuery(ctx, se, indexQuery)

		var (
			lastTableRecord *recordWithChecksum
			tblRecords      []*recordWithChecksum
			idxRecords      []*recordWithChecksum
		)

		if idxRecords, err = builder.getRecords(ctx, se, indexQuery); err != nil {
			return err
		}

		if tblRecords, err = builder.getRecords(ctx, se, tableQuery); err != nil {
			return err
		}

		for i := range min(len(tblRecords), len(idxRecords)) {
			tableRecord := tblRecords[i]
			indexRecord := idxRecords[i]
			handleCmp := tableRecord.Handle.Compare(indexRecord.Handle)
			if handleCmp == 0 {
				if indexRecord.checksum != tableRecord.checksum {
					err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord.RecordData, tableRecord.RecordData)
					return err
				}
				lastTableRecord = tableRecord
			} else if handleCmp > 0 {
				if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
					err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord.RecordData, lastTableRecord.RecordData)
				} else {
					err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord.RecordData, nil)
				}
				return err
			} else {
				return ir.ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, nil, tableRecord.RecordData)
			}
		}

		if len(idxRecords) < len(tblRecords) {
			// Table side has more rows
			tableRecord := tblRecords[len(idxRecords)]
			err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, nil, tableRecord.RecordData)
		} else if len(tblRecords) < len(idxRecords) {
			// Index side has more rows
			indexRecord := idxRecords[len(tblRecords)]
			if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
				err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord.RecordData, lastTableRecord.RecordData)
			} else {
				err = ir.ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord.RecordData, nil)
			}
		} else {
			// Both sides have same rows, but no error detected, this shouldn't happen.
			err = errors.Errorf("no error detected during row comparison, but an error is detected in the previous round")
		}
		return err
	}

	return nil
}

// Close implements the Worker interface.
func (*checkIndexWorker) Close() error {
	return nil
}

type checkIndexTask struct {
	indexOffset int
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (c checkIndexTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return "fast_check_table", "RecoverArgs", errors.Errorf("checkIndexTask panicked, indexOffset: %d", c.indexOffset)
}

type groupByChecksum struct {
	bucket   uint64
	checksum uint64
	count    int64
}

type recordWithChecksum struct {
	*consistency.RecordData
	checksum uint64
}

func verifyIndexSideQuery(ctx context.Context, se sessionctx.Context, sql string) {
	rows, err := queryToRow(ctx, se, "explain "+sql)
	if err != nil {
		panic(err)
	}

	isTableScan := false
	isIndexScan := false
	for _, row := range rows {
		op := row.GetString(0)
		if strings.Contains(op, "TableFullScan") {
			isTableScan = true
		} else if strings.Contains(op, "IndexFullScan") {
			isIndexScan = true
		}
	}

	if isTableScan || !isIndexScan {
		panic(fmt.Sprintf("check query %s error, table scan: %t, index scan: %t",
			sql, isTableScan, isIndexScan))
	}
}

func queryToRow(qCtx context.Context, se sessionctx.Context, sql string) ([]chunk.Row, error) {
	qCtx = kv.WithInternalSourceType(qCtx, kv.InternalTxnAdmin)
	rs, err := se.GetSQLExecutor().ExecuteInternal(qCtx, sql)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rs.Close(); err != nil {
			logutil.BgLogger().Warn("close result set failed", zap.Error(err))
		}
	}()
	return sqlexec.DrainRecordSet(qCtx, rs, 4096)
}

func getGroupChecksum(ctx context.Context, se sessionctx.Context, sql string) ([]groupByChecksum, error) {
	rows, err := queryToRow(ctx, se, sql)
	if err != nil {
		return nil, err
	}

	checksums := make([]groupByChecksum, 0, len(rows))
	for _, row := range rows {
		checksums = append(checksums, groupByChecksum{
			bucket:   row.GetUint64(1),
			checksum: row.GetUint64(0),
			count:    row.GetInt64(2),
		})
	}

	slices.SortFunc(checksums, func(i, j groupByChecksum) int {
		return cmp.Compare(i.bucket, j.bucket)
	})

	return checksums, nil
}

func getRecordWithChecksum(
	ctx context.Context, se sessionctx.Context, sql string,
	isCommonHandle bool,
	pkTypes, indexColTypes []*types.FieldType,
) ([]*recordWithChecksum, error) {
	rows, err := queryToRow(ctx, se, sql)
	if err != nil {
		return nil, err
	}

	errCtx := se.GetSessionVars().StmtCtx.ErrCtx()
	records := make([]*recordWithChecksum, 0, len(rows))
	for _, row := range rows {
		r := &recordWithChecksum{
			RecordData: &consistency.RecordData{},
		}
		if isCommonHandle {
			handleDatum := make([]types.Datum, 0, len(pkTypes))
			for i, t := range pkTypes {
				handleDatum = append(handleDatum, row.GetDatum(i, t))
			}

			handleBytes, err := codec.EncodeKey(se.GetSessionVars().StmtCtx.TimeZone(), nil, handleDatum...)
			if err = errCtx.HandleError(err); err != nil {
				return nil, err
			}
			if r.Handle, err = kv.NewCommonHandle(handleBytes); err != nil {
				return nil, err
			}
		} else {
			r.Handle = kv.IntHandle(row.GetInt64(0))
		}

		valueDatum := make([]types.Datum, 0, len(indexColTypes))
		for i, tp := range indexColTypes {
			valueDatum = append(valueDatum, row.GetDatum(i+len(pkTypes), tp))
		}

		r.Values = valueDatum
		r.checksum = row.GetUint64(len(pkTypes) + len(indexColTypes))
		records = append(records, r)
	}

	return records, nil
}

// TableName returns `schema`.`table`
func TableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

// ColumnName returns `column`
func ColumnName(column string) string {
	return fmt.Sprintf("`%s`", escapeName(column))
}

func escapeName(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

func buildAlias(colName ast.CIStr, suffix string) string {
	return ColumnName(fmt.Sprintf("%s%s%s", aliasPrefix, colName.O, suffix))
}

func crc32FromCols(cols []string) string {
	return fmt.Sprintf("crc32(md5(concat_ws(0x2, %s)))", strings.Join(cols, ","))
}
