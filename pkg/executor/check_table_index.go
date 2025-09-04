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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
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

	// CheckTableFastBucketSize is the bucket size of fast check table, exported for test
	CheckTableFastBucketSize = 1024

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
		idx := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		return admin.CheckRecordAndIndex(ctx, e.Ctx(), txn, e.table, idx)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx := tables.NewIndex(def.ID, e.table.Meta(), idxInfo)
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
	err        *atomic.Pointer[error]
	wg         sync.WaitGroup
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

	workerPool := workerpool.NewWorkerPool("checkIndex",
		poolutil.CheckTable, 3, e.createWorker)
	workerPool.Start(ctx)

	e.wg.Add(len(e.indexInfos))
	for i := range e.indexInfos {
		workerPool.AddTask(checkIndexTask{indexOffset: i, err: e.err})
	}

	e.wg.Wait()
	workerPool.ReleaseAndWait()

	p := e.err.Load()
	if p == nil {
		return nil
	}
	return *p
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

/*
buildChecksumSQLForMVIndex builds the checksum SQL for MVIndex.
The basic rationale of the checksum SQL (ignore non-int handle processing and overflow handling):

	Table side:
		SELECT SUM(handle * JSON_SUM_CRC32(array_col)), handle MOD 1024, COUNT(*)
			FROM table
			WHERE (JSON_LENGTH(array_col) > 0 OR JSON_LENGTH(array_col) IS NULL)
			GROUP BY handle MOD 1024
	Index side:
		SELECT SUM(handle * array_hidden_col), handle MOD 1024, COUNT(*)
			FROM table
			GROUP BY handle MOD 1024",
*/
func buildChecksumSQLForMVIndex(
	tblName string, handleCols []string,
	idxInfo *model.IndexInfo, tblMeta *model.TableInfo,
) (md5Handle, tableChecksumSQL, indexChecksumSQL string) {
	idxName := ColumnName(idxInfo.Name.String())

	var (
		tableArrayCols string
		indexArrayCols string
		tableFilterCol string
		crc32Cols      = handleCols
	)

	for _, col := range idxInfo.Columns {
		tblCol := tblMeta.Columns[col.Offset]
		rawExpr := ExtractCastArrayExpr(tblCol)
		generatedExpr := strings.ToLower(tblCol.GeneratedExprString)

		colName := ColumnName(col.Name.O)
		if len(rawExpr) > 0 {
			// JSON with zero length array (like "[]") doesn't correspond to any index entry,
			// but null JSON will create a null index entry. So we need to do some filtering.
			tableFilterCol = fmt.Sprintf("(JSON_LENGTH(%s) > 0 or JSON_LENGTH(%s) is null)", rawExpr, rawExpr)
			tableArrayCols = strings.Replace(generatedExpr, "cast", "JSON_SUM_CRC32", 1)
			indexArrayCols = fmt.Sprintf("CRC32(%s)", colName)
		} else if col.Length == types.UnspecifiedLength {
			crc32Cols = append(crc32Cols, colName)
		} else {
			crc32Cols = append(crc32Cols, fmt.Sprintf("CAST(%s AS CHAR(%d))", ColumnName(col.Name.O), col.Length))
		}
	}

	md5Handle = crc32FromCols(crc32Cols)

	tableChecksumSQL = fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ CAST(SUM((%s MOD %d) * %s) AS SIGNED), %s, COUNT(*) FROM %s USE INDEX() WHERE %s AND %s = 0 GROUP BY %s",
		tblName, md5Handle, expression.JSONCRC32Mod, tableArrayCols, "%s",
		tblName, tableFilterCol, "%s", "%s",
	)
	indexChecksumSQL = fmt.Sprintf(
		"SELECT CAST(SUM((%s MOD %d) * (%s MOD %d)) AS SIGNED), %s, COUNT(*) FROM %s USE INDEX(%s) WHERE %s = 0 GROUP BY %s",
		md5Handle, expression.JSONCRC32Mod, indexArrayCols, expression.JSONCRC32Mod,
		"%s", tblName, idxName, "%s", "%s",
	)
	return
}

/*
buildCheckRowSQLForMVIndex builds the SQL to check rows for MVIndex.
The basic rationale of the check row SQL (ignore non-int handle processing and overflow handling):

	Table side:
		SELECT handle,
			array_col,
			handle * JSON_SUM_CRC32(array_col),
		from   table
		where  (
					json_length(array_col) > 0
			OR     json_length(array_col) IS NULL)
		AND    handle MOD 1024 = 1
	Index side:
		SELECT Min(handle),
			Json_arrayagg(hidden_col),
			Sum(handle * crc)
		FROM   (SELECT handle,
					hidden_col,
					Crc32(hidden_col) AS crc
				FROM   table
				WHERE  handle MOD 1024 = 1)
		GROUP  BY handle
*/
func buildCheckRowSQLForMVIndex(
	tblName string, handleCols []string,
	idxInfo *model.IndexInfo, tblMeta *model.TableInfo,
) (tableCheckSQL, indexCheckSQL string) {
	idxName := ColumnName(idxInfo.Name.String())

	var (
		tableFilterCol  string
		tableSelectCols = handleCols
		tableHandleCols = handleCols
		tableArrayCol   string

		indexSelectCols   = handleCols
		indexSubqueryCols = handleCols
		indexHandleCols   = handleCols
		indexArrayCol     string
	)

	for _, col := range idxInfo.Columns {
		tblCol := tblMeta.Columns[col.Offset]
		generatedExpr := strings.ToLower(tblCol.GeneratedExprString)
		rawExpr := ExtractCastArrayExpr(tblCol)

		alias := buildAlias(col.Name, "")
		colName := ColumnName(col.Name.O)
		if len(rawExpr) > 0 {
			tableFilterCol = fmt.Sprintf("(JSON_LENGTH(%s) > 0 or JSON_LENGTH(%s) is null)", rawExpr, rawExpr)
			tableArrayCol = strings.Replace(generatedExpr, "cast", "JSON_SUM_CRC32", 1)
			tableSelectCols = append(tableSelectCols, rawExpr)

			indexSubqueryCols = append(indexSubqueryCols, fmt.Sprintf("CRC32(%s) as %s", colName, alias))
			indexArrayCol = fmt.Sprintf("CAST(SUM(%s MOD %d) AS SIGNED)", alias, expression.JSONCRC32Mod)

			alias = buildAlias(col.Name, "_array")
			arrayExpr := strings.Replace(generatedExpr, "array", "", 1)
			arrayExpr = strings.Replace(arrayExpr, rawExpr, colName, 1)
			arrayExpr = fmt.Sprintf("(%s) as %s", arrayExpr, alias)

			indexSubqueryCols = append(indexSubqueryCols, arrayExpr)
			indexSelectCols = append(indexSelectCols, fmt.Sprintf("JSON_ARRAYAGG(%s)", alias))
		} else {
			// For prefix column
			if col.Length != types.UnspecifiedLength {
				colName = fmt.Sprintf("CAST(%s AS CHAR(%d))", ColumnName(col.Name.O), col.Length)
			}

			tableSelectCols = append(tableSelectCols, colName)
			tableHandleCols = append(tableHandleCols, colName)

			indexSubqueryCols = append(indexSubqueryCols, fmt.Sprintf("%s AS %s", colName, alias))
			indexSelectCols = append(indexSelectCols, fmt.Sprintf("MIN(%s)", alias))
			indexHandleCols = append(indexHandleCols, fmt.Sprintf("MIN(%s)", alias))
		}
	}

	tableChecksum := fmt.Sprintf("%s * %s", crc32FromCols(tableHandleCols), tableArrayCol)
	indexChecksum := fmt.Sprintf("%s * %s", crc32FromCols(indexHandleCols), indexArrayCol)
	handleColsStr := strings.Join(handleCols, ", ")

	tableCheckSQL = fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ %s, %s FROM %s USE INDEX() WHERE %s AND %s = 0 ORDER BY %s",
		tblName, strings.Join(tableSelectCols, ", "), tableChecksum,
		tblName, tableFilterCol, "%s", handleColsStr,
	)
	indexCheckSQL = fmt.Sprintf(
		"SELECT %s, %s FROM (select %s FROM %s USE INDEX(%s) WHERE %s = 0) tmp GROUP BY %s ORDER BY %s",
		strings.Join(indexSelectCols, ", "), indexChecksum,
		strings.Join(indexSubqueryCols, ", "), tblName,
		idxName, "%s", handleColsStr, handleColsStr,
	)
	return
}

func buildChecksumSQLForNormalIndex(
	tblName string, handleCols []string,
	idxInfo *model.IndexInfo, tblMeta *model.TableInfo,
) (tableChecksumSQL, indexChecksumSQL string) {
	checksumCols := handleCols
	for _, col := range idxInfo.Columns {
		tblCol := tblMeta.Columns[col.Offset]
		if col.Length == types.UnspecifiedLength {
			checksumCols = append(checksumCols, ColumnName(tblCol.Name.O))
		} else {
			checksumCols = append(checksumCols, fmt.Sprintf("CAST(%s AS CHAR(%d))", ColumnName(col.Name.O), col.Length))
		}
	}

	md5HandleAndIndexCol := crc32FromCols(checksumCols)

	tableChecksumSQL = fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX() WHERE %s = 0 GROUP BY %s",
		md5HandleAndIndexCol, "%s", tblName, "%s", "%s",
	)
	indexChecksumSQL = fmt.Sprintf(
		"SELECT BIT_XOR(%s), %s, COUNT(*) FROM %s USE INDEX(%s) WHERE %s = 0 GROUP BY %s",
		md5HandleAndIndexCol, "%s", tblName, ColumnName(idxInfo.Name.String()), "%s", "%s",
	)
	return
}

func buildCheckRowSQLForNormalIndex(
	tblName string, handleCols []string,
	idxInfo *model.IndexInfo, tblMeta *model.TableInfo,
) (tableCheckSQL, indexCheckSQL string) {
	idxName := ColumnName(idxInfo.Name.String())

	checksumCols := handleCols
	for _, col := range idxInfo.Columns {
		tblCol := tblMeta.Columns[col.Offset]
		if col.Length == types.UnspecifiedLength {
			checksumCols = append(checksumCols, ColumnName(tblCol.Name.O))
		} else {
			checksumCols = append(checksumCols, fmt.Sprintf("CAST(%s AS CHAR(%d))", ColumnName(col.Name.O), col.Length))
		}
	}

	md5Handle := strings.Join(handleCols, ", ")
	checksumColsStr := strings.Join(checksumCols, ", ")
	md5HandleAndIndexCol := fmt.Sprintf("CRC32(MD5(CONCAT_WS(0x2, %s)))", checksumColsStr)

	tableCheckSQL = fmt.Sprintf(
		"SELECT /*+ read_from_storage(tikv[%s]) */ %s, %s FROM %s USE INDEX() WHERE %s = 0 ORDER BY %s",
		tblName, checksumColsStr, md5HandleAndIndexCol, tblName, "%s", md5Handle)
	indexCheckSQL = fmt.Sprintf(
		"SELECT %s, %s FROM %s USE INDEX(%s) WHERE %s = 0 ORDER BY %s",
		checksumColsStr, md5HandleAndIndexCol, tblName, idxName, "%s", md5Handle)
	return
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

func (w *checkIndexWorker) HandleTask(task checkIndexTask, _ func(workerpool.None)) {
	if err := w.handleTask(task); err != nil {
		w.e.err.CompareAndSwap(nil, &err)
	}
}

// HandleTask implements the Worker interface.
func (w *checkIndexWorker) handleTask(task checkIndexTask) error {
	defer w.e.wg.Done()

	ctx := kv.WithInternalSourceType(w.e.contextCtx, kv.InternalTxnAdmin)
	se, restoreCtx, err := w.initSessCtx()
	if err != nil {
		return err
	}
	defer restoreCtx()

	var (
		tableChecksumSQL string
		indexChecksumSQL string
		tableCheckSQL    string
		indexCheckSQL    string
		md5Handle        string

		tableQuery string
		indexQuery string
	)

	idxInfo := w.indexInfos[task.indexOffset]
	tblInfo := w.table.Meta()
	tblName := TableName(w.e.dbName, tblInfo.Name.String())
	handleCols, pkTypes := extractHandleColumnsAndType(tblInfo)

	indexColTypes := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for _, t := range idxInfo.Columns {
		tblCol := tblInfo.Columns[t.Offset]
		if len(ExtractCastArrayExpr(tblCol)) > 0 {
			indexColTypes = append(indexColTypes, types.NewFieldType(mysql.TypeJSON))
		} else {
			indexColTypes = append(indexColTypes, &tblCol.FieldType)
		}
	}

	if idxInfo.MVIndex {
		// We will add non-array columns of the MV Index into handle too, to simplify the SQL.
		md5Handle, tableChecksumSQL, indexChecksumSQL = buildChecksumSQLForMVIndex(tblName, handleCols, idxInfo, tblInfo)
		tableCheckSQL, indexCheckSQL = buildCheckRowSQLForMVIndex(tblName, handleCols, idxInfo, tblInfo)
	} else {
		md5Handle = crc32FromCols(handleCols)
		tableChecksumSQL, indexChecksumSQL = buildChecksumSQLForNormalIndex(tblName, handleCols, idxInfo, tblInfo)
		tableCheckSQL, indexCheckSQL = buildCheckRowSQLForNormalIndex(tblName, handleCols, idxInfo, tblInfo)
	}

	if _, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin"); err != nil {
		return err
	}

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
		groupByKey := fmt.Sprintf("((CAST(%s AS SIGNED) - %d) DIV %d MOD %d)", md5Handle, offset, mod, CheckTableFastBucketSize)
		if checkTimes == 1 {
			whereKey = "0"
		}

		tableQuery = fmt.Sprintf(tableChecksumSQL, groupByKey, whereKey, groupByKey)
		indexQuery = fmt.Sprintf(indexChecksumSQL, groupByKey, whereKey, groupByKey)
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

		// TODO(joechenrh): remove me after testing.
		// meetError = true
		// currentOffset = int(tableChecksum[0].bucket)
		offset += currentOffset * mod
		mod *= CheckTableFastBucketSize
	}

	failpoint.Inject("mockMeetErrorInCheckTable", func(val failpoint.Value) {
		// Only set meetError when no error detected.
		if v, ok := val.(bool); ok && !meetError {
			meetError = v
		}
	})

	if meetError {
		groupByKey := fmt.Sprintf("((CAST(%s AS SIGNED) - %d) MOD %d)", md5Handle, offset, mod)
		tableQuery = fmt.Sprintf(tableCheckSQL, groupByKey)
		indexQuery = fmt.Sprintf(indexCheckSQL, groupByKey)
		verifyIndexSideQuery(ctx, se, indexQuery)

		var (
			lastTableRecord *recordWithChecksum
			tblRecords      []*recordWithChecksum
			idxRecords      []*recordWithChecksum
		)

		if idxRecords, err = getRecordWithChecksum(ctx, se, indexQuery, tblInfo.IsCommonHandle, pkTypes, indexColTypes); err != nil {
			return err
		}

		if tblRecords, err = getRecordWithChecksum(ctx, se, tableQuery, tblInfo.IsCommonHandle, pkTypes, indexColTypes); err != nil {
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
func (*checkIndexWorker) Close() {}

type checkIndexTask struct {
	indexOffset int
	err         *atomic.Pointer[error]
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (c checkIndexTask) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return "fast_check_table", "RecoverArgs", func() {
		err := errors.Errorf("checkIndexTask panicked, indexOffset: %d", c.indexOffset)
		c.err.CompareAndSwap(nil, &err)
	}, false
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
	row, err := sqlexec.DrainRecordSet(qCtx, rs, 4096)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		logutil.BgLogger().Warn("close result set failed", zap.Error(err))
	}
	return row, nil
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
