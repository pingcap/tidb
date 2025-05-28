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
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
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
	is         infoschema.InfoSchema
	exitCh     chan struct{}
	retCh      chan error
	checkIndex bool
}

var _ exec.Executor = &CheckTableExec{}

const (
	// mvIndexCRC32Sum is the aggregated column from subquery for MVIndex check.
	mvIndexCRC32Sum = "_crc32_sum_column"

	// mvJSONColumn is the json array to output in error message.
	// For table scan, it's read directly from the table,
	// for index scan, it's concated from all the values of the same handle.
	mvJSONColumn = "_generated_json_column"

	maxPartitionTimes = 10
)

var (
	// LookupCheckThreshold is the threshold to do check, exported for test
	LookupCheckThreshold = int64(100)

	// CheckTableFastBucketSize is the bucket size of fast check table, exported for test
	CheckTableFastBucketSize atomic.Int64
)

func init() {
	CheckTableFastBucketSize.Store(1024)
}

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
	is         infoschema.InfoSchema
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

	workerPool := workerpool.NewWorkerPool[checkIndexTask]("checkIndex",
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
) (string, int, []*types.FieldType) {
	var pkCols []string
	var pkTypes []*types.FieldType
	switch {
	case tblMeta.IsCommonHandle:
		pkColsInfo := tblMeta.GetPrimaryKey().Columns
		for _, colInfo := range pkColsInfo {
			pkCols = append(pkCols, ColumnName(colInfo.Name.O))
			pkTypes = append(pkTypes, &tblMeta.Columns[colInfo.Offset].FieldType)
		}
	case tblMeta.PKIsHandle:
		pkCols = append(pkCols, ColumnName(tblMeta.GetPkName().O))
	default: // support decoding _tidb_rowid.
		pkCols = append(pkCols, ColumnName(model.ExtraHandleName.O))
	}

	return strings.Join(pkCols, ","), len(pkCols), pkTypes
}

type checkIndexWorker struct {
	sctx       sessionctx.Context
	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	e          *FastCheckTableExec
}

func (w *checkIndexWorker) initSessCtx(se sessionctx.Context) (restore func()) {
	sessVars := se.GetSessionVars()
	originOptUseInvisibleIdx := sessVars.OptimizerUseInvisibleIndexes
	originMemQuotaQuery := sessVars.MemQuotaQuery

	sessVars.OptimizerUseInvisibleIndexes = true
	sessVars.MemQuotaQuery = w.sctx.GetSessionVars().MemQuotaQuery
	return func() {
		sessVars.OptimizerUseInvisibleIndexes = originOptUseInvisibleIdx
		sessVars.MemQuotaQuery = originMemQuotaQuery
	}
}

// HandleTask implements the Worker interface.
func (w *checkIndexWorker) HandleTask(task checkIndexTask, _ func(workerpool.None)) {
	defer w.e.wg.Done()
	idxInfo := w.indexInfos[task.indexOffset]
	bucketSize := int(CheckTableFastBucketSize.Load())

	ctx := kv.WithInternalSourceType(w.e.contextCtx, kv.InternalTxnAdmin)

	trySaveErr := func(err error) {
		w.e.err.CompareAndSwap(nil, &err)
	}

	se, err := w.e.BaseExecutor.GetSysSession()
	if err != nil {
		trySaveErr(err)
		return
	}
	restoreCtx := w.initSessCtx(se)
	defer func() {
		restoreCtx()
		w.e.BaseExecutor.ReleaseSysSession(ctx, se)
	}()

	tblMeta := w.table.Meta()
	tblName := TableName(w.e.dbName, tblMeta.Name.String())
	idxName := ColumnName(idxInfo.Name.String())
	handleCols, numPkCols, pkTypes := extractHandleColumnsAndType(tblMeta)

	var (
		// extractExpr is the expression to create MV Index, e.g., j->'$.path'
		extractExpr string

		// generatedExpr is string like "cast(extractExpr as array))"
		generatedExpr string

		// jsonSumExpr is string like "json_sum_crc32(extractExpr as array)"
		jsonSumExpr string

		indexCols  string
		useMVIndex = idxInfo.MVIndex
	)

	if useMVIndex {
		tblCol := tblMeta.Columns[idxInfo.Columns[0].Offset]
		indexCols = mvIndexCRC32Sum

		generatedExpr = strings.ToLower(tblCol.GeneratedExprString)
		jsonSumExpr = strings.Replace(generatedExpr, "cast", "json_sum_crc32", 1)
		start := strings.Index(generatedExpr, "cast(")
		end := strings.LastIndex(generatedExpr, "as")
		extractExpr = generatedExpr[start+5 : end]
	} else {
		indexColNames := make([]string, len(idxInfo.Columns))
		for i, col := range idxInfo.Columns {
			tblCol := tblMeta.Columns[col.Offset]
			if tblCol.IsVirtualGenerated() && tblCol.Hidden {
				indexColNames[i] = tblCol.GeneratedExprString
			} else {
				indexColNames[i] = ColumnName(col.Name.O)
			}
		}
		indexCols = strings.Join(indexColNames, ",")
	}

	// CheckSum of (handle + index columns).
	md5HandleAndIndexCol := fmt.Sprintf("crc32(md5(concat_ws(0x2, %s, %s)))", handleCols, indexCols)

	// Checksum of handle, used for group by and order by.
	md5Handle := fmt.Sprintf("crc32(md5(concat_ws(0x2, %s)))", handleCols)

	var (
		tableRowCntToCheck = int64(0)
		offset             = 0
		mod                = 1

		meetError = false
		checkOnce = false
	)

	if w.e.Ctx().GetSessionVars().SnapshotTS != 0 {
		se.GetSessionVars().SnapshotTS = w.e.Ctx().GetSessionVars().SnapshotTS
		defer func() {
			se.GetSessionVars().SnapshotTS = 0
		}()
	}
	_, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin")
	if err != nil {
		return
	}

	fromTable := fmt.Sprintf("%s use index()", tblName)
	fromIndex := fmt.Sprintf("%s use index(%s)", tblName, idxName)
	if useMVIndex {
		// For mv index, we will use subquery to gather all values with the same handle.
		// The query will be like:
		//     table side: select handle, JSON_SUM_CRC32(xxx as array) from t use index()
		//     index side: select /*+ force_index(`idx`) */ handle, SUM(CRC32(CAST(xxx as array))) from t group by handle
		fromTable = fmt.Sprintf("(select %s, %s as %s from %s use index()) tmp",
			handleCols, jsonSumExpr, indexCols, tblName)
		fromIndex = fmt.Sprintf("(select /*+ force_index(%s, %s) */ %s, SUM(CRC32(%s)) as %s from %s group by %s) tmp",
			tblName, idxName, handleCols, generatedExpr, indexCols, tblName, handleCols)
	}

	indexCtx := plannercore.WithEnableMVIndexScan(w.e.contextCtx)
	tableCtx := w.e.contextCtx

	times := 0
	for tableRowCntToCheck > LookupCheckThreshold || !checkOnce {
		times++
		if times == maxPartitionTimes {
			logutil.BgLogger().Warn("compare checksum by group reaches time limit", zap.Int("times", times))
			break
		}
		whereKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle, offset, mod)
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) div %d %% %d)", md5Handle, offset, mod, bucketSize)
		if !checkOnce {
			whereKey = "0"
		}
		checkOnce = true

		tblQuery := fmt.Sprintf(
			"select /*+ read_from_storage(tikv[%s]) */ bit_xor(%s), %s, count(*) from %s where %s = 0 group by %s",
			tblName, md5HandleAndIndexCol, groupByKey, fromTable, whereKey, groupByKey)
		idxQuery := fmt.Sprintf(
			"select bit_xor(%s), %s, count(*) from %s where %s = 0 group by %s",
			md5HandleAndIndexCol, groupByKey, fromIndex, whereKey, groupByKey)

		logutil.BgLogger().Info(
			"fast check table by group",
			zap.String("table name", tblMeta.Name.String()),
			zap.String("index name", idxInfo.Name.String()),
			zap.Int("times", times),
			zap.Int("current offset", offset), zap.Int("current mod", mod),
			zap.String("table sql", tblQuery), zap.String("index sql", idxQuery),
		)

		// compute table side checksum.
		tableChecksum, err := getCheckSum(tableCtx, se, tblQuery)
		if err != nil {
			trySaveErr(err)
			return
		}
		slices.SortFunc(tableChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

		// compute index side checksum.
		indexChecksum, err := getCheckSum(indexCtx, se, idxQuery)
		if err != nil {
			trySaveErr(err)
			return
		}
		slices.SortFunc(indexChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

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
			} else if times != 1 {
				// Both sides have same buckets, which means no error found.
				// But error is detected in previous round, it's unexpected.
				logutil.BgLogger().Error(
					"unexpected result, no error detected in this round, but an error is detected in the previous round",
					zap.Int("times", times), zap.Int("offset", offset), zap.Int("mod", mod))
			}
		}

		offset += currentOffset * mod
		mod *= bucketSize
	}

	queryToRow := func(qCtx context.Context, se sessionctx.Context, sql string) ([]chunk.Row, error) {
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

	if meetError {
		if useMVIndex {
			// If meet error, output original json in the subquery as well.
			fromTable = fmt.Sprintf("(select %s, %s as %s, %s as %s from %s use index()) tmp",
				handleCols, extractExpr, mvJSONColumn, jsonSumExpr, indexCols, tblName)
			fromIndex = fmt.Sprintf("(select /*+ force_index(%[1]s, %[2]s) */ %[3]s, JSON_ARRAYAGG(%[4]s) as %[5]s, SUM(CRC32(%[4]s)) as %[6]s from %[1]s group by %[3]s) tmp",
				tblName, idxName, handleCols, generatedExpr, mvJSONColumn, indexCols)
			indexCols = mvJSONColumn
		}

		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle, offset, mod)
		indexSQL := fmt.Sprintf(
			"select %s, %s, %s from %s where %s = 0 order by %s",
			handleCols, indexCols, md5HandleAndIndexCol, fromIndex, groupByKey, handleCols)
		tableSQL := fmt.Sprintf(
			"select /*+ read_from_storage(tikv[%s]) */ %s, %s, %s from %s where %s = 0 order by %s",
			tblName, handleCols, indexCols, md5HandleAndIndexCol, fromTable, groupByKey, handleCols)

		idxRow, err := queryToRow(indexCtx, se, indexSQL)
		if err != nil {
			trySaveErr(err)
			return
		}
		tblRow, err := queryToRow(tableCtx, se, tableSQL)
		if err != nil {
			trySaveErr(err)
			return
		}

		errCtx := w.sctx.GetSessionVars().StmtCtx.ErrCtx()
		getRecordFromRow := func(row chunk.Row) (*consistency.RecordData, error) {
			r := &consistency.RecordData{}
			handleDatum := make([]types.Datum, 0)
			for i, t := range pkTypes {
				handleDatum = append(handleDatum, row.GetDatum(i, t))
			}
			if w.table.Meta().IsCommonHandle {
				handleBytes, err := codec.EncodeKey(w.sctx.GetSessionVars().StmtCtx.TimeZone(), nil, handleDatum...)
				err = errCtx.HandleError(err)
				if err != nil {
					return nil, err
				}
				if r.Handle, err = kv.NewCommonHandle(handleBytes); err != nil {
					return nil, err
				}
			}
			r.Handle = kv.IntHandle(row.GetInt64(0))

			valueDatum := make([]types.Datum, 0)
			if useMVIndex {
				valueDatum = append(valueDatum, row.GetDatum(numPkCols, types.NewFieldType(mysql.TypeJSON)))
			} else {
				for i, t := range idxInfo.Columns {
					valueDatum = append(valueDatum, row.GetDatum(i+numPkCols, &tblMeta.Columns[t.Offset].FieldType))
				}
			}

			r.Values = valueDatum
			return r, nil
		}

		ir := func() *consistency.Reporter {
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
				Tbl:             tblMeta,
				Idx:             idxInfo,
				EnableRedactLog: w.sctx.GetSessionVars().EnableRedactLog,
				Storage:         w.sctx.GetStore(),
			}
		}

		getCheckSum := func(row chunk.Row) uint64 {
			return row.GetUint64(numPkCols + len(idxInfo.Columns))
		}

		var (
			tableRecord     *consistency.RecordData
			indexRecord     *consistency.RecordData
			lastTableRecord *consistency.RecordData
		)
		for i := range min(len(tblRow), len(idxRow)) {
			if tableRecord, err = getRecordFromRow(tblRow[i]); err != nil {
				trySaveErr(err)
				return
			}

			if indexRecord, err = getRecordFromRow(idxRow[i]); err != nil {
				trySaveErr(err)
				return
			}

			handleCmp := tableRecord.Handle.Compare(indexRecord.Handle)
			if handleCmp == 0 {
				if getCheckSum(tblRow[i]) != getCheckSum(idxRow[i]) {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, tableRecord)
					trySaveErr(err)
					return
				}
				lastTableRecord = tableRecord
			} else if handleCmp > 0 {
				if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, lastTableRecord)
				} else {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, nil)
				}
				trySaveErr(err)
				return
			} else {
				err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, nil, tableRecord)
				trySaveErr(err)
				return
			}
		}

		if len(idxRow) < len(tblRow) {
			// Table side has more rows
			if tableRecord, err = getRecordFromRow(tblRow[len(idxRow)]); err == nil {
				err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, nil, tableRecord)
			}
		} else if len(tblRow) < len(idxRow) {
			// Index side has more rows
			if indexRecord, err = getRecordFromRow(idxRow[len(tblRow)]); err == nil {
				if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, lastTableRecord)
				} else {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, nil)
				}
			}
		} else {
			// Both sides have same rows, but no error detected, this shouldn't happen.
			err = errors.Errorf("no error detected during row comparision, but an error is detected in the previous round")
		}
		trySaveErr(err)
	}
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

func getCheckSum(ctx context.Context, se sessionctx.Context, sql string) ([]groupByChecksum, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer func(rs sqlexec.RecordSet) {
		err := rs.Close()
		if err != nil {
			logutil.BgLogger().Error("close record set failed", zap.Error(err))
		}
	}(rs)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 256)
	if err != nil {
		return nil, err
	}
	checksums := make([]groupByChecksum, 0, len(rows))
	for _, row := range rows {
		checksums = append(checksums, groupByChecksum{bucket: row.GetUint64(1), checksum: row.GetUint64(0), count: row.GetInt64(2)})
	}
	return checksums, nil
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
