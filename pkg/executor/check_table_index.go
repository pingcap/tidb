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
		if idx.MVIndex || idx.VectorInfo != nil {
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
	for i := 0; i < concurrency; i++ {
		wg.Run(func() {
			util.WithRecovery(func() {
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
		})
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

	var pkCols []string
	var pkTypes []*types.FieldType
	switch {
	case w.e.table.Meta().IsCommonHandle:
		pkColsInfo := w.e.table.Meta().GetPrimaryKey().Columns
		for _, colInfo := range pkColsInfo {
			colStr := colInfo.Name.O
			pkCols = append(pkCols, colStr)
			pkTypes = append(pkTypes, &w.e.table.Meta().Columns[colInfo.Offset].FieldType)
		}
	case w.e.table.Meta().PKIsHandle:
		pkCols = append(pkCols, w.e.table.Meta().GetPkName().O)
	default: // support decoding _tidb_rowid.
		pkCols = append(pkCols, model.ExtraHandleName.O)
	}

	// CheckSum of (handle + index columns).
	var md5HandleAndIndexCol strings.Builder
	md5HandleAndIndexCol.WriteString("crc32(md5(concat_ws(0x2, ")
	for _, col := range pkCols {
		md5HandleAndIndexCol.WriteString(ColumnName(col))
		md5HandleAndIndexCol.WriteString(", ")
	}
	for offset, col := range idxInfo.Columns {
		tblCol := w.table.Meta().Columns[col.Offset]
		if tblCol.IsGenerated() && !tblCol.GeneratedStored {
			md5HandleAndIndexCol.WriteString(tblCol.GeneratedExprString)
		} else {
			md5HandleAndIndexCol.WriteString(ColumnName(col.Name.O))
		}
		if offset != len(idxInfo.Columns)-1 {
			md5HandleAndIndexCol.WriteString(", ")
		}
	}
	md5HandleAndIndexCol.WriteString(")))")

	// Used to group by and order.
	var md5Handle strings.Builder
	md5Handle.WriteString("crc32(md5(concat_ws(0x2, ")
	for i, col := range pkCols {
		md5Handle.WriteString(ColumnName(col))
		if i != len(pkCols)-1 {
			md5Handle.WriteString(", ")
		}
	}
	md5Handle.WriteString(")))")

	handleColumnField := strings.Join(pkCols, ", ")
	var indexColumnField strings.Builder
	for offset, col := range idxInfo.Columns {
		indexColumnField.WriteString(ColumnName(col.Name.O))
		if offset != len(idxInfo.Columns)-1 {
			indexColumnField.WriteString(", ")
		}
	}

	tableRowCntToCheck := int64(0)

	offset := 0
	mod := 1
	meetError := false

	lookupCheckThreshold := int64(100)
	checkOnce := false

	if w.e.Ctx().GetSessionVars().SnapshotTS != 0 {
		se.GetSessionVars().SnapshotTS = w.e.Ctx().GetSessionVars().SnapshotTS
		defer func() {
			se.GetSessionVars().SnapshotTS = 0
		}()
	}
	_, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin")
	if err != nil {
		trySaveErr(err)
		return
	}

	times := 0
	const maxTimes = 10
	for tableRowCntToCheck > lookupCheckThreshold || !checkOnce {
		times++
		if times == maxTimes {
			logutil.BgLogger().Warn("compare checksum by group reaches time limit", zap.Int("times", times))
			break
		}
		whereKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle.String(), offset, mod)
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) div %d %% %d)", md5Handle.String(), offset, mod, bucketSize)
		if !checkOnce {
			whereKey = "0"
		}
		checkOnce = true

		tblQuery := fmt.Sprintf("select /*+ read_from_storage(tikv[%s]) */ bit_xor(%s), %s, count(*) from %s use index() where %s = 0 group by %s", TableName(w.e.dbName, w.e.table.Meta().Name.String()), md5HandleAndIndexCol.String(), groupByKey, TableName(w.e.dbName, w.e.table.Meta().Name.String()), whereKey, groupByKey)
		idxQuery := fmt.Sprintf("select bit_xor(%s), %s, count(*) from %s use index(`%s`) where %s = 0 group by %s", md5HandleAndIndexCol.String(), groupByKey, TableName(w.e.dbName, w.e.table.Meta().Name.String()), idxInfo.Name, whereKey, groupByKey)

		logutil.BgLogger().Info("fast check table by group", zap.String("table name", w.table.Meta().Name.String()), zap.String("index name", idxInfo.Name.String()), zap.Int("times", times), zap.Int("current offset", offset), zap.Int("current mod", mod), zap.String("table sql", tblQuery), zap.String("index sql", idxQuery))

		// compute table side checksum.
		tableChecksum, err := getCheckSum(w.e.contextCtx, se, tblQuery)
		if err != nil {
			trySaveErr(err)
			return
		}
		slices.SortFunc(tableChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

		// compute index side checksum.
		indexChecksum, err := getCheckSum(w.e.contextCtx, se, idxQuery)
		if err != nil {
			trySaveErr(err)
			return
		}
		slices.SortFunc(indexChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

		currentOffset := 0

		// Every checksum in table side should be the same as the index side.
		i := 0
		for i < len(tableChecksum) && i < len(indexChecksum) {
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
			i++
		}

		if !meetError && i < len(indexChecksum) && i == len(tableChecksum) {
			// Table side has fewer buckets.
			currentOffset = int(indexChecksum[i].bucket)
			tableRowCntToCheck = indexChecksum[i].count
			meetError = true
		} else if !meetError && i < len(tableChecksum) && i == len(indexChecksum) {
			// Index side has fewer buckets.
			currentOffset = int(tableChecksum[i].bucket)
			tableRowCntToCheck = tableChecksum[i].count
			meetError = true
		}

		if !meetError {
			if times != 1 {
				logutil.BgLogger().Error("unexpected result, no error detected in this round, but an error is detected in the previous round", zap.Int("times", times), zap.Int("offset", offset), zap.Int("mod", mod))
			}
			break
		}

		offset += currentOffset * mod
		mod *= bucketSize
	}

	queryToRow := func(se sessionctx.Context, sql string) ([]chunk.Row, error) {
		rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql)
		if err != nil {
			return nil, err
		}
		row, err := sqlexec.DrainRecordSet(ctx, rs, 4096)
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
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle.String(), offset, mod)
		indexSQL := fmt.Sprintf("select %s, %s, %s from %s use index(`%s`) where %s = 0 order by %s", handleColumnField, indexColumnField.String(), md5HandleAndIndexCol.String(), TableName(w.e.dbName, w.e.table.Meta().Name.String()), idxInfo.Name, groupByKey, handleColumnField)
		tableSQL := fmt.Sprintf("select /*+ read_from_storage(tikv[%s]) */ %s, %s, %s from %s use index() where %s = 0 order by %s", TableName(w.e.dbName, w.e.table.Meta().Name.String()), handleColumnField, indexColumnField.String(), md5HandleAndIndexCol.String(), TableName(w.e.dbName, w.e.table.Meta().Name.String()), groupByKey, handleColumnField)

		idxRow, err := queryToRow(se, indexSQL)
		if err != nil {
			trySaveErr(err)
			return
		}
		tblRow, err := queryToRow(se, tableSQL)
		if err != nil {
			trySaveErr(err)
			return
		}

		errCtx := w.sctx.GetSessionVars().StmtCtx.ErrCtx()
		getHandleFromRow := func(row chunk.Row) (kv.Handle, error) {
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
				return kv.NewCommonHandle(handleBytes)
			}
			return kv.IntHandle(row.GetInt64(0)), nil
		}
		getValueFromRow := func(row chunk.Row) ([]types.Datum, error) {
			valueDatum := make([]types.Datum, 0)
			for i, t := range idxInfo.Columns {
				valueDatum = append(valueDatum, row.GetDatum(i+len(pkCols), &w.table.Meta().Columns[t.Offset].FieldType))
			}
			return valueDatum, nil
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
				Tbl:             w.table.Meta(),
				Idx:             idxInfo,
				EnableRedactLog: w.sctx.GetSessionVars().EnableRedactLog,
				Storage:         w.sctx.GetStore(),
			}
		}

		getCheckSum := func(row chunk.Row) uint64 {
			return row.GetUint64(len(pkCols) + len(idxInfo.Columns))
		}

		var handle kv.Handle
		var tableRecord *consistency.RecordData
		var lastTableRecord *consistency.RecordData
		var indexRecord *consistency.RecordData
		i := 0
		for i < len(tblRow) || i < len(idxRow) {
			if i == len(tblRow) {
				// No more rows in table side.
				tableRecord = nil
			} else {
				handle, err = getHandleFromRow(tblRow[i])
				if err != nil {
					trySaveErr(err)
					return
				}
				value, err := getValueFromRow(tblRow[i])
				if err != nil {
					trySaveErr(err)
					return
				}
				tableRecord = &consistency.RecordData{Handle: handle, Values: value}
			}
			if i == len(idxRow) {
				// No more rows in index side.
				indexRecord = nil
			} else {
				indexHandle, err := getHandleFromRow(idxRow[i])
				if err != nil {
					trySaveErr(err)
					return
				}
				indexValue, err := getValueFromRow(idxRow[i])
				if err != nil {
					trySaveErr(err)
					return
				}
				indexRecord = &consistency.RecordData{Handle: indexHandle, Values: indexValue}
			}

			if tableRecord == nil {
				if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
					tableRecord = lastTableRecord
				}
				err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, tableRecord)
			} else if indexRecord == nil {
				err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, indexRecord, tableRecord)
			} else if tableRecord.Handle.Equal(indexRecord.Handle) && getCheckSum(tblRow[i]) != getCheckSum(idxRow[i]) {
				err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, indexRecord, tableRecord)
			} else if !tableRecord.Handle.Equal(indexRecord.Handle) {
				if tableRecord.Handle.Compare(indexRecord.Handle) < 0 {
					err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, tableRecord.Handle, nil, tableRecord)
				} else {
					if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
						err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, lastTableRecord)
					} else {
						err = ir().ReportAdminCheckInconsistent(w.e.contextCtx, indexRecord.Handle, indexRecord, nil)
					}
				}
			}
			if err != nil {
				trySaveErr(err)
				return
			}
			i++
			if tableRecord != nil {
				lastTableRecord = &consistency.RecordData{Handle: tableRecord.Handle, Values: tableRecord.Values}
			} else {
				lastTableRecord = nil
			}
		}
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
