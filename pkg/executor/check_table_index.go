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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/admin"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var errCheckPartialIndexWithoutFastCheck = dbterror.ClassExecutor.NewStd(errno.ErrCheckPartialIndexWithoutFastCheck)

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
		if idx.HasCondition() {
			return errors.Trace(errCheckPartialIndexWithoutFastCheck)
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
			return err
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
	is         infoschema.InfoSchema
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

	ch := make(chan checkIndexTask)
	dc := operator.NewSimpleDataChannel(ch)

	wctx := workerpool.NewContext(ctx)
	op := operator.NewAsyncOperator(wctx,
		workerpool.NewWorkerPool("checkIndex",
			poolutil.CheckTable, 3, e.createWorker))
	op.SetSource(dc)
	//nolint: errcheck
	op.Open()

OUTER:
	for i := range e.indexInfos {
		select {
		case ch <- checkIndexTask{indexOffset: i}:
		case <-wctx.Done():
			break OUTER
		}
	}
	close(ch)

	err := op.Close()
	if opErr := wctx.OperatorErr(); opErr != nil {
		return opErr
	}
	return err
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

type fastCheckSysSessionVarsBackup struct {
	optimizerUseInvisibleIndexes bool
	memQuotaQuery                int64
	distSQLScanConcurrency       int
	executorConcurrency          int
	enablePaging                 bool
	minPagingSize                int
	maxPagingSize                int
	storeBatchSize               int
	enabledRateLimitAction       bool
	maxExecutionTime             uint64
	tikvClientReadTimeout        uint64
}

func backupFastCheckSysSessionVars(sv *variable.SessionVars) fastCheckSysSessionVarsBackup {
	return fastCheckSysSessionVarsBackup{
		optimizerUseInvisibleIndexes: sv.OptimizerUseInvisibleIndexes,
		memQuotaQuery:                sv.MemQuotaQuery,
		distSQLScanConcurrency:       sv.DistSQLScanConcurrency(),
		executorConcurrency:          sv.ExecutorConcurrency,
		enablePaging:                 sv.EnablePaging,
		minPagingSize:                sv.MinPagingSize,
		maxPagingSize:                sv.MaxPagingSize,
		storeBatchSize:               sv.StoreBatchSize,
		enabledRateLimitAction:       sv.EnabledRateLimitAction,
		maxExecutionTime:             sv.MaxExecutionTime,
		tikvClientReadTimeout:        sv.TiKVClientReadTimeout,
	}
}

func (b fastCheckSysSessionVarsBackup) restoreTo(sv *variable.SessionVars) {
	sv.OptimizerUseInvisibleIndexes = b.optimizerUseInvisibleIndexes
	sv.MemQuotaQuery = b.memQuotaQuery
	sv.SetDistSQLScanConcurrency(b.distSQLScanConcurrency)
	sv.ExecutorConcurrency = b.executorConcurrency
	sv.EnablePaging = b.enablePaging
	sv.MinPagingSize = b.minPagingSize
	sv.MaxPagingSize = b.maxPagingSize
	sv.StoreBatchSize = b.storeBatchSize
	sv.EnabledRateLimitAction = b.enabledRateLimitAction
	sv.MaxExecutionTime = b.maxExecutionTime
	sv.TiKVClientReadTimeout = b.tikvClientReadTimeout
}

func applyFastCheckSysSessionVars(sysVars, userVars *variable.SessionVars) {
	sysVars.OptimizerUseInvisibleIndexes = true
	sysVars.MemQuotaQuery = userVars.MemQuotaQuery
	sysVars.SetDistSQLScanConcurrency(userVars.DistSQLScanConcurrency())
	sysVars.ExecutorConcurrency = userVars.ExecutorConcurrency
	sysVars.EnablePaging = userVars.EnablePaging
	sysVars.MinPagingSize = userVars.MinPagingSize
	sysVars.MaxPagingSize = userVars.MaxPagingSize
	sysVars.StoreBatchSize = userVars.StoreBatchSize
	sysVars.EnabledRateLimitAction = userVars.EnabledRateLimitAction
	sysVars.MaxExecutionTime = userVars.MaxExecutionTime
	sysVars.TiKVClientReadTimeout = userVars.TiKVClientReadTimeout
}

func (w *checkIndexWorker) initSessCtx(se sessionctx.Context) (restore func()) {
	sessVars := se.GetSessionVars()
	backup := backupFastCheckSysSessionVars(sessVars)
	applyFastCheckSysSessionVars(sessVars, w.sctx.GetSessionVars())
	snapshot := w.e.Ctx().GetSessionVars().SnapshotTS
	if snapshot != 0 {
		_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, fmt.Sprintf("set session tidb_snapshot = %d", snapshot))
		if err != nil {
			logutil.BgLogger().Error("fail to set tidb_snapshot", zap.Error(err), zap.Uint64("snapshot ts", snapshot))
		}
	}

	return func() {
		backup.restoreTo(sessVars)
		if snapshot != 0 {
			_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, "set session tidb_snapshot = 0")
			if err != nil {
				logutil.BgLogger().Error("fail to set tidb_snapshot to 0", zap.Error(err))
			}
		}
	}
}

func queryToRow(ctx context.Context, se sessionctx.Context, sql string) ([]chunk.Row, error) {
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql)
	if err != nil {
		return nil, err
	}
	return sqlexec.DrainRecordSetAndClose(ctx, rs, 4096)
}

func verifyIndexSideQuery(ctx context.Context, se sessionctx.Context, sql string) bool {
	rows, err := queryToRow(ctx, se, "explain "+sql)
	if err != nil {
		return false
	}

	isTableScan := false
	isIndexScan := false
	for _, row := range rows {
		op := row.GetString(0)
		if strings.Contains(op, "TableFullScan") {
			isTableScan = true
		} else if strings.Contains(op, "IndexFullScan") || strings.Contains(op, "IndexRangeScan") {
			// It's also possible to be index range scan if the index has condition.
			// TODO: if the planner eliminates the range in the index scan, we can remove the `IndexRangeScan` check.
			isIndexScan = true
		} else if strings.Contains(op, "PointGet") || strings.Contains(op, "BatchPointGet") {
			if row.Len() > 3 {
				accessObject := row.GetString(3)
				// accessObject is a normalized string with components separated by ", ".
				// Match ", index:" to identify non-clustered index paths and avoid
				// treating ", clustered index:" (PRIMARY/handle path) as index access.
				if strings.Contains(accessObject, ", index:") {
					isIndexScan = true
				}
			}
		}
	}

	if isTableScan || !isIndexScan {
		return false
	}
	return true
}

func (w *checkIndexWorker) quickPassGlobalChecksum(ctx context.Context, se sessionctx.Context, tblName string, idxInfo *model.IndexInfo, rowChecksumExpr, idxCondition string) (bool, error) {
	tableGlobalQuery := fmt.Sprintf(
		"select /*+ read_from_storage(tikv[%s]), AGG_TO_COP() */ bit_xor(%s), count(*) from %s use index() where %s(0 = 0)",
		tblName, rowChecksumExpr, tblName, idxCondition)
	indexGlobalQuery := fmt.Sprintf(
		"select /*+ AGG_TO_COP() */ bit_xor(%s), count(*) from %s use index(`%s`) where %s(0 = 0)",
		rowChecksumExpr, tblName, idxInfo.Name, idxCondition)

	tableGlobalChecksum, tableGlobalCnt, err := getGlobalCheckSum(w.e.contextCtx, se, tableGlobalQuery)
	if err != nil {
		return false, err
	}
	intest.AssertFunc(func() bool {
		return verifyIndexSideQuery(ctx, se, indexGlobalQuery)
	}, "index side query plan is not correct: %s", indexGlobalQuery)
	indexGlobalChecksum, indexGlobalCnt, err := getGlobalCheckSum(w.e.contextCtx, se, indexGlobalQuery)
	if err != nil {
		return false, err
	}

	return tableGlobalCnt == indexGlobalCnt && tableGlobalChecksum == indexGlobalChecksum, nil
}

// HandleTask implements the Worker interface.
func (w *checkIndexWorker) HandleTask(task checkIndexTask, _ func(workerpool.None)) error {
	failpoint.Inject("mockFastCheckTableError", func() {
		failpoint.Return(errors.New("mock fast check table error"))
	})

	idxInfo := w.indexInfos[task.indexOffset]
	bucketSize := int(CheckTableFastBucketSize.Load())

	ctx := kv.WithInternalSourceType(w.e.contextCtx, kv.InternalTxnAdmin)

	se, err := w.e.BaseExecutor.GetSysSession()
	if err != nil {
		return err
	}
	restoreCtx := w.initSessCtx(se)
	defer func() {
		restoreCtx()
		w.e.BaseExecutor.ReleaseSysSession(ctx, se)
	}()
	var fpErr error
	failpoint.InjectCall("fastCheckTableAfterInitSessCtx", se.GetSessionVars(), &fpErr)
	if fpErr != nil {
		return fpErr
	}

	tblMeta := w.table.Meta()
	tblName := TableName(w.e.dbName, tblMeta.Name.String())

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
	handleColumns := strings.Join(pkCols, ",")

	indexColNames := make([]string, len(idxInfo.Columns))
	for i, col := range idxInfo.Columns {
		tblCol := tblMeta.Columns[col.Offset]
		if tblCol.IsVirtualGenerated() && tblCol.Hidden {
			indexColNames[i] = tblCol.GeneratedExprString
		} else {
			indexColNames[i] = ColumnName(col.Name.O)
		}
	}
	indexColumns := strings.Join(indexColNames, ",")

	// CheckSum of (handle + index columns).
	md5HandleAndIndexCol := fmt.Sprintf("crc32(md5(concat_ws(0x2, %s, %s)))", handleColumns, indexColumns)

	// Used to group by and order.
	md5Handle := fmt.Sprintf("crc32(md5(concat_ws(0x2, %s)))", handleColumns)

	tableRowCntToCheck := int64(0)

	offset := 0
	mod := 1
	meetError := false

	lookupCheckThreshold := int64(100)
	checkOnce := false

	idxCondition := ""
	if idxInfo.HasCondition() {
		idxCondition = fmt.Sprintf("(%s) AND ", idxInfo.ConditionExprString)
	}

	_, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin")
	if err != nil {
		return err
	}

	// Quick pass: avoid bucketed grouping if table/index checksum already matches.
	match, err := w.quickPassGlobalChecksum(ctx, se, tblName, idxInfo, md5HandleAndIndexCol, idxCondition)
	if err != nil {
		return err
	}
	failpoint.Inject("forceAdminCheckBucketed", func() {
		match = false
	})
	if match {
		return nil
	}

	failpoint.Inject("mockFastCheckTableBucketedCalled", func() {
		failpoint.Return(errors.New("mock fast check table bucketed called"))
	})

	times := 0
	const maxTimes = 10
	for tableRowCntToCheck > lookupCheckThreshold || !checkOnce {
		times++
		if times == maxTimes {
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
			"select /*+ read_from_storage(tikv[%s]), AGG_TO_COP() */ bit_xor(%s), %s, count(*) from %s use index() where %s(%s = 0) group by %s",
			tblName, md5HandleAndIndexCol, groupByKey, tblName, idxCondition, whereKey, groupByKey)
		idxQuery := fmt.Sprintf(
			"select /*+ AGG_TO_COP() */ bit_xor(%s), %s, count(*) from %s use index(`%s`) where %s (%s = 0) group by%s",
			md5HandleAndIndexCol, groupByKey, tblName, idxInfo.Name, idxCondition, whereKey, groupByKey)

		logutil.BgLogger().Info(
			"fast check table by group",
			zap.String("table name", tblMeta.Name.String()),
			zap.String("index name", idxInfo.Name.String()),
			zap.Int("times", times),
			zap.Int("current offset", offset), zap.Int("current mod", mod),
			zap.String("table sql", tblQuery), zap.String("index sql", idxQuery),
		)

		// compute table side checksum.
		tableChecksum, err := getCheckSum(w.e.contextCtx, se, tblQuery)
		if err != nil {
			return err
		}
		slices.SortFunc(tableChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

		// compute index side checksum.
		intest.AssertFunc(func() bool {
			return verifyIndexSideQuery(ctx, se, idxQuery)
		}, "index side query plan is not correct: %s", idxQuery)
		indexChecksum, err := getCheckSum(w.e.contextCtx, se, idxQuery)
		if err != nil {
			return err
		}
		slices.SortFunc(indexChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})

		currentOffset := 0

		meetError = false
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
				logutil.BgLogger().Error(
					"unexpected result, no error detected in this round, but an error is detected in the previous round",
					zap.Int("times", times), zap.Int("offset", offset), zap.Int("mod", mod))
			}
			break
		}

		offset += currentOffset * mod
		mod *= bucketSize
	}

	if meetError {
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle, offset, mod)
		indexSQL := fmt.Sprintf(
			"select /*+ AGG_TO_COP() */ %s, %s, %s from %s use index(`%s`) where %s(%s = 0) order by %s",
			handleColumns, indexColumns, md5HandleAndIndexCol, tblName, idxInfo.Name, idxCondition, groupByKey, handleColumns)
		tableSQL := fmt.Sprintf(
			"select /*+ read_from_storage(tikv[%s]), AGG_TO_COP() */ %s, %s, %s from %s use index() where %s(%s = 0) order by %s",
			tblName, handleColumns, indexColumns, md5HandleAndIndexCol, tblName, idxCondition, groupByKey, handleColumns)
		intest.AssertFunc(func() bool {
			return verifyIndexSideQuery(ctx, se, indexSQL)
		}, "index side query plan is not correct: %s", indexSQL)
		idxRow, err := queryToRow(ctx, se, indexSQL)
		if err != nil {
			return err
		}
		tblRow, err := queryToRow(ctx, se, tableSQL)
		if err != nil {
			return err
		}

		errCtx := w.sctx.GetSessionVars().StmtCtx.ErrCtx()
		getHandleFromRow := func(row chunk.Row) (kv.Handle, error) {
			if tblMeta.IsCommonHandle {
				handleDatum := make([]types.Datum, 0)
				for i, t := range pkTypes {
					handleDatum = append(handleDatum, row.GetDatum(i, t))
				}
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
				valueDatum = append(valueDatum, row.GetDatum(i+len(pkCols), &tblMeta.Columns[t.Offset].FieldType))
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
				Tbl:             tblMeta,
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
					return err
				}
				value, err := getValueFromRow(tblRow[i])
				if err != nil {
					return err
				}
				tableRecord = &consistency.RecordData{Handle: handle, Values: value}
			}
			if i == len(idxRow) {
				// No more rows in index side.
				indexRecord = nil
			} else {
				indexHandle, err := getHandleFromRow(idxRow[i])
				if err != nil {
					return err
				}
				indexValue, err := getValueFromRow(idxRow[i])
				if err != nil {
					return err
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
				return err
			}
			i++
			if tableRecord != nil {
				lastTableRecord = &consistency.RecordData{Handle: tableRecord.Handle, Values: tableRecord.Values}
			} else {
				lastTableRecord = nil
			}
		}
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
func (checkIndexTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return "fast_check_table", "checkIndexTask", nil
}

type groupByChecksum struct {
	bucket   uint64
	checksum uint64
	count    int64
}

func getCheckSum(ctx context.Context, se sessionctx.Context, sql string) ([]groupByChecksum, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
	// Create a new ExecDetails for the internal SQL to avoid data race with the parent statement.
	// The parent context may carry an ExecDetails that is still being written to by concurrent goroutines.
	ctx = execdetails.ContextWithInitializedExecDetails(ctx)
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

func getGlobalCheckSum(ctx context.Context, se sessionctx.Context, sql string) (checksum uint64, count int64, err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql)
	if err != nil {
		return 0, 0, err
	}
	defer func(rs sqlexec.RecordSet) {
		err := rs.Close()
		if err != nil {
			logutil.BgLogger().Error("close record set failed", zap.Error(err))
		}
	}(rs)

	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}

	row := rows[0]
	if !row.IsNull(0) {
		checksum = row.GetUint64(0)
	}
	count = row.GetInt64(1)
	return checksum, count, nil
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
