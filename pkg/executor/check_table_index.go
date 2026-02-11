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
	collector  *AdminCheckIndexInconsistentCollector
	wg         sync.WaitGroup
	contextCtx context.Context
}

// AdminCheckIndexInconsistentType describes the inconsistency category found by
// fast `admin check index`.
type AdminCheckIndexInconsistentType string

const (
	// AdminCheckIndexRowWithoutIndex means one table row exists but the index row is missing.
	AdminCheckIndexRowWithoutIndex AdminCheckIndexInconsistentType = "row_without_index"
	// AdminCheckIndexIndexWithoutRow means one index row exists but the table row is missing.
	AdminCheckIndexIndexWithoutRow AdminCheckIndexInconsistentType = "index_without_row"
	// AdminCheckIndexRowIndexMismatch means table and index rows both exist but values don't match.
	AdminCheckIndexRowIndexMismatch AdminCheckIndexInconsistentType = "row_index_mismatch"
)

// AdminCheckIndexInconsistentRow stores one inconsistent handle and mismatch type.
type AdminCheckIndexInconsistentRow struct {
	Handle       string                          `json:"handle"`
	MismatchType AdminCheckIndexInconsistentType `json:"mismatch_type"`
}

// AdminCheckIndexInconsistentSummary is returned by HTTP API.
type AdminCheckIndexInconsistentSummary struct {
	InconsistentRowCount uint64                           `json:"inconsistent_row_count"`
	Rows                 []AdminCheckIndexInconsistentRow `json:"rows"`
}

// AdminCheckIndexInconsistentCollector collects inconsistent handles for one statement execution.
type AdminCheckIndexInconsistentCollector struct {
	collectLimit int

	mu     sync.Mutex
	first  error
	rows   []AdminCheckIndexInconsistentRow
	rowSet map[string]struct{}
}

// NewAdminCheckIndexInconsistentCollector creates an uncapped collector for fast `admin check index`.
func NewAdminCheckIndexInconsistentCollector() *AdminCheckIndexInconsistentCollector {
	return NewAdminCheckIndexInconsistentCollectorWithLimit(0)
}

// NewAdminCheckIndexInconsistentCollectorWithLimit creates a collector with an optional cap.
// When limit is greater than zero, at most `limit` rows are collected.
func NewAdminCheckIndexInconsistentCollectorWithLimit(limit int) *AdminCheckIndexInconsistentCollector {
	if limit < 0 {
		limit = 0
	}
	return &AdminCheckIndexInconsistentCollector{
		collectLimit: limit,
		rowSet:       make(map[string]struct{}),
	}
}

func (c *AdminCheckIndexInconsistentCollector) recordInconsistent(
	handle string,
	mismatchType AdminCheckIndexInconsistentType,
	err error,
) {
	if err == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.first == nil {
		c.first = err
	}
	if handle == "" {
		return
	}

	// Deduplicate by (handle, mismatch_type) so one handle can still appear
	// multiple times when different mismatch types are detected.
	rowKey := handle + "\x00" + string(mismatchType)
	if _, exists := c.rowSet[rowKey]; exists {
		return
	}
	if c.collectLimit > 0 && len(c.rows) >= c.collectLimit {
		return
	}
	c.rowSet[rowKey] = struct{}{}
	c.rows = append(c.rows, AdminCheckIndexInconsistentRow{Handle: handle, MismatchType: mismatchType})
}

func (c *AdminCheckIndexInconsistentCollector) recordErr(err error) {
	if c == nil || err == nil {
		return
	}
	c.mu.Lock()
	if c.first == nil {
		c.first = err
	}
	c.mu.Unlock()
}

func (c *AdminCheckIndexInconsistentCollector) firstErr() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.first
}

// Summary returns a copy of collected inconsistency rows.
func (c *AdminCheckIndexInconsistentCollector) Summary() *AdminCheckIndexInconsistentSummary {
	if c == nil {
		return &AdminCheckIndexInconsistentSummary{}
	}

	c.mu.Lock()
	rows := make([]AdminCheckIndexInconsistentRow, len(c.rows))
	copy(rows, c.rows)
	count := uint64(len(c.rows))
	c.mu.Unlock()

	return &AdminCheckIndexInconsistentSummary{
		InconsistentRowCount: count,
		Rows:                 rows,
	}
}

type checksumBucketTask struct {
	offset int
	mod    int
	depth  int
}

type mismatchBucket struct {
	bucket uint64
	count  int64
}

type leafRowStream struct {
	ctx context.Context
	rs  sqlexec.RecordSet
	chk *chunk.Chunk
	idx int
}

func newLeafRowStream(ctx context.Context, se sessionctx.Context, sql string) (*leafRowStream, error) {
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &leafRowStream{
		ctx: ctx,
		rs:  rs,
		chk: rs.NewChunk(nil),
	}, nil
}

func (s *leafRowStream) Next() (chunk.Row, bool, error) {
	for {
		if s.idx < s.chk.NumRows() {
			row := s.chk.GetRow(s.idx)
			s.idx++
			return row, true, nil
		}
		err := s.rs.Next(s.ctx, s.chk)
		if err != nil {
			return chunk.Row{}, false, err
		}
		if s.chk.NumRows() == 0 {
			return chunk.Row{}, false, nil
		}
		s.idx = 0
	}
}

func (s *leafRowStream) Close() {
	if s == nil || s.rs == nil {
		return
	}
	if err := s.rs.Close(); err != nil {
		logutil.BgLogger().Warn("close leaf row stream failed", zap.Error(err))
	}
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
	sessVars := e.Ctx().GetSessionVars()
	collectAll := sessVars.FastCheckTableCollectInconsistent
	sessVars.FastCheckTableInconsistentSummary = nil
	if collectAll {
		// Collector mode is opened by session variable.
		e.collector = NewAdminCheckIndexInconsistentCollectorWithLimit(sessVars.FastCheckTableInconsistentLimit)
	} else {
		// Keep original fail-fast path untouched when switch is disabled.
		e.collector = nil
	}

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

	if e.collector != nil {
		sessVars.FastCheckTableInconsistentSummary = e.collector.Summary()
		if firstErr := e.collector.firstErr(); firstErr != nil {
			return firstErr
		}
	}

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
	snapshot := w.e.Ctx().GetSessionVars().SnapshotTS
	if snapshot != 0 {
		_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, fmt.Sprintf("set session tidb_snapshot = %d", snapshot))
		if err != nil {
			logutil.BgLogger().Error("fail to set tidb_snapshot", zap.Error(err), zap.Uint64("snapshot ts", snapshot))
		}
	}

	return func() {
		sessVars.OptimizerUseInvisibleIndexes = originOptUseInvisibleIdx
		sessVars.MemQuotaQuery = originMemQuotaQuery
		if snapshot != 0 {
			_, err := se.GetSQLExecutor().ExecuteInternal(w.e.contextCtx, "set session tidb_snapshot = 0")
			if err != nil {
				logutil.BgLogger().Error("fail to set tidb_snapshot to 0", zap.Error(err))
			}
		}
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
	reportErr := func(err error) bool {
		if err == nil {
			return false
		}
		if w.e.collector == nil {
			// Normal SQL path: stop at first inconsistency/error.
			trySaveErr(err)
			return true
		}
		// Collector path: keep the first error but continue to collect
		// mismatched handles from other buckets.
		w.e.collector.recordErr(err)
		trySaveErr(err)
		return true
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

	lookupCheckThreshold := int64(100)

	_, err = se.GetSQLExecutor().ExecuteInternal(ctx, "begin")
	if err != nil {
		trySaveErr(err)
		return
	}

	errCtx := w.sctx.GetSessionVars().StmtCtx.ErrCtx()
	getHandleFromRow := func(row chunk.Row) (kv.Handle, error) {
		if tblMeta.IsCommonHandle {
			handleDatum := make([]types.Datum, 0, len(pkTypes))
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
		valueDatum := make([]types.Datum, 0, len(idxInfo.Columns))
		for i, t := range idxInfo.Columns {
			valueDatum = append(valueDatum, row.GetDatum(i+len(pkCols), &tblMeta.Columns[t.Offset].FieldType))
		}
		return valueDatum, nil
	}
	getRowChecksum := func(row chunk.Row) uint64 {
		return row.GetUint64(len(pkCols) + len(idxInfo.Columns))
	}

	buildRecordData := func(row chunk.Row) (*consistency.RecordData, error) {
		handle, err := getHandleFromRow(row)
		if err != nil {
			return nil, err
		}
		value, err := getValueFromRow(row)
		if err != nil {
			return nil, err
		}
		return &consistency.RecordData{Handle: handle, Values: value}, nil
	}
	cloneRecordData := func(record *consistency.RecordData) *consistency.RecordData {
		if record == nil {
			return nil
		}
		return &consistency.RecordData{Handle: record.Handle, Values: types.CloneRow(record.Values)}
	}
	newReporter := func() *consistency.Reporter {
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
	reportInconsistent := func(reporter *consistency.Reporter, mismatchType AdminCheckIndexInconsistentType, handle kv.Handle, idxRecord, tblRecord *consistency.RecordData) bool {
		err := reporter.ReportAdminCheckInconsistent(w.e.contextCtx, handle, idxRecord, tblRecord)
		if err == nil {
			return false
		}
		if w.e.collector == nil {
			trySaveErr(err)
			return true
		}
		w.e.collector.recordInconsistent(handle.String(), mismatchType, err)
		return false
	}

	compareChecksumBuckets := func(tableChecksum, indexChecksum []groupByChecksum) []mismatchBucket {
		mismatch := make([]mismatchBucket, 0)
		i, j := 0, 0
		for i < len(tableChecksum) && j < len(indexChecksum) {
			if tableChecksum[i].bucket == indexChecksum[j].bucket {
				if tableChecksum[i].checksum != indexChecksum[j].checksum {
					mismatch = append(mismatch, mismatchBucket{bucket: tableChecksum[i].bucket, count: max(tableChecksum[i].count, indexChecksum[j].count)})
				}
				i++
				j++
				continue
			}
			if tableChecksum[i].bucket < indexChecksum[j].bucket {
				mismatch = append(mismatch, mismatchBucket{bucket: tableChecksum[i].bucket, count: tableChecksum[i].count})
				i++
				continue
			}
			mismatch = append(mismatch, mismatchBucket{bucket: indexChecksum[j].bucket, count: indexChecksum[j].count})
			j++
		}
		for ; i < len(tableChecksum); i++ {
			mismatch = append(mismatch, mismatchBucket{bucket: tableChecksum[i].bucket, count: tableChecksum[i].count})
		}
		for ; j < len(indexChecksum); j++ {
			mismatch = append(mismatch, mismatchBucket{bucket: indexChecksum[j].bucket, count: indexChecksum[j].count})
		}
		return mismatch
	}
	sumChecksumCount := func(checksums []groupByChecksum) int64 {
		total := int64(0)
		for _, checksum := range checksums {
			total += checksum.count
		}
		return total
	}
	buildChecksumQueries := func(task checksumBucketTask, forceRoot bool) (tblQuery, idxQuery string) {
		whereKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle, task.offset, task.mod)
		if forceRoot {
			whereKey = "0"
		}
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) div %d %% %d)", md5Handle, task.offset, task.mod, bucketSize)
		tblQuery = fmt.Sprintf(
			"select /*+ read_from_storage(tikv[%s]) */ bit_xor(%s), %s, count(*) from %s use index() where %s = 0 group by %s",
			tblName, md5HandleAndIndexCol, groupByKey, tblName, whereKey, groupByKey)
		idxQuery = fmt.Sprintf(
			"select bit_xor(%s), %s, count(*) from %s use index(`%s`) where %s = 0 group by %s",
			md5HandleAndIndexCol, groupByKey, tblName, idxInfo.Name, whereKey, groupByKey)
		return tblQuery, idxQuery
	}
	buildRowQueries := func(task checksumBucketTask) (tableSQL, indexSQL string) {
		groupByKey := fmt.Sprintf("((cast(%s as signed) - %d) %% %d)", md5Handle, task.offset, task.mod)
		indexSQL = fmt.Sprintf(
			"select %s, %s, %s from %s use index(`%s`) where %s = 0 order by %s",
			handleColumns, indexColumns, md5HandleAndIndexCol, tblName, idxInfo.Name, groupByKey, handleColumns)
		tableSQL = fmt.Sprintf(
			"select /*+ read_from_storage(tikv[%s]) */ %s, %s, %s from %s use index() where %s = 0 order by %s",
			tblName, handleColumns, indexColumns, md5HandleAndIndexCol, tblName, groupByKey, handleColumns)
		return tableSQL, indexSQL
	}
	nextRecord := func(stream *leafRowStream) (*consistency.RecordData, chunk.Row, bool, error) {
		row, ok, err := stream.Next()
		if err != nil || !ok {
			return nil, chunk.Row{}, ok, err
		}
		record, err := buildRecordData(row)
		if err != nil {
			return nil, chunk.Row{}, false, err
		}
		return record, row, true, nil
	}
	checkLeafBucket := func(task checksumBucketTask) bool {
		tableSQL, indexSQL := buildRowQueries(task)
		tableStream, err := newLeafRowStream(ctx, se, tableSQL)
		if err != nil {
			return reportErr(err)
		}
		defer tableStream.Close()
		indexStream, err := newLeafRowStream(ctx, se, indexSQL)
		if err != nil {
			return reportErr(err)
		}
		defer indexStream.Close()

		reporter := newReporter()
		var lastTableRecord *consistency.RecordData
		tableRecord, tableRow, hasTable, err := nextRecord(tableStream)
		if err != nil {
			return reportErr(err)
		}
		indexRecord, indexRow, hasIndex, err := nextRecord(indexStream)
		if err != nil {
			return reportErr(err)
		}
		// Merge two ordered streams by handle and classify each mismatch.
		for hasTable || hasIndex {
			switch {
			case !hasTable:
				if lastTableRecord != nil && lastTableRecord.Handle.Equal(indexRecord.Handle) {
					tableRecord = lastTableRecord
				}
				if reportInconsistent(reporter, AdminCheckIndexIndexWithoutRow, indexRecord.Handle, indexRecord, tableRecord) {
					return true
				}
				indexRecord, indexRow, hasIndex, err = nextRecord(indexStream)
				if err != nil {
					return reportErr(err)
				}
			case !hasIndex:
				if reportInconsistent(reporter, AdminCheckIndexRowWithoutIndex, tableRecord.Handle, nil, tableRecord) {
					return true
				}
				lastTableRecord = cloneRecordData(tableRecord)
				tableRecord, tableRow, hasTable, err = nextRecord(tableStream)
				if err != nil {
					return reportErr(err)
				}
			case tableRecord.Handle.Equal(indexRecord.Handle):
				if getRowChecksum(tableRow) != getRowChecksum(indexRow) {
					if reportInconsistent(reporter, AdminCheckIndexRowIndexMismatch, tableRecord.Handle, indexRecord, tableRecord) {
						return true
					}
				}
				lastTableRecord = cloneRecordData(tableRecord)
				tableRecord, tableRow, hasTable, err = nextRecord(tableStream)
				if err != nil {
					return reportErr(err)
				}
				indexRecord, indexRow, hasIndex, err = nextRecord(indexStream)
				if err != nil {
					return reportErr(err)
				}
			case tableRecord.Handle.Compare(indexRecord.Handle) < 0:
				if reportInconsistent(reporter, AdminCheckIndexRowWithoutIndex, tableRecord.Handle, nil, tableRecord) {
					return true
				}
				lastTableRecord = cloneRecordData(tableRecord)
				tableRecord, tableRow, hasTable, err = nextRecord(tableStream)
				if err != nil {
					return reportErr(err)
				}
			default:
				compareTableRecord := lastTableRecord
				if compareTableRecord != nil && compareTableRecord.Handle.Equal(indexRecord.Handle) {
					if reportInconsistent(reporter, AdminCheckIndexRowIndexMismatch, indexRecord.Handle, indexRecord, compareTableRecord) {
						return true
					}
				} else if reportInconsistent(reporter, AdminCheckIndexIndexWithoutRow, indexRecord.Handle, indexRecord, nil) {
					return true
				}
				indexRecord, indexRow, hasIndex, err = nextRecord(indexStream)
				if err != nil {
					return reportErr(err)
				}
			}
		}
		return false
	}

	// Breadth-first refinement over checksum buckets: narrow down mismatched
	// buckets first, then perform row-level comparison only on suspect buckets.
	queue := make([]checksumBucketTask, 0, 16)
	queue = append(queue, checksumBucketTask{offset: 0, mod: 1, depth: 0})
	const maxRefineDepth = 10
	for len(queue) > 0 {
		task := queue[0]
		queue = queue[1:]

		tblQuery, idxQuery := buildChecksumQueries(task, task.depth == 0)
		logutil.BgLogger().Info(
			"fast check table by group",
			zap.String("table name", tblMeta.Name.String()),
			zap.String("index name", idxInfo.Name.String()),
			zap.Int("depth", task.depth),
			zap.Int("current offset", task.offset),
			zap.Int("current mod", task.mod),
			zap.String("table sql", tblQuery),
			zap.String("index sql", idxQuery),
		)

		tableChecksum, err := getCheckSum(w.e.contextCtx, se, tblQuery)
		if err != nil {
			reportErr(err)
			return
		}
		slices.SortFunc(tableChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})
		indexChecksum, err := getCheckSum(w.e.contextCtx, se, idxQuery)
		if err != nil {
			reportErr(err)
			return
		}
		slices.SortFunc(indexChecksum, func(i, j groupByChecksum) int {
			return cmp.Compare(i.bucket, j.bucket)
		})
		if task.depth == 0 {
			logutil.BgLogger().Info(
				"fast check index root row count",
				zap.String("table name", tblMeta.Name.String()),
				zap.String("index name", idxInfo.Name.String()),
				zap.Int64("table row count", sumChecksumCount(tableChecksum)),
				zap.Int64("index row count", sumChecksumCount(indexChecksum)),
			)
		}

		mismatches := compareChecksumBuckets(tableChecksum, indexChecksum)
		if len(mismatches) == 0 {
			continue
		}
		// Fast path keeps original fail-fast behavior; collector mode keeps all mismatches.
		if w.e.collector == nil {
			mismatches = mismatches[:1]
		}
		for _, mismatch := range mismatches {
			nextTask := checksumBucketTask{
				offset: task.offset + int(mismatch.bucket)*task.mod,
				mod:    task.mod * bucketSize,
				depth:  task.depth + 1,
			}
			if nextTask.mod <= 0 {
				if checkLeafBucket(task) {
					return
				}
				if w.e.collector == nil {
					break
				}
				continue
			}
			// Fail-fast mode: stop refining early once one candidate bucket is small enough.
			if w.e.collector == nil && (mismatch.count <= lookupCheckThreshold || nextTask.depth >= maxRefineDepth) {
				if checkLeafBucket(nextTask) {
					return
				}
				break
			}
			// Collector mode: continue refining until row-level scan can enumerate
			// all inconsistent handles deterministically.
			if w.e.collector != nil && (mismatch.count <= 1 || nextTask.depth >= maxRefineDepth) {
				if checkLeafBucket(nextTask) {
					return
				}
				continue
			}
			queue = append(queue, nextTask)
			if w.e.collector == nil {
				break
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
