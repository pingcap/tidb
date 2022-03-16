// Copyright 2015 PingCAP, Inc.
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
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/deadlockhistory"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/topsql"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	tikvutil "github.com/tikv/client-go/v2/util"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	_ Executor = &baseExecutor{}
	_ Executor = &CheckTableExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &HashJoinExec{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &MergeJoinExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowNextRowIDExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &ShowDDLJobsExec{}
	_ Executor = &ShowDDLJobQueriesExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableReaderExecutor{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}

	// GlobalMemoryUsageTracker is the ancestor of all the Executors' memory tracker and GlobalMemory Tracker
	GlobalMemoryUsageTracker *memory.Tracker
	// GlobalDiskUsageTracker is the ancestor of all the Executors' disk tracker
	GlobalDiskUsageTracker *disk.Tracker
)

var (
	_ dataSourceExecutor = &TableReaderExecutor{}
	_ dataSourceExecutor = &IndexReaderExecutor{}
	_ dataSourceExecutor = &IndexLookUpExecutor{}
	_ dataSourceExecutor = &IndexMergeReaderExecutor{}
)

// dataSourceExecutor is a table DataSource converted Executor.
// Currently, there are TableReader/IndexReader/IndexLookUp/IndexMergeReader.
// Note, partition reader is special and the caller should handle it carefully.
type dataSourceExecutor interface {
	Executor
	Table() table.Table
}

type baseExecutor struct {
	ctx           sessionctx.Context
	id            int
	schema        *expression.Schema // output schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.BasicRuntimeStats
}

const (
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Global Storage Quota!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type globalPanicOnExceed struct {
	memory.BaseOOMAction
	mutex sync.Mutex // For synchronization.
}

func init() {
	action := &globalPanicOnExceed{}
	GlobalMemoryUsageTracker = memory.NewGlobalTracker(memory.LabelForGlobalMemory, -1)
	GlobalMemoryUsageTracker.SetActionOnExceed(action)
	GlobalDiskUsageTracker = disk.NewGlobalTrcaker(memory.LabelForGlobalStorage, -1)
	GlobalDiskUsageTracker.SetActionOnExceed(action)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *globalPanicOnExceed) SetLogHook(hook func(uint64)) {}

// Action panics when storage usage exceeds storage quota.
func (a *globalPanicOnExceed) Action(t *memory.Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	msg := ""
	switch t.Label() {
	case memory.LabelForGlobalStorage:
		msg = globalPanicStorageExceed
	case memory.LabelForGlobalMemory:
		msg = globalPanicMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	panic(msg)
}

// GetPriority get the priority of the Action
func (a *globalPanicOnExceed) GetPriority() int64 {
	return memory.DefPanicPriority
}

// base returns the baseExecutor of an executor, don't override this method!
func (e *baseExecutor) base() *baseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// newFirstChunk creates a new chunk to buffer current executor's result.
func newFirstChunk(e Executor) *chunk.Chunk {
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// newList creates a new List to buffer current executor's result.
func newList(e Executor) *chunk.List {
	base := e.base()
	return chunk.NewList(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// retTypes returns all output column types.
func retTypes(e Executor) []*types.FieldType {
	base := e.base()
	return base.retFieldTypes
}

// Next fills multiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

func (e *baseExecutor) updateDeltaForTableID(id int64) {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
}

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id int, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().InitChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id > 0 {
			e.runtimeStats = &execdetails.BasicRuntimeStats{}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(id, e.runtimeStats)
		}
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, req *chunk.Chunk) error {
	base := e.base()
	if base.runtimeStats != nil {
		start := time.Now()
		defer func() { base.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	sessVars := base.ctx.GetSessionVars()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		return ErrQueryInterrupted
	}
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("%T.Next", e), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	if trace.IsEnabled() {
		defer trace.StartRegion(ctx, fmt.Sprintf("%T.Next", e)).End()
	}
	err := e.Next(ctx, req)

	if err != nil {
		return err
	}
	// recheck whether the session/query is killed during the Next()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		err = ErrQueryInterrupted
	}
	return err
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Open implements the Executor Open interface.
func (e *CancelDDLJobsExec) Open(ctx context.Context) error {
	// We want to use a global transaction to execute the admin command, so we don't use e.ctx here.
	errInTxn := kv.RunInNewTxn(context.Background(), e.ctx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) (err error) {
		e.errs, err = admin.CancelJobs(txn, e.jobIDs)
		return
	})
	return errInTxn
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, strconv.FormatInt(e.jobIDs[i], 10))
		if e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowNextRowIDExec represents a show the next row ID executor.
type ShowNextRowIDExec struct {
	baseExecutor
	tblName *ast.TableName
	done    bool
}

// Next implements the Executor Next interface.
func (e *ShowNextRowIDExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	is := domain.GetDomain(e.ctx).InfoSchema()
	tbl, err := is.TableByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return err
	}
	tblMeta := tbl.Meta()

	allocators := tbl.Allocators(e.ctx)
	for _, alloc := range allocators {
		nextGlobalID, err := alloc.NextGlobalAutoID()
		if err != nil {
			return err
		}

		var colName, idType string
		switch alloc.GetType() {
		case autoid.RowIDAllocType, autoid.AutoIncrementType:
			idType = "AUTO_INCREMENT"
			if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
				colName = col.Name.O
			} else {
				colName = model.ExtraHandleName.O
			}
		case autoid.AutoRandomType:
			idType = "AUTO_RANDOM"
			colName = tblMeta.GetPkName().O
		case autoid.SequenceType:
			idType = "SEQUENCE"
			colName = ""
		default:
			return autoid.ErrInvalidAllocatorType.GenWithStackByArgs()
		}

		req.AppendString(0, e.tblName.Schema.O)
		req.AppendString(1, e.tblName.Name.O)
		req.AppendString(2, colName)
		req.AppendInt64(3, nextGlobalID)
		req.AppendString(4, idType)
	}

	e.done = true
	return nil
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	baseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *admin.DDLInfo
	done       bool
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	ddlJobs := ""
	query := ""
	l := len(e.ddlInfo.Jobs)
	for i, job := range e.ddlInfo.Jobs {
		ddlJobs += job.String()
		query += job.Query
		if i != l-1 {
			ddlJobs += "\n"
			query += "\n"
		}
	}

	serverInfo, err := infosync.GetServerInfoByID(ctx, e.ddlOwnerID)
	if err != nil {
		return err
	}

	serverAddress := serverInfo.IP + ":" +
		strconv.FormatUint(uint64(serverInfo.Port), 10)

	req.AppendInt64(0, e.ddlInfo.SchemaVer)
	req.AppendString(1, e.ddlOwnerID)
	req.AppendString(2, serverAddress)
	req.AppendString(3, ddlJobs)
	req.AppendString(4, e.selfID)
	req.AppendString(5, query)

	e.done = true
	return nil
}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor
	DDLJobRetriever

	jobNumber int
	is        infoschema.InfoSchema
}

// DDLJobRetriever retrieve the DDLJobs.
// nolint:structcheck
type DDLJobRetriever struct {
	runningJobs    []*model.Job
	historyJobIter *meta.LastJobIterator
	cursor         int
	is             infoschema.InfoSchema
	activeRoles    []*auth.RoleIdentity
	cacheJobs      []*model.Job
	TZLoc          *time.Location
}

func (e *DDLJobRetriever) initial(txn kv.Transaction) error {
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		return err
	}
	m := meta.NewMeta(txn)
	e.historyJobIter, err = m.GetLastHistoryDDLJobsIterator()
	if err != nil {
		return err
	}
	e.runningJobs = jobs
	e.cursor = 0
	return nil
}

func (e *DDLJobRetriever) appendJobToChunk(req *chunk.Chunk, job *model.Job, checker privilege.Manager) {
	schemaName := job.SchemaName
	tableName := ""
	finishTS := uint64(0)
	if job.BinlogInfo != nil {
		finishTS = job.BinlogInfo.FinishedTS
		if job.BinlogInfo.TableInfo != nil {
			tableName = job.BinlogInfo.TableInfo.Name.L
		}
		if job.BinlogInfo.MultipleTableInfos != nil {
			tablenames := new(strings.Builder)
			for i, affect := range job.BinlogInfo.MultipleTableInfos {
				if i > 0 {
					fmt.Fprintf(tablenames, ",")
				}
				fmt.Fprintf(tablenames, "%s", affect.Name.L)
			}
			tableName = tablenames.String()
		}
		if len(schemaName) == 0 && job.BinlogInfo.DBInfo != nil {
			schemaName = job.BinlogInfo.DBInfo.Name.L
		}
	}
	// For compatibility, the old version of DDL Job wasn't store the schema name and table name.
	if len(schemaName) == 0 {
		schemaName = getSchemaName(e.is, job.SchemaID)
	}
	if len(tableName) == 0 {
		tableName = getTableName(e.is, job.TableID)
	}

	createTime := ts2Time(job.StartTS, e.TZLoc)
	startTime := ts2Time(job.RealStartTS, e.TZLoc)
	finishTime := ts2Time(finishTS, e.TZLoc)

	// Check the privilege.
	if checker != nil && !checker.RequestVerification(e.activeRoles, strings.ToLower(schemaName), strings.ToLower(tableName), "", mysql.AllPrivMask) {
		return
	}

	req.AppendInt64(0, job.ID)
	req.AppendString(1, schemaName)
	req.AppendString(2, tableName)
	req.AppendString(3, job.Type.String())
	req.AppendString(4, job.SchemaState.String())
	req.AppendInt64(5, job.SchemaID)
	req.AppendInt64(6, job.TableID)
	req.AppendInt64(7, job.RowCount)
	req.AppendTime(8, createTime)
	if job.RealStartTS > 0 {
		req.AppendTime(9, startTime)
	} else {
		req.AppendNull(9)
	}
	if finishTS > 0 {
		req.AppendTime(10, finishTime)
	} else {
		req.AppendNull(10)
	}
	req.AppendString(11, job.State.String())
}

func ts2Time(timestamp uint64, loc *time.Location) types.Time {
	duration := time.Duration(math.Pow10(9-types.DefaultFsp)) * time.Nanosecond
	t := model.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t.In(loc)), mysql.TypeDatetime, types.DefaultFsp)
}

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs
type ShowDDLJobQueriesExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		return err
	}
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	if err != nil {
		return err
	}

	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				req.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobsExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.DDLJobRetriever.is = e.is
	if e.jobNumber == 0 {
		e.jobNumber = admin.DefNumHistoryJobs
	}
	err = e.DDLJobRetriever.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if (e.cursor - len(e.runningJobs)) >= e.jobNumber {
		return nil
	}
	count := 0

	// Append running ddl jobs.
	if e.cursor < len(e.runningJobs) {
		numCurBatch := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], nil)
		}
		e.cursor += numCurBatch
		count += numCurBatch
	}

	// Append history ddl jobs.
	var err error
	if count < req.Capacity() {
		num := req.Capacity() - count
		remainNum := e.jobNumber - (e.cursor - len(e.runningJobs))
		num = mathutil.Min(num, remainNum)
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(num, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, nil)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func getSchemaName(is infoschema.InfoSchema, id int64) string {
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = DBInfo.Name.O
		return schemaName
	}

	return schemaName
}

func getTableName(is infoschema.InfoSchema, id int64) string {
	var tableName string
	table, ok := is.TableByID(id)
	if ok {
		tableName = table.Meta().Name.O
		return tableName
	}

	return tableName
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

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

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	for _, src := range e.srcs {
		if err := src.Open(ctx); err != nil {
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
		if err := src.Close(); err != nil && firstErr == nil {
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
	cols := src.schema.Columns
	retFieldTypes := make([]*types.FieldType, len(cols))
	for i := range cols {
		retFieldTypes[i] = cols[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.initCap, e.maxChunkSize)

	var err error
	for {
		err = Next(ctx, src, chk)
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

func (e *CheckTableExec) handlePanic(r interface{}) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	idxNames := make([]string, 0, len(e.indexInfos))
	for _, idx := range e.indexInfos {
		idxNames = append(idxNames, idx.Name.O)
	}
	greater, idxOffset, err := admin.CheckIndicesCount(e.ctx, e.dbName, e.table.Meta().Name.O, idxNames)
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
		return e.checkIndexHandle(ctx, e.srcs[0])
	}
	taskCh := make(chan *IndexLookUpExecutor, len(e.srcs))
	failure := atomicutil.NewBool(false)
	concurrency := mathutil.Min(3, len(e.srcs))
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
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.table.Meta().GetPartitionInfo() == nil {
		idx := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		return admin.CheckRecordAndIndex(ctx, e.ctx, txn, e.table, idx)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx := tables.NewIndex(def.ID, e.table.Meta(), idxInfo)
		if err := admin.CheckRecordAndIndex(ctx, e.ctx, txn, partition, idx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ShowSlowExec represents the executor of showing the slow queries.
// It is build from the "admin show slow" statement:
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	baseExecutor

	ShowSlow *ast.ShowSlow
	result   []*domain.SlowQueryInfo
	cursor   int
}

// Open implements the Executor Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	dom := domain.GetDomain(e.ctx)
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowSlowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && req.NumRows() < e.maxChunkSize {
		slow := e.result[e.cursor]
		req.AppendString(0, slow.SQL)
		req.AppendTime(1, types.NewTime(types.FromGoTime(slow.Start), mysql.TypeTimestamp, types.MaxFsp))
		req.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		req.AppendString(3, slow.Detail.String())
		if slow.Succ {
			req.AppendInt64(4, 1)
		} else {
			req.AppendInt64(4, 0)
		}
		req.AppendUint64(5, slow.ConnID)
		req.AppendUint64(6, slow.TxnTS)
		req.AppendString(7, slow.User)
		req.AppendString(8, slow.DB)
		req.AppendString(9, slow.TableIDs)
		req.AppendString(10, slow.IndexNames)
		if slow.Internal {
			req.AppendInt64(11, 1)
		} else {
			req.AppendInt64(11, 0)
		}
		req.AppendString(12, slow.Digest)
		e.cursor++
	}
	return nil
}

// SelectLockExec represents a select lock executor.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Executor.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	baseExecutor

	Lock *ast.SelectLockInfo
	keys []kv.Key

	// The children may be a join of multiple tables, so we need a map.
	tblID2Handle map[int64][]plannercore.HandleCols

	// When SelectLock work on a partition table, we need the partition ID
	// (Physical Table ID) instead of the 'logical' table ID to calculate
	// the lock KV. In that case, the Physical Table ID is extracted
	// from the row key in the store and as an extra column in the chunk row.

	// tblID2PhyTblIDCol is used for partitioned tables.
	// The child executor need to return an extra column containing
	// the Physical Table ID (i.e. from which partition the row came from)
	// Used during building
	tblID2PhysTblIDCol map[int64]*expression.Column

	// Used during execution
	// Map from logic tableID to column index where the physical table id is stored
	// For dynamic prune mode, model.ExtraPhysTblID columns are requested from
	// storage and used for physical table id
	// For static prune mode, model.ExtraPhysTblID is still sent to storage/Protobuf
	// but could be filled in by the partitions TableReaderExecutor
	// due to issues with chunk handling between the TableReaderExecutor and the
	// SelectReader result.
	tblID2PhysTblIDColIdx map[int64]int
}

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if len(e.tblID2PhysTblIDCol) > 0 {
		e.tblID2PhysTblIDColIdx = make(map[int64]int)
		cols := e.Schema().Columns
		for i := len(cols) - 1; i >= 0; i-- {
			if cols[i].ID == model.ExtraPhysTblID {
				for tblID, col := range e.tblID2PhysTblIDCol {
					if cols[i].UniqueID == col.UniqueID {
						e.tblID2PhysTblIDColIdx[tblID] = i
						break
					}
				}
			}
		}
	}
	return e.baseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.tblID2Handle) == 0 || (!plannercore.IsSelectForUpdateLockType(e.Lock.LockType)) {
		return nil
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			for tblID, cols := range e.tblID2Handle {
				for _, col := range cols {
					handle, err := col.BuildHandle(row)
					if err != nil {
						return err
					}
					physTblID := tblID
					if physTblColIdx, ok := e.tblID2PhysTblIDColIdx[tblID]; ok {
						physTblID = row.GetInt64(physTblColIdx)
					}
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(physTblID, handle))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.ctx.GetSessionVars().LockWaitTimeout
	if e.Lock.LockType == ast.SelectLockForUpdateNoWait {
		lockWaitTime = tikvstore.LockNoWait
	} else if e.Lock.LockType == ast.SelectLockForUpdateWaitN {
		lockWaitTime = int64(e.Lock.WaitSec) * 1000
	}

	for id := range e.tblID2Handle {
		e.updateDeltaForTableID(id)
	}

	return doLockKeys(ctx, e.ctx, newLockCtx(e.ctx.GetSessionVars(), lockWaitTime, len(e.keys)), e.keys...)
}

func newLockCtx(seVars *variable.SessionVars, lockWaitTime int64, numKeys int) *tikvstore.LockCtx {
	lockCtx := tikvstore.NewLockCtx(seVars.TxnCtx.GetForUpdateTS(), lockWaitTime, seVars.StmtCtx.GetLockWaitStartTime())
	lockCtx.Killed = &seVars.Killed
	lockCtx.PessimisticLockWaited = &seVars.StmtCtx.PessimisticLockWaited
	lockCtx.LockKeysDuration = &seVars.StmtCtx.LockKeysDuration
	lockCtx.LockKeysCount = &seVars.StmtCtx.LockKeysCount
	lockCtx.LockExpired = &seVars.TxnCtx.LockExpire
	lockCtx.ResourceGroupTagger = func(req *kvrpcpb.PessimisticLockRequest) []byte {
		if req == nil {
			return nil
		}
		if len(req.Mutations) == 0 {
			return nil
		}
		if mutation := req.Mutations[0]; mutation != nil {
			label := resourcegrouptag.GetResourceGroupLabelByKey(mutation.Key)
			normalized, digest := seVars.StmtCtx.SQLDigest()
			if len(normalized) == 0 {
				return nil
			}
			_, planDigest := seVars.StmtCtx.GetPlanDigest()
			return resourcegrouptag.EncodeResourceGroupTag(digest, planDigest, label)
		}
		return nil
	}
	lockCtx.OnDeadlock = func(deadlock *tikverr.ErrDeadlock) {
		cfg := config.GetGlobalConfig()
		if deadlock.IsRetryable && !cfg.PessimisticTxn.DeadlockHistoryCollectRetryable {
			return
		}
		rec := deadlockhistory.ErrDeadlockToDeadlockRecord(deadlock)
		deadlockhistory.GlobalDeadlockHistory.Push(rec)
	}
	if lockCtx.ForUpdateTS > 0 && seVars.AssertionLevel != variable.AssertionLevelOff {
		lockCtx.InitCheckExistence(numKeys)
	}
	return lockCtx
}

// doLockKeys is the main entry for pessimistic lock keys
// waitTime means the lock operation will wait in milliseconds if target key is already
// locked by others. used for (select for update nowait) situation
func doLockKeys(ctx context.Context, se sessionctx.Context, lockCtx *tikvstore.LockCtx, keys ...kv.Key) error {
	sessVars := se.GetSessionVars()
	sctx := sessVars.StmtCtx
	if !sctx.InUpdateStmt && !sctx.InDeleteStmt {
		atomic.StoreUint32(&se.GetSessionVars().TxnCtx.ForUpdate, 1)
	}
	// Lock keys only once when finished fetching all results.
	txn, err := se.Txn(true)
	if err != nil {
		return err
	}

	// Skip the temporary table keys.
	keys = filterTemporaryTableKeys(sessVars, keys)

	keys = filterLockTableKeys(sessVars.StmtCtx, keys)
	var lockKeyStats *tikvutil.LockKeysDetails
	ctx = context.WithValue(ctx, tikvutil.LockKeysDetailCtxKey, &lockKeyStats)
	err = txn.LockKeys(tikvutil.SetSessionID(ctx, se.GetSessionVars().ConnectionID), lockCtx, keys...)
	if lockKeyStats != nil {
		sctx.MergeLockKeysExecDetails(lockKeyStats)
	}
	return err
}

func filterTemporaryTableKeys(vars *variable.SessionVars, keys []kv.Key) []kv.Key {
	txnCtx := vars.TxnCtx
	if txnCtx == nil || txnCtx.TemporaryTables == nil {
		return keys
	}

	newKeys := keys[:0:len(keys)]
	for _, key := range keys {
		tblID := tablecodec.DecodeTableID(key)
		if _, ok := txnCtx.TemporaryTables[tblID]; !ok {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func filterLockTableKeys(stmtCtx *stmtctx.StatementContext, keys []kv.Key) []kv.Key {
	if len(stmtCtx.LockTableIDs) == 0 {
		return keys
	}
	newKeys := keys[:0:len(keys)]
	for _, key := range keys {
		tblID := tablecodec.DecodeTableID(key)
		if _, ok := stmtCtx.LockTableIDs[tblID]; ok {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk

	// columnIdxsUsedByChild keep column indexes of child executor used for inline projection
	columnIdxsUsedByChild []int
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.maxChunkSize)
		err := Next(ctx, e.children[0], e.adjustRequiredRows(e.childResult))
		if err != nil {
			return err
		}
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			if e.columnIdxsUsedByChild != nil {
				req.Append(e.childResult.Prune(e.columnIdxsUsedByChild), int(begin), int(end))
			} else {
				req.Append(e.childResult, int(begin), int(end))
			}
			return nil
		}
		e.cursor += batchSize
	}
	e.childResult.Reset()
	e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.maxChunkSize)
	e.adjustRequiredRows(e.childResult)
	err := Next(ctx, e.children[0], e.childResult)
	if err != nil {
		return err
	}
	batchSize := uint64(e.childResult.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		e.childResult.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize

	if e.columnIdxsUsedByChild != nil {
		for i, childIdx := range e.columnIdxsUsedByChild {
			if err = req.SwapColumn(i, e.childResult, childIdx); err != nil {
				return err
			}
		}
	} else {
		req.SwapColumns(e.childResult)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.childResult = newFirstChunk(e.children[0])
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	e.childResult = nil
	return e.baseExecutor.Close()
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
		limitRequired = chk.RequiredRows()
	}

	return chk.SetRequiredRows(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubqueryFirstRow = func(ctx context.Context, p plannercore.PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) ([]types.Datum, error) {
		defer func(begin time.Time) {
			s := sctx.GetSessionVars()
			s.RewritePhaseInfo.PreprocessSubQueries++
			s.RewritePhaseInfo.DurationPreprocessSubQuery += time.Since(begin)
		}(time.Now())

		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("executor.EvalSubQuery", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}

		e := &executorBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return nil, e.err
		}
		err := exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return nil, err
		}
		chk := newFirstChunk(exec)

		err = Next(ctx, exec, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return nil, nil
		}
		row := chk.GetRow(0).GetDatumRow(retTypes(exec))
		return row, err
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		req.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	baseExecutor

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk

	memTracker *memory.Tracker
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	failpoint.Inject("mockSelectionExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock SelectionExec.baseExecutor.Open returned error"))
		}
	})
	return e.open(ctx)
}

func (e *SelectionExec) open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plannercore.Plan Close interface.
func (e *SelectionExec) Close() error {
	if e.childResult != nil {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
	}
	e.selected = nil
	return e.baseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)

	if !e.batched {
		return e.unBatchedNext(ctx, req)
	}

	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			if req.IsFull() {
				return nil
			}

			if !e.selected[e.inputRow.Idx()] {
				continue
			}

			req.AppendRow(e.inputRow)
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				return err
			}
			if selected {
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t                     table.Table
	columns               []*model.ColumnInfo
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	return e.nextChunk4InfoSchema(ctx, req)
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		type tableIter interface {
			IterRecords(sessionctx.Context, []*table.Column, table.RecordIterFunc) error
		}
		err := (e.t.(tableIter)).IterRecords(e.ctx, columns, func(_ kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.evaluated = false
	return nil
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}

	if num := req.NumRows(); num == 0 {
		for i := range e.schema.Columns {
			req.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return ErrSubqueryMoreThan1Row
	}

	childChunk := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumRows() != 0 {
		return ErrSubqueryMoreThan1Row
	}

	return nil
}

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionExec struct {
	baseExecutor
	concurrency int
	childIDChan chan int

	stopFetchData atomic.Value

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult

	results     []*chunk.Chunk
	wg          sync.WaitGroup
	initialized bool
	mu          struct {
		*sync.Mutex
		maxOpenedChildID int
	}

	childInFlightForTest int32
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	e.mu.Mutex = &sync.Mutex{}
	e.mu.maxOpenedChildID = -1
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	if e.concurrency > len(e.children) {
		e.concurrency = len(e.children)
	}
	for i := 0; i < e.concurrency; i++ {
		e.results = append(e.results, newFirstChunk(e.children[0]))
	}
	e.resultPool = make(chan *unionWorkerResult, e.concurrency)
	e.resourcePools = make([]chan *chunk.Chunk, e.concurrency)
	e.childIDChan = make(chan int, len(e.children))
	for i := 0; i < e.concurrency; i++ {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.results[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	for i := 0; i < len(e.children); i++ {
		e.childIDChan <- i
	}
	close(e.childIDChan)
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, workerID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[workerID],
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("resultPuller panicked", zap.String("stack", string(buf)))
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		e.wg.Done()
	}()
	for childID := range e.childIDChan {
		e.mu.Lock()
		if childID > e.mu.maxOpenedChildID {
			e.mu.maxOpenedChildID = childID
		}
		e.mu.Unlock()
		if err := e.children[childID].Open(ctx); err != nil {
			result.err = err
			e.stopFetchData.Store(true)
			e.resultPool <- result
		}
		failpoint.Inject("issue21441", func() {
			atomic.AddInt32(&e.childInFlightForTest, 1)
		})
		for {
			if e.stopFetchData.Load().(bool) {
				return
			}
			select {
			case <-e.finished:
				return
			case result.chk = <-e.resourcePools[workerID]:
			}
			result.err = Next(ctx, e.children[childID], result.chk)
			if result.err == nil && result.chk.NumRows() == 0 {
				e.resourcePools[workerID] <- result.chk
				break
			}
			failpoint.Inject("issue21441", func() {
				if int(atomic.LoadInt32(&e.childInFlightForTest)) > e.concurrency {
					panic("the count of child in flight is larger than e.concurrency unexpectedly")
				}
			})
			e.resultPool <- result
			if result.err != nil {
				e.stopFetchData.Store(true)
				return
			}
		}
		failpoint.Inject("issue21441", func() {
			atomic.AddInt32(&e.childInFlightForTest, -1)
		})
	}
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if !e.initialized {
		e.initialize(ctx)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	if result.chk.NumCols() != req.NumCols() {
		return errors.Errorf("Internal error: UnionExec chunk column count mismatch, req: %d, result: %d",
			req.NumCols(), result.chk.NumCols())
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.results = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	if e.childIDChan != nil {
		for range e.childIDChan {
		}
	}
	// We do not need to acquire the e.mu.Lock since all the resultPuller can be
	// promised to exit when reaching here (e.childIDChan been closed).
	var firstErr error
	for i := 0; i <= e.mu.maxOpenedChildID; i++ {
		if err := e.children[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	vars := ctx.GetSessionVars()
	var sc *stmtctx.StatementContext
	if vars.TxnCtx.CouldRetry {
		// Must construct new statement context object, the retry history need context for every statement.
		// TODO: Maybe one day we can get rid of transaction retry, then this logic can be deleted.
		sc = &stmtctx.StatementContext{}
	} else {
		sc = vars.InitStatementContext()
	}
	sc.TimeZone = vars.Location()
	sc.TaskID = stmtctx.AllocateTaskID()
	sc.CTEStorageMap = map[int]*CTEStorages{}
	sc.IsStaleness = false
	sc.LockTableIDs = make(map[int64]struct{})
	sc.EnableOptimizeTrace = false
	sc.OptimizeTracer = nil
	sc.OptimizerCETrace = nil

	sc.SysdateIsNow = ctx.GetSessionVars().SysdateIsNow

	sc.InitMemTracker(memory.LabelForSQLText, vars.MemQuotaQuery)
	sc.InitDiskTracker(memory.LabelForSQLText, -1)
	sc.MemTracker.AttachToGlobalTracker(GlobalMemoryUsageTracker)
	globalConfig := config.GetGlobalConfig()
	if globalConfig.OOMUseTmpStorage && GlobalDiskUsageTracker != nil {
		sc.DiskTracker.AttachToGlobalTracker(GlobalDiskUsageTracker)
	}
	switch globalConfig.OOMAction {
	case config.OOMActionCancel:
		action := &memory.PanicOnExceed{ConnID: ctx.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	case config.OOMActionLog:
		fallthrough
	default:
		action := &memory.LogOnExceed{ConnID: ctx.GetSessionVars().ConnectionID}
		action.SetLogHook(domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	}
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, vars)
		if err != nil {
			return err
		}
		s = prepareStmt.PreparedAst.Stmt
		sc.InitSQLDigest(prepareStmt.NormalizedSQL, prepareStmt.SQLDigest)
		// For `execute stmt` SQL, should reset the SQL digest with the prepare SQL digest.
		goCtx := context.Background()
		if variable.EnablePProfSQLCPU.Load() && len(prepareStmt.NormalizedSQL) > 0 {
			goCtx = pprof.WithLabels(goCtx, pprof.Labels("sql", util.QueryStrForLog(prepareStmt.NormalizedSQL)))
			pprof.SetGoroutineLabels(goCtx)
		}
		if topsqlstate.TopSQLEnabled() && prepareStmt.SQLDigest != nil {
			topsql.AttachSQLInfo(goCtx, prepareStmt.NormalizedSQL, prepareStmt.SQLDigest, "", nil, vars.InRestrictedSQL)
		}
		if s, ok := prepareStmt.PreparedAst.Stmt.(*ast.SelectStmt); ok {
			if s.LockInfo == nil {
				sc.WeakConsistency = isWeakConsistencyRead(ctx, execStmt)
			}
		}
	}
	// execute missed stmtID uses empty sql
	sc.OriginalSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.InExplainStmt = true
		sc.IgnoreExplainIDSuffix = strings.ToLower(explainStmt.Format) == types.ExplainFormatBrief
		sc.InVerboseExplain = strings.ToLower(explainStmt.Format) == types.ExplainFormatVerbose
		s = explainStmt.Stmt
	}
	if explainForStmt, ok := s.(*ast.ExplainForStmt); ok {
		sc.InExplainStmt = true
		sc.InVerboseExplain = strings.ToLower(explainForStmt.Format) == types.ExplainFormatVerbose
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.
	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		ResetUpdateStmtCtx(sc, stmt, vars)
	case *ast.DeleteStmt:
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() || !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning.
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.IgnoreNoPartition = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() || !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		sc.InCreateOrAlterStmt = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.SQLMode.HasNoZeroInDateMode() || !vars.StrictSQLMode || sc.AllowInvalidDate
		sc.NoZeroDate = vars.SQLMode.HasNoZeroDateMode()
		sc.TruncateAsWarning = !vars.StrictSQLMode
	case *ast.LoadDataStmt:
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		// With IGNORE or LOCAL, data-interpretation errors become warnings and the load operation continues,
		// even if the SQL mode is restrictive. For details: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
		// TODO: since TiDB only support the LOCAL by now, so the TruncateAsWarning are always true here.
		sc.TruncateAsWarning = true
		sc.InLoadDataStmt = true
		// return warning instead of error when load data meet no partition for value
		sc.IgnoreNoPartition = true
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
		// said "For statements such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.WeakConsistency = isWeakConsistencyRead(ctx, stmt)
		// Try to mark the `RCCheckTS` flag for the first time execution of in-transaction read requests
		// using read-consistency isolation level.
		if NeedSetRCCheckTSFlag(ctx, stmt) {
			sc.RCCheckTS = true
		}
	case *ast.SetOprStmt:
		sc.InSelectStmt = true
		sc.OverflowAsWarning = true
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	case *ast.ShowStmt:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.IgnoreTruncate = false
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	default:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	}
	sc.SkipUTF8Check = vars.SkipUTF8Check
	sc.SkipASCIICheck = vars.SkipASCIICheck
	sc.SkipUTF8MB4Check = !globalConfig.CheckMb4ValueInUTF8.Load()
	vars.PreparedParams = vars.PreparedParams[:0]
	if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
		sc.Priority = priority
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt {
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedRows = -1
	}
	if globalConfig.EnableCollectExecutionInfo {
		// In ExplainFor case, RuntimeStatsColl should not be reset for reuse,
		// because ExplainFor need to display the last statement information.
		reuseObj := vars.StmtCtx.RuntimeStatsColl
		if _, ok := s.(*ast.ExplainForStmt); ok {
			reuseObj = nil
		}
		sc.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(reuseObj)
	}

	sc.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.StmtCtx = sc
	vars.PrevFoundInPlanCache = vars.FoundInPlanCache
	vars.FoundInPlanCache = false
	vars.ClearStmtVars()
	vars.PrevFoundInBinding = vars.FoundInBinding
	vars.FoundInBinding = false
	return
}

// ResetUpdateStmtCtx resets statement context for UpdateStmt.
func ResetUpdateStmtCtx(sc *stmtctx.StatementContext, stmt *ast.UpdateStmt, vars *variable.SessionVars) {
	sc.InUpdateStmt = true
	sc.DupKeyAsWarning = stmt.IgnoreErr
	sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	sc.IgnoreZeroInDate = !vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() || !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
	sc.Priority = stmt.Priority
	sc.IgnoreNoPartition = stmt.IgnoreErr
}

// FillVirtualColumnValue will calculate the virtual column value by evaluating generated
// expression using rows from a chunk, and then fill this value into the chunk
func FillVirtualColumnValue(virtualRetTypes []*types.FieldType, virtualColumnIndex []int,
	schema *expression.Schema, columns []*model.ColumnInfo, sctx sessionctx.Context, req *chunk.Chunk) error {
	virCols := chunk.NewChunkWithCapacity(virtualRetTypes, req.Capacity())
	iter := chunk.NewIterator4Chunk(req)
	for i, idx := range virtualColumnIndex {
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			datum, err := schema.Columns[idx].EvalVirtualColumn(row)
			if err != nil {
				return err
			}
			// Because the expression might return different type from
			// the generated column, we should wrap a CAST on the result.
			castDatum, err := table.CastValue(sctx, datum, columns[idx], false, true)
			if err != nil {
				return err
			}
			// Handle the bad null error.
			if (mysql.HasNotNullFlag(columns[idx].Flag) || mysql.HasPreventNullInsertFlag(columns[idx].Flag)) && castDatum.IsNull() {
				castDatum = table.GetZeroValue(columns[idx])
			}
			virCols.AppendDatum(i, &castDatum)
		}
		req.SetCol(idx, virCols.Column(i))
	}
	return nil
}

func setResourceGroupTaggerForTxn(sc *stmtctx.StatementContext, snapshot kv.Snapshot) {
	if snapshot != nil && topsqlstate.TopSQLEnabled() {
		snapshot.SetOption(kv.ResourceGroupTagger, sc.GetResourceGroupTagger())
	}
}

// setRPCInterceptorOfExecCounterForTxn binds an interceptor for client-go to count
// the number of SQL executions of each TiKV.
func setRPCInterceptorOfExecCounterForTxn(vars *variable.SessionVars, snapshot kv.Snapshot) {
	if snapshot != nil && topsqlstate.TopSQLEnabled() && vars.StmtCtx.KvExecCounter != nil {
		snapshot.SetOption(kv.RPCInterceptor, vars.StmtCtx.KvExecCounter.RPCInterceptor())
	}
}

func isWeakConsistencyRead(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	return sessionVars.ConnectionID > 0 && sessionVars.ReadConsistency.IsWeak() &&
		plannercore.IsAutoCommitTxn(ctx) && plannercore.IsReadOnly(node, sessionVars)
}

// NeedSetRCCheckTSFlag checks whether it's needed to set `RCCheckTS` flag in current stmtctx.
func NeedSetRCCheckTSFlag(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	if sessionVars.ConnectionID > 0 && sessionVars.RcReadCheckTS && sessionVars.InTxn() &&
		sessionVars.IsPessimisticReadConsistency() && !sessionVars.RetryInfo.Retrying && plannercore.IsReadOnly(node, sessionVars) {
		return true
	}
	return false
}
