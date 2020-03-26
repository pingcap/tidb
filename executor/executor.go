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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
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
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	_ Executor = &baseExecutor{}
	_ Executor = &CheckIndexExec{}
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
)

type baseExecutor struct {
	ctx           sessionctx.Context
	id            fmt.Stringer
	schema        *expression.Schema // output schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.RuntimeStats
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

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id fmt.Stringer, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().InitChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id != nil {
			e.runtimeStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.id.String())
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
	if atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0) {
		return ErrQueryInterrupted
	}
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("%T.Next", e), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return e.Next(ctx, req)
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
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
	colName := model.ExtraHandleName
	for _, col := range tbl.Meta().Columns {
		if mysql.HasAutoIncrementFlag(col.Flag) {
			colName = col.Name
			break
		}
	}
	nextGlobalID, err := tbl.Allocators(e.ctx).Get(autoid.RowIDAllocType).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		return err
	}
	req.AppendString(0, e.tblName.Schema.O)
	req.AppendString(1, e.tblName.Name.O)
	req.AppendString(2, colName.O)
	req.AppendInt64(3, nextGlobalID)
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
	done      bool
}

// DDLJobRetriever retrieve the DDLJobs.
type DDLJobRetriever struct {
	runningJobs    []*model.Job
	historyJobIter *meta.LastJobIterator
	cursor         int
	is             infoschema.InfoSchema
	activeRoles    []*auth.RoleIdentity
	cacheJobs      []*model.Job
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

	startTime := ts2Time(job.StartTS)
	finishTime := ts2Time(finishTS)

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
	req.AppendTime(8, startTime)
	if finishTS > 0 {
		req.AppendTime(9, finishTime)
	} else {
		req.AppendNull(9)
	}
	req.AppendString(10, job.State.String())
}

func ts2Time(timestamp uint64) types.Time {
	duration := time.Duration(math.Pow10(9-int(types.DefaultFsp))) * time.Nanosecond
	t := model.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.DefaultFsp)
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
			break
		}
		if chk.NumRows() == 0 {
			break
		}

		select {
		case <-e.exitCh:
			return nil
		default:
		}
	}
	e.retCh <- errors.Trace(err)
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
		if greater == admin.IdxCntGreater {
			err = e.checkTableIndexHandle(ctx, e.indexInfos[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkTableRecord(idxOffset)
		}
		if err != nil && admin.ErrDataInConsistent.Equal(err) {
			return ErrAdminCheckTable.GenWithStack("%v err:%v", e.table.Meta().Name, err)
		}
		return errors.Trace(err)
	}

	// The number of table rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjustable. And we can consider the number of records.
	concurrency := 3
	wg := sync.WaitGroup{}
	for i := range e.srcs {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			util.WithRecovery(func() {
				err1 := e.checkIndexHandle(ctx, e.srcs[num])
				if err1 != nil {
					logutil.Logger(ctx).Info("check index handle failed", zap.Error(err))
				}
			}, e.handlePanic)
		}(i)

		if (i+1)%concurrency == 0 {
			wg.Wait()
		}
	}

	for i := 0; i < len(e.srcs); i++ {
		err = <-e.retCh
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *CheckTableExec) checkTableRecord(idxOffset int) error {
	idxInfo := e.indexInfos[idxOffset]
	// TODO: Fix me later, can not use genExprs in indexLookUpReader, because the schema of expression is different.
	genExprs := e.srcs[idxOffset].genExprs
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.table.Meta().GetPartitionInfo() == nil {
		idx := tables.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		return admin.CheckRecordAndIndex(e.ctx, txn, e.table, idx, genExprs)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx := tables.NewIndex(def.ID, e.table.Meta(), idxInfo)
		if err := admin.CheckRecordAndIndex(e.ctx, txn, partition, idx, genExprs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CheckIndexExec represents the executor of checking an index.
// It is built from the "admin check index" statement, and it checks
// the consistency of the index data with the records of the table.
type CheckIndexExec struct {
	baseExecutor

	dbName    string
	tableName string
	idxName   string
	src       *IndexLookUpExecutor
	done      bool
	is        infoschema.InfoSchema
}

// Open implements the Executor Open interface.
func (e *CheckIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	if err := e.src.Open(ctx); err != nil {
		return err
	}
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckIndexExec) Close() error {
	return e.src.Close()
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()

	_, _, err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tableName, []string{e.idxName})
	if err != nil {
		return err
	}
	chk := newFirstChunk(e.src)
	for {
		err := Next(ctx, e.src, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
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

	Lock ast.SelectLockType
	keys []kv.Key

	tblID2Handle     map[int64][]*expression.Column
	partitionedTable []table.PartitionedTable

	// tblID2Table is cached to reduce cost.
	tblID2Table map[int64]table.PartitionedTable
}

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	txnCtx := e.ctx.GetSessionVars().TxnCtx
	for id := range e.tblID2Handle {
		// This operation is only for schema validator check.
		txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
	}

	if len(e.tblID2Handle) > 0 && len(e.partitionedTable) > 0 {
		e.tblID2Table = make(map[int64]table.PartitionedTable, len(e.partitionedTable))
		for id := range e.tblID2Handle {
			for _, p := range e.partitionedTable {
				if id == p.Meta().ID {
					e.tblID2Table[id] = p
				}
			}
		}
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.tblID2Handle) == 0 || (e.Lock != ast.SelectLockForUpdate && e.Lock != ast.SelectLockForUpdateNoWait) {
		return nil
	}

	if req.NumRows() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			for id, cols := range e.tblID2Handle {
				physicalID := id
				if pt, ok := e.tblID2Table[id]; ok {
					// On a partitioned table, we have to use physical ID to encode the lock key!
					p, err := pt.GetPartitionByRow(e.ctx, row.GetDatumRow(e.base().retFieldTypes))
					if err != nil {
						return err
					}
					physicalID = p.GetPhysicalID()
				}

				for _, col := range cols {
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(physicalID, row.GetInt64(col.Index)))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.ctx.GetSessionVars().LockWaitTimeout
	if e.Lock == ast.SelectLockForUpdateNoWait {
		lockWaitTime = kv.LockNoWait
	}
	return doLockKeys(ctx, e.ctx, newLockCtx(e.ctx.GetSessionVars(), lockWaitTime), e.keys...)
}

func newLockCtx(seVars *variable.SessionVars, lockWaitTime int64) *kv.LockCtx {
	return &kv.LockCtx{
		Killed:                &seVars.Killed,
		ForUpdateTS:           seVars.TxnCtx.GetForUpdateTS(),
		LockWaitTime:          lockWaitTime,
		WaitStartTime:         seVars.StmtCtx.GetLockWaitStartTime(),
		PessimisticLockWaited: &seVars.StmtCtx.PessimisticLockWaited,
		LockKeysDuration:      &seVars.StmtCtx.LockKeysDuration,
		LockKeysCount:         &seVars.StmtCtx.LockKeysCount,
	}
}

// doLockKeys is the main entry for pessimistic lock keys
// waitTime means the lock operation will wait in milliseconds if target key is already
// locked by others. used for (select for update nowait) situation
// except 0 means alwaysWait 1 means nowait
func doLockKeys(ctx context.Context, se sessionctx.Context, lockCtx *kv.LockCtx, keys ...kv.Key) error {
	sctx := se.GetSessionVars().StmtCtx
	if !sctx.InUpdateStmt && !sctx.InDeleteStmt {
		se.GetSessionVars().TxnCtx.ForUpdate = true
	}
	// Lock keys only once when finished fetching all results.
	txn, err := se.Txn(true)
	if err != nil {
		return err
	}
	return txn.LockKeys(sessionctx.SetCtxConnID(ctx, se), lockCtx, keys...)
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
			req.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	e.adjustRequiredRows(req)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	batchSize := uint64(req.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		req.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
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
	plannercore.EvalSubquery = func(ctx context.Context, p plannercore.PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) (rows [][]types.Datum, err error) {
		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("executor.EvalSubQuery", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}

		e := &executorBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, e.err
		}
		err = exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return rows, err
		}
		chk := newFirstChunk(exec)
		for {
			err = Next(ctx, exec, chk)
			if err != nil {
				return rows, err
			}
			if chk.NumRows() == 0 {
				return rows, nil
			}
			iter := chunk.NewIterator4Chunk(chk)
			for r := iter.Begin(); r != iter.End(); r = iter.Next() {
				row := r.GetDatumRow(retTypes(exec))
				rows = append(rows, row)
			}
			chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
		}
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
	e.memTracker.Consume(-e.childResult.MemoryUsage())
	e.childResult = nil
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
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if req.IsFull() {
				return nil
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
	seekHandle            int64
	iter                  kv.Iterator
	columns               []*model.ColumnInfo
	isVirtualTable        bool
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.isVirtualTable {
		return e.nextChunk4InfoSchema(ctx, req)
	}
	handle, found, err := e.nextHandle()
	if err != nil || !found {
		return err
	}

	mutableRow := chunk.MutRowFromTypes(retTypes(e))
	for req.NumRows() < req.Capacity() {
		row, err := e.getRow(handle)
		if err != nil {
			return err
		}
		e.seekHandle = handle + 1
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	return nil
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
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
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

// nextHandle gets the unique handle for next row.
func (e *TableScanExec) nextHandle() (handle int64, found bool, err error) {
	handle, found, err = e.t.Seek(e.ctx, e.seekHandle)
	if err != nil || !found {
		return 0, false, err
	}
	return handle, true, nil
}

func (e *TableScanExec) getRow(handle int64) ([]types.Datum, error) {
	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		columns[i] = table.ToColumn(v)
	}
	row, err := e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		return nil, err
	}

	return row, nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	e.iter = nil
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
		return errors.New("subquery returns more than 1 row")
	}

	childChunk := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumRows() != 0 {
		return errors.New("subquery returns more than 1 row")
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

	stopFetchData atomic.Value

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult

	childrenResults []*chunk.Chunk
	wg              sync.WaitGroup
	initialized     bool
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
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	e.resultPool = make(chan *unionWorkerResult, len(e.children))
	e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
	for i := range e.children {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.childrenResults[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, childID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
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
	for {
		if e.stopFetchData.Load().(bool) {
			return
		}
		select {
		case <-e.finished:
			return
		case result.chk = <-e.resourcePools[childID]:
		}
		result.err = Next(ctx, e.children[childID], result.chk)
		if result.err == nil && result.chk.NumRows() == 0 {
			return
		}
		e.resultPool <- result
		if result.err != nil {
			e.stopFetchData.Store(true)
			return
		}
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

	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.childrenResults = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	return e.baseExecutor.Close()
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	vars := ctx.GetSessionVars()
	sc := &stmtctx.StatementContext{
		TimeZone:    vars.Location(),
		MemTracker:  memory.NewTracker(stringutil.MemoizeStr(s.Text), vars.MemQuotaQuery),
		DiskTracker: disk.NewTracker(stringutil.MemoizeStr(s.Text), -1),
	}
	switch config.GetGlobalConfig().OOMAction {
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
		s, err = getPreparedStmt(execStmt, vars)
		if err != nil {
			return
		}
	}
	// execute missed stmtID uses empty sql
	sc.OriginalSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.InExplainStmt = true
		s = explainStmt.Stmt
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.
	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		sc.InUpdateStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.DeleteStmt:
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning or BadNullAsWarning,
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		// Make sure the sql_mode is strict when checking column default value.
	case *ast.LoadDataStmt:
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		sc.TruncateAsWarning = !vars.StrictSQLMode
		sc.InLoadDataStmt = true
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
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.StmtCtx = sc
	return
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
			castDatum, err := table.CastValue(sctx, datum, columns[idx])
			if err != nil {
				return err
			}
			virCols.AppendDatum(i, &castDatum)
		}
		req.SetCol(idx, virCols.Column(i))
	}
	return nil
}
