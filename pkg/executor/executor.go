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
	"cmp"
	"context"
	stderrors "errors"
	"fmt"
	"math"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/pdhelper"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/admin"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	tikvutil "github.com/tikv/client-go/v2/util"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &CheckTableExec{}
	_ exec.Executor = &aggregate.HashAggExec{}
	_ exec.Executor = &IndexLookUpExecutor{}
	_ exec.Executor = &IndexReaderExecutor{}
	_ exec.Executor = &LimitExec{}
	_ exec.Executor = &MaxOneRowExec{}
	_ exec.Executor = &ProjectionExec{}
	_ exec.Executor = &SelectionExec{}
	_ exec.Executor = &SelectLockExec{}
	_ exec.Executor = &ShowNextRowIDExec{}
	_ exec.Executor = &ShowDDLExec{}
	_ exec.Executor = &ShowDDLJobsExec{}
	_ exec.Executor = &ShowDDLJobQueriesExec{}
	_ exec.Executor = &sortexec.SortExec{}
	_ exec.Executor = &aggregate.StreamAggExec{}
	_ exec.Executor = &TableDualExec{}
	_ exec.Executor = &TableReaderExecutor{}
	_ exec.Executor = &TableScanExec{}
	_ exec.Executor = &sortexec.TopNExec{}
	_ exec.Executor = &FastCheckTableExec{}
	_ exec.Executor = &AdminShowBDRRoleExec{}

	// GlobalMemoryUsageTracker is the ancestor of all the Executors' memory tracker and GlobalMemory Tracker
	GlobalMemoryUsageTracker *memory.Tracker
	// GlobalDiskUsageTracker is the ancestor of all the Executors' disk tracker
	GlobalDiskUsageTracker *disk.Tracker
	// GlobalAnalyzeMemoryTracker is the ancestor of all the Analyze jobs' memory tracker and child of global Tracker
	GlobalAnalyzeMemoryTracker *memory.Tracker
)

var (
	_ dataSourceExecutor = &TableReaderExecutor{}
	_ dataSourceExecutor = &IndexReaderExecutor{}
	_ dataSourceExecutor = &IndexLookUpExecutor{}
	_ dataSourceExecutor = &IndexMergeReaderExecutor{}

	// CheckTableFastBucketSize is the bucket size of fast check table.
	CheckTableFastBucketSize = atomic.Int64{}
)

// dataSourceExecutor is a table DataSource converted Executor.
// Currently, there are TableReader/IndexReader/IndexLookUp/IndexMergeReader.
// Note, partition reader is special and the caller should handle it carefully.
type dataSourceExecutor interface {
	exec.Executor
	Table() table.Table
}

const (
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Quota For Local Temporary Space!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
	// globalPanicAnalyzeMemoryExceed represents the panic message when out of analyze memory limit.
	globalPanicAnalyzeMemoryExceed string = "Out Of Global Analyze Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type globalPanicOnExceed struct {
	memory.BaseOOMAction
	mutex syncutil.Mutex // For synchronization.
}

func init() {
	action := &globalPanicOnExceed{}
	GlobalMemoryUsageTracker = memory.NewGlobalTracker(memory.LabelForGlobalMemory, -1)
	GlobalMemoryUsageTracker.SetActionOnExceed(action)
	GlobalDiskUsageTracker = disk.NewGlobalTrcaker(memory.LabelForGlobalStorage, -1)
	GlobalDiskUsageTracker.SetActionOnExceed(action)
	GlobalAnalyzeMemoryTracker = memory.NewTracker(memory.LabelForGlobalAnalyzeMemory, -1)
	GlobalAnalyzeMemoryTracker.SetActionOnExceed(action)
	// register quota funcs
	variable.SetMemQuotaAnalyze = GlobalAnalyzeMemoryTracker.SetBytesLimit
	variable.GetMemQuotaAnalyze = GlobalAnalyzeMemoryTracker.GetBytesLimit
	// TODO: do not attach now to avoid impact to global, will attach later when analyze memory track is stable
	//GlobalAnalyzeMemoryTracker.AttachToGlobalTracker(GlobalMemoryUsageTracker)

	schematracker.ConstructResultOfShowCreateDatabase = ConstructResultOfShowCreateDatabase
	schematracker.ConstructResultOfShowCreateTable = ConstructResultOfShowCreateTable

	// CheckTableFastBucketSize is used to set the fast analyze bucket size for check table.
	CheckTableFastBucketSize.Store(1024)
}

// Start the backend components
func Start() {
	pdhelper.GlobalPDHelper.Start()
}

// Stop the backend components
func Stop() {
	pdhelper.GlobalPDHelper.Stop()
}

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
	case memory.LabelForGlobalAnalyzeMemory:
		msg = globalPanicAnalyzeMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	// TODO(hawkingrei): should return error instead.
	panic(msg)
}

// GetPriority get the priority of the Action
func (*globalPanicOnExceed) GetPriority() int64 {
	return memory.DefPanicPriority
}

// CommandDDLJobsExec is the general struct for Cancel/Pause/Resume commands on
// DDL jobs. These command currently by admin have the very similar struct and
// operations, it should be a better idea to have them in the same struct.
type CommandDDLJobsExec struct {
	exec.BaseExecutor

	cursor int
	jobIDs []int64
	errs   []error

	execute func(se sessionctx.Context, ids []int64) (errs []error, err error)
}

// Open implements the Executor for all Cancel/Pause/Resume command on DDL jobs
// just with different processes. And, it should not be called directly by the
// Executor.
func (e *CommandDDLJobsExec) Open(context.Context) error {
	// We want to use a global transaction to execute the admin command, so we don't use e.Ctx() here.
	newSess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	e.errs, err = e.execute(newSess, e.jobIDs)
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), newSess)
	return err
}

// Next implements the Executor Next interface for Cancel/Pause/Resume
func (e *CommandDDLJobsExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, strconv.FormatInt(e.jobIDs[i], 10))
		if e.errs != nil && e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	*CommandDDLJobsExec
}

// PauseDDLJobsExec indicates an Executor for Pause a DDL Job.
type PauseDDLJobsExec struct {
	*CommandDDLJobsExec
}

// ResumeDDLJobsExec indicates an Executor for Resume a DDL Job.
type ResumeDDLJobsExec struct {
	*CommandDDLJobsExec
}

// ShowNextRowIDExec represents a show the next row ID executor.
type ShowNextRowIDExec struct {
	exec.BaseExecutor
	tblName *ast.TableName
	done    bool
}

// Next implements the Executor Next interface.
func (e *ShowNextRowIDExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	tbl, err := is.TableByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return err
	}
	tblMeta := tbl.Meta()

	allocators := tbl.Allocators(e.Ctx().GetTableCtx())
	for _, alloc := range allocators.Allocs {
		nextGlobalID, err := alloc.NextGlobalAutoID()
		if err != nil {
			return err
		}

		var colName, idType string
		switch alloc.GetType() {
		case autoid.RowIDAllocType:
			idType = "_TIDB_ROWID"
			if tblMeta.PKIsHandle {
				if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
					colName = col.Name.O
				}
			} else {
				colName = model.ExtraHandleName.O
			}
		case autoid.AutoIncrementType:
			idType = "AUTO_INCREMENT"
			if tblMeta.PKIsHandle {
				if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
					colName = col.Name.O
				}
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
	exec.BaseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *ddl.Info
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
	exec.BaseExecutor
	DDLJobRetriever

	jobNumber int
	is        infoschema.InfoSchema
	sess      sessionctx.Context
}

// DDLJobRetriever retrieve the DDLJobs.
// nolint:structcheck
type DDLJobRetriever struct {
	runningJobs    []*model.Job
	historyJobIter meta.LastJobIterator
	cursor         int
	is             infoschema.InfoSchema
	activeRoles    []*auth.RoleIdentity
	cacheJobs      []*model.Job
	TZLoc          *time.Location
}

func (e *DDLJobRetriever) initial(txn kv.Transaction, sess sessionctx.Context) error {
	m := meta.NewMeta(txn)
	jobs, err := ddl.GetAllDDLJobs(sess)
	if err != nil {
		return err
	}
	e.historyJobIter, err = ddl.GetLastHistoryDDLJobsIterator(m)
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
	if len(tableName) == 0 {
		tableName = job.TableName
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
	req.AppendString(3, job.Type.String()+showAddIdxReorgTp(job))
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
	if job.Type == model.ActionMultiSchemaChange {
		isDistTask := job.ReorgMeta != nil && job.ReorgMeta.IsDistReorg
		for _, subJob := range job.MultiSchemaInfo.SubJobs {
			req.AppendInt64(0, job.ID)
			req.AppendString(1, schemaName)
			req.AppendString(2, tableName)
			req.AppendString(3, subJob.Type.String()+" /* subjob */"+showAddIdxReorgTpInSubJob(subJob, isDistTask))
			req.AppendString(4, subJob.SchemaState.String())
			req.AppendInt64(5, job.SchemaID)
			req.AppendInt64(6, job.TableID)
			req.AppendInt64(7, subJob.RowCount)
			req.AppendTime(8, createTime)
			if subJob.RealStartTS > 0 {
				realStartTS := ts2Time(subJob.RealStartTS, e.TZLoc)
				req.AppendTime(9, realStartTS)
			} else {
				req.AppendNull(9)
			}
			if finishTS > 0 {
				req.AppendTime(10, finishTime)
			} else {
				req.AppendNull(10)
			}
			req.AppendString(11, subJob.State.String())
		}
	}
}

func showAddIdxReorgTp(job *model.Job) string {
	if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
		if job.ReorgMeta != nil {
			sb := strings.Builder{}
			tp := job.ReorgMeta.ReorgTp.String()
			if len(tp) > 0 {
				sb.WriteString(" /* ")
				sb.WriteString(tp)
				if job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge &&
					job.ReorgMeta.IsDistReorg &&
					job.ReorgMeta.UseCloudStorage {
					sb.WriteString(" cloud")
				}
				sb.WriteString(" */")
			}
			return sb.String()
		}
	}
	return ""
}

func showAddIdxReorgTpInSubJob(subJob *model.SubJob, useDistTask bool) string {
	if subJob.Type == model.ActionAddIndex || subJob.Type == model.ActionAddPrimaryKey {
		sb := strings.Builder{}
		tp := subJob.ReorgTp.String()
		if len(tp) > 0 {
			sb.WriteString(" /* ")
			sb.WriteString(tp)
			if subJob.ReorgTp == model.ReorgTypeLitMerge && useDistTask && subJob.UseCloud {
				sb.WriteString(" cloud")
			}
			sb.WriteString(" */")
		}
		return sb.String()
	}
	return ""
}

func ts2Time(timestamp uint64, loc *time.Location) types.Time {
	duration := time.Duration(math.Pow10(9-types.DefaultFsp)) * time.Nanosecond
	t := model.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t.In(loc)), mysql.TypeDatetime, types.MaxFsp)
}

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs.
type ShowDDLJobQueriesExec struct {
	exec.BaseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	var err error
	var jobs []*model.Job
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	session, err := e.GetSysSession()
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(context.Background(), session)
	if err != nil {
		return err
	}
	defer func() {
		// ReleaseSysSession will rollbacks txn automatically.
		e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), session)
	}()
	txn, err := session.Txn(true)
	if err != nil {
		return err
	}
	session.GetSessionVars().SetInTxn(true)

	m := meta.NewMeta(txn)
	jobs, err = ddl.GetAllDDLJobs(session)
	if err != nil {
		return err
	}

	historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, ddl.DefNumHistoryJobs)
	if err != nil {
		return err
	}

	appendedJobID := make(map[int64]struct{})
	// deduplicate job results
	// for situations when this operation happens at the same time with new DDLs being executed
	for _, job := range jobs {
		if _, ok := appendedJobID[job.ID]; !ok {
			appendedJobID[job.ID] = struct{}{}
			e.jobs = append(e.jobs, job)
		}
	}
	for _, historyJob := range historyJobs {
		if _, ok := appendedJobID[historyJob.ID]; !ok {
			appendedJobID[historyJob.ID] = struct{}{}
			e.jobs = append(e.jobs, historyJob)
		}
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobs)-e.cursor)
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

// ShowDDLJobQueriesWithRangeExec represents a show DDL job queries with range executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// can be searched within a specified range in history jobs using offset and limit.
type ShowDDLJobQueriesWithRangeExec struct {
	exec.BaseExecutor

	cursor int
	jobs   []*model.Job
	offset uint64
	limit  uint64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesWithRangeExec) Open(ctx context.Context) error {
	var err error
	var jobs []*model.Job
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	session, err := e.GetSysSession()
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(context.Background(), session)
	if err != nil {
		return err
	}
	defer func() {
		// ReleaseSysSession will rollbacks txn automatically.
		e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), session)
	}()
	txn, err := session.Txn(true)
	if err != nil {
		return err
	}
	session.GetSessionVars().SetInTxn(true)

	m := meta.NewMeta(txn)
	jobs, err = ddl.GetAllDDLJobs(session)
	if err != nil {
		return err
	}

	historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, int(e.offset+e.limit))
	if err != nil {
		return err
	}

	appendedJobID := make(map[int64]struct{})
	// deduplicate job results
	// for situations when this operation happens at the same time with new DDLs being executed
	for _, job := range jobs {
		if _, ok := appendedJobID[job.ID]; !ok {
			appendedJobID[job.ID] = struct{}{}
			e.jobs = append(e.jobs, job)
		}
	}
	for _, historyJob := range historyJobs {
		if _, ok := appendedJobID[historyJob.ID]; !ok {
			appendedJobID[historyJob.ID] = struct{}{}
			e.jobs = append(e.jobs, historyJob)
		}
	}

	if e.cursor < int(e.offset) {
		e.cursor = int(e.offset)
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesWithRangeExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if int(e.offset) > len(e.jobs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		// i is make true to be >= int(e.offset)
		if i >= int(e.offset+e.limit) {
			break
		}
		req.AppendString(0, strconv.FormatInt(e.jobs[i].ID, 10))
		req.AppendString(1, e.jobs[i].Query)
	}
	e.cursor += numCurBatch
	return nil
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobsExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.DDLJobRetriever.is = e.is
	if e.jobNumber == 0 {
		e.jobNumber = ddl.DefNumHistoryJobs
	}
	sess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	e.sess = sess
	err = sessiontxn.NewTxn(context.Background(), sess)
	if err != nil {
		return err
	}
	txn, err := sess.Txn(true)
	if err != nil {
		return err
	}
	sess.GetSessionVars().SetInTxn(true)
	err = e.DDLJobRetriever.initial(txn, sess)
	return err
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if (e.cursor - len(e.runningJobs)) >= e.jobNumber {
		return nil
	}
	count := 0

	// Append running ddl jobs.
	if e.cursor < len(e.runningJobs) {
		numCurBatch := min(req.Capacity(), len(e.runningJobs)-e.cursor)
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
		num = min(num, remainNum)
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

// Close implements the Executor Close interface.
func (e *ShowDDLJobsExec) Close() error {
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), e.sess)
	return e.BaseExecutor.Close()
}

func getSchemaName(is infoschema.InfoSchema, id int64) string {
	var schemaName string
	dbInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = dbInfo.Name.O
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
		if idx.MVIndex {
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

// ShowSlowExec represents the executor of showing the slow queries.
// It is build from the "admin show slow" statement:
//
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	exec.BaseExecutor

	ShowSlow *ast.ShowSlow
	result   []*domain.SlowQueryInfo
	cursor   int
}

// Open implements the Executor Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	dom := domain.GetDomain(e.Ctx())
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowSlowExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && req.NumRows() < e.MaxChunkSize() {
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
		req.AppendString(13, slow.SessAlias)
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
	exec.BaseExecutor

	Lock *ast.SelectLockInfo
	keys []kv.Key

	// The children may be a join of multiple tables, so we need a map.
	tblID2Handle map[int64][]plannerutil.HandleCols

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
	return e.BaseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	err := exec.Next(ctx, e.Children(0), req)
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
						if physTblID == 0 {
							// select * from t1 left join t2 on t1.c = t2.c for update
							// The join right side might be added NULL in left join
							// In that case, physTblID is 0, so skip adding the lock.
							//
							// Note, we can't distinguish whether it's the left join case,
							// or a bug that TiKV return without correct physical ID column.
							continue
						}
					}
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(physTblID, handle))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.Ctx().GetSessionVars().LockWaitTimeout
	if e.Lock.LockType == ast.SelectLockForUpdateNoWait {
		lockWaitTime = tikvstore.LockNoWait
	} else if e.Lock.LockType == ast.SelectLockForUpdateWaitN {
		lockWaitTime = int64(e.Lock.WaitSec) * 1000
	}

	for id := range e.tblID2Handle {
		e.UpdateDeltaForTableID(id)
	}
	lockCtx, err := newLockCtx(e.Ctx(), lockWaitTime, len(e.keys))
	if err != nil {
		return err
	}
	return doLockKeys(ctx, e.Ctx(), lockCtx, e.keys...)
}

func newLockCtx(sctx sessionctx.Context, lockWaitTime int64, numKeys int) (*tikvstore.LockCtx, error) {
	seVars := sctx.GetSessionVars()
	forUpdateTS, err := sessiontxn.GetTxnManager(sctx).GetStmtForUpdateTS()
	if err != nil {
		return nil, err
	}
	lockCtx := tikvstore.NewLockCtx(forUpdateTS, lockWaitTime, seVars.StmtCtx.GetLockWaitStartTime())
	lockCtx.Killed = &seVars.SQLKiller.Signal
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
	return lockCtx, nil
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
	exec.BaseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk

	// columnIdxsUsedByChild keep column indexes of child executor used for inline projection
	columnIdxsUsedByChild []int

	// Log the close time when opentracing is enabled.
	span opentracing.Span
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.MaxChunkSize())
		err := exec.Next(ctx, e.Children(0), e.adjustRequiredRows(e.childResult))
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
	e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.MaxChunkSize())
	e.adjustRequiredRows(e.childResult)
	err := exec.Next(ctx, e.Children(0), e.childResult)
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
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		e.span = span
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	start := time.Now()

	e.childResult = nil
	err := e.BaseExecutor.Close()

	elapsed := time.Since(start)
	if elapsed > time.Millisecond {
		logutil.BgLogger().Info("limit executor close takes a long time",
			zap.Duration("elapsed", elapsed))
		if e.span != nil {
			span1 := e.span.Tracer().StartSpan("limitExec.Close", opentracing.ChildOf(e.span.Context()), opentracing.StartTime(start))
			defer span1.Finish()
		}
	}
	return err
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

	return chk.SetRequiredRows(min(limitTotal, limitRequired), e.MaxChunkSize())
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubqueryFirstRow = func(ctx context.Context, p base.PhysicalPlan, is infoschema.InfoSchema, pctx planctx.PlanContext) ([]types.Datum, error) {
		defer func(begin time.Time) {
			s := pctx.GetSessionVars()
			s.StmtCtx.SetSkipPlanCache("query has uncorrelated sub-queries is un-cacheable")
			s.RewritePhaseInfo.PreprocessSubQueries++
			s.RewritePhaseInfo.DurationPreprocessSubQuery += time.Since(begin)
		}(time.Now())

		r, ctx := tracing.StartRegionEx(ctx, "executor.EvalSubQuery")
		defer r.End()

		sctx, err := plannercore.AsSctx(pctx)
		intest.AssertNoError(err)
		if err != nil {
			return nil, err
		}

		e := newExecutorBuilder(sctx, is)
		executor := e.build(p)
		if e.err != nil {
			return nil, e.err
		}
		err = exec.Open(ctx, executor)
		defer func() { terror.Log(exec.Close(executor)) }()
		if err != nil {
			return nil, err
		}
		if pi, ok := sctx.(processinfoSetter); ok {
			// Before executing the sub-query, we need update the processinfo to make the progress bar more accurate.
			// because the sub-query may take a long time.
			pi.UpdateProcessInfo()
		}
		chk := exec.TryNewCacheChunk(executor)
		err = exec.Next(ctx, executor, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return nil, nil
		}
		row := chk.GetRow(0).GetDatumRow(exec.RetTypes(executor))
		return row, err
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	exec.BaseExecutorV2

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(_ context.Context, req *chunk.Chunk) error {
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
	exec.BaseExecutor

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
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	failpoint.Inject("mockSelectionExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock SelectionExec.baseExecutor.Open returned error"))
		}
	})
	return e.open(ctx)
}

func (e *SelectionExec) open(context.Context) error {
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
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
	return e.BaseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())

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
		err := exec.Next(ctx, e.Children(0), e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.Ctx().GetExprCtx().GetEvalCtx(), e.Ctx().GetSessionVars().EnableVectorizedExpression, e.filters, e.inputIter, e.selected)
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
	exprCtx := e.Ctx().GetExprCtx()
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(exprCtx.GetEvalCtx(), e.filters, e.inputRow)
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
		err := exec.Next(ctx, e.Children(0), e.childResult)
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
	exec.BaseExecutor

	t                     table.Table
	columns               []*model.ColumnInfo
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	return e.nextChunk4InfoSchema(ctx, req)
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.MaxChunkSize())
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(exec.RetTypes(e), e.InitCap(), e.MaxChunkSize())
		columns := make([]*table.Column, e.Schema().Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(exec.RetTypes(e))
		type tableIter interface {
			IterRecords(ctx context.Context, sctx sessionctx.Context, cols []*table.Column, fn table.RecordIterFunc) error
		}
		err := (e.t.(tableIter)).IterRecords(ctx, e.Ctx(), columns, func(_ kv.Handle, rec []types.Datum, _ []*table.Column) (bool, error) {
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
func (e *TableScanExec) Open(context.Context) error {
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	exec.BaseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
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
	err := exec.Next(ctx, e.Children(0), req)
	if err != nil {
		return err
	}

	if num := req.NumRows(); num == 0 {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return exeerrors.ErrSubqueryMoreThan1Row
	}

	childChunk := exec.TryNewCacheChunk(e.Children(0))
	err = exec.Next(ctx, e.Children(0), childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumRows() != 0 {
		return exeerrors.ErrSubqueryMoreThan1Row
	}

	return nil
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("ResetContextOfStmt panicked", zap.Stack("stack"), zap.Any("recover", r), zap.Error(err))
			if err != nil {
				err = stderrors.Join(err, util.GetRecoverError(r))
			} else {
				err = util.GetRecoverError(r)
			}
		}
	}()
	vars := ctx.GetSessionVars()
	for name, val := range vars.StmtCtx.SetVarHintRestore {
		err := vars.SetSystemVar(name, val)
		if err != nil {
			logutil.BgLogger().Warn("Failed to restore the variable after SET_VAR hint", zap.String("variable name", name), zap.String("expected value", val))
		}
	}
	vars.StmtCtx.SetVarHintRestore = nil
	var sc *stmtctx.StatementContext
	if vars.TxnCtx.CouldRetry || vars.HasStatusFlag(mysql.ServerStatusCursorExists) {
		// Must construct new statement context object, the retry history need context for every statement.
		// TODO: Maybe one day we can get rid of transaction retry, then this logic can be deleted.
		sc = stmtctx.NewStmtCtx()
	} else {
		sc = vars.InitStatementContext()
	}
	sc.SetTimeZone(vars.Location())
	sc.TaskID = stmtctx.AllocateTaskID()
	sc.CTEStorageMap = map[int]*CTEStorages{}
	sc.IsStaleness = false
	sc.LockTableIDs = make(map[int64]struct{})
	sc.EnableOptimizeTrace = false
	sc.OptimizeTracer = nil
	sc.OptimizerCETrace = nil
	sc.IsSyncStatsFailed = false
	sc.IsExplainAnalyzeDML = false
	sc.ResourceGroupName = vars.ResourceGroupName
	// Firstly we assume that UseDynamicPruneMode can be enabled according session variable, then we will check other conditions
	// in PlanBuilder.buildDataSource
	if ctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
		sc.UseDynamicPruneMode = true
	} else {
		sc.UseDynamicPruneMode = false
	}

	sc.StatsLoad.Timeout = 0
	sc.StatsLoad.NeededItems = nil
	sc.StatsLoad.ResultCh = nil

	sc.SysdateIsNow = ctx.GetSessionVars().SysdateIsNow

	vars.MemTracker.Detach()
	vars.MemTracker.UnbindActions()
	vars.MemTracker.SetBytesLimit(vars.MemQuotaQuery)
	vars.MemTracker.ResetMaxConsumed()
	vars.DiskTracker.Detach()
	vars.DiskTracker.ResetMaxConsumed()
	vars.MemTracker.SessionID.Store(vars.ConnectionID)
	vars.MemTracker.Killer = &vars.SQLKiller
	vars.DiskTracker.Killer = &vars.SQLKiller
	vars.SQLKiller.Reset()
	vars.SQLKiller.ConnID = vars.ConnectionID
	vars.StmtCtx.TableStats = make(map[int64]any)
	sc.MDLRelatedTableIDs = make(map[int64]struct{})

	isAnalyze := false
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, vars)
		if err != nil {
			return err
		}
		_, isAnalyze = prepareStmt.PreparedAst.Stmt.(*ast.AnalyzeTableStmt)
	} else if _, ok := s.(*ast.AnalyzeTableStmt); ok {
		isAnalyze = true
	}
	if isAnalyze {
		sc.InitMemTracker(memory.LabelForAnalyzeMemory, -1)
		vars.MemTracker.SetBytesLimit(-1)
		vars.MemTracker.AttachTo(GlobalAnalyzeMemoryTracker)
	} else {
		sc.InitMemTracker(memory.LabelForSQLText, -1)
	}
	logOnQueryExceedMemQuota := domain.GetDomain(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota
	switch variable.OOMAction.Load() {
	case variable.OOMActionCancel:
		action := &memory.PanicOnExceed{ConnID: vars.ConnectionID, Killer: vars.MemTracker.Killer}
		action.SetLogHook(logOnQueryExceedMemQuota)
		vars.MemTracker.SetActionOnExceed(action)
	case variable.OOMActionLog:
		fallthrough
	default:
		action := &memory.LogOnExceed{ConnID: vars.ConnectionID}
		action.SetLogHook(logOnQueryExceedMemQuota)
		vars.MemTracker.SetActionOnExceed(action)
	}
	sc.MemTracker.SessionID.Store(vars.ConnectionID)
	sc.MemTracker.AttachTo(vars.MemTracker)
	sc.InitDiskTracker(memory.LabelForSQLText, -1)
	globalConfig := config.GetGlobalConfig()
	if variable.EnableTmpStorageOnOOM.Load() && sc.DiskTracker != nil {
		sc.DiskTracker.AttachTo(vars.DiskTracker)
		if GlobalDiskUsageTracker != nil {
			vars.DiskTracker.AttachTo(GlobalDiskUsageTracker)
		}
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
			goCtx = pprof.WithLabels(goCtx, pprof.Labels("sql", FormatSQL(prepareStmt.NormalizedSQL).String()))
			pprof.SetGoroutineLabels(goCtx)
		}
		if topsqlstate.TopSQLEnabled() && prepareStmt.SQLDigest != nil {
			sc.IsSQLRegistered.Store(true)
			topsql.AttachAndRegisterSQLInfo(goCtx, prepareStmt.NormalizedSQL, prepareStmt.SQLDigest, vars.InRestrictedSQL)
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
		sc.ExplainFormat = explainStmt.Format
		sc.InExplainAnalyzeStmt = explainStmt.Analyze
		sc.IgnoreExplainIDSuffix = strings.ToLower(explainStmt.Format) == types.ExplainFormatBrief
		sc.InVerboseExplain = strings.ToLower(explainStmt.Format) == types.ExplainFormatVerbose
		s = explainStmt.Stmt
	} else {
		sc.ExplainFormat = ""
	}
	if explainForStmt, ok := s.(*ast.ExplainForStmt); ok {
		sc.InExplainStmt = true
		sc.InExplainAnalyzeStmt = true
		sc.InVerboseExplain = strings.ToLower(explainForStmt.Format) == types.ExplainFormatVerbose
	}

	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.

	sc.InRestrictedSQL = vars.InRestrictedSQL
	strictSQLMode := vars.SQLMode.HasStrictMode()

	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
	switch stmt := s.(type) {
	// `ResetUpdateStmtCtx` and `ResetDeleteStmtCtx` may modify the flags, so we'll need to store them.
	case *ast.UpdateStmt:
		ResetUpdateStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.DeleteStmt:
		ResetDeleteStmtCtx(sc, stmt, vars)
		errLevels = sc.ErrLevels()
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning.
		if stmt.IgnoreErr {
			errLevels[errctx.ErrGroupDupKey] = errctx.LevelWarn
			errLevels[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelWarn
			errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
		}
		errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
		errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
			!vars.SQLMode.HasErrorForDivisionByZeroMode(),
			!strictSQLMode || stmt.IgnoreErr,
		)
		sc.Priority = stmt.Priority
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() ||
				!vars.SQLMode.HasNoZeroDateMode() || !strictSQLMode || stmt.IgnoreErr ||
				vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		sc.InCreateOrAlterStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(!strictSQLMode).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !strictSQLMode ||
				vars.SQLMode.HasAllowInvalidDatesMode()).
			WithIgnoreZeroDateErr(!vars.SQLMode.HasNoZeroDateMode() || !strictSQLMode))

	case *ast.LoadDataStmt:
		sc.InLoadDataStmt = true
		// return warning instead of error when load data meet no partition for value
		errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.LevelWarn
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// Return warning for truncate error in selection.
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.WeakConsistency = isWeakConsistencyRead(ctx, stmt)
	case *ast.SetOprStmt:
		sc.InSelectStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithTruncateAsWarning(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.ShowStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors || stmt.Tp == ast.ShowSessionStates {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(false).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	case *ast.SetSessionStatesStmt:
		sc.InSetSessionStatesStmt = true
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	default:
		sc.SetTypeFlags(sc.TypeFlags().
			WithIgnoreTruncateErr(true).
			WithIgnoreZeroInDate(true).
			WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()))
	}

	if errLevels != sc.ErrLevels() {
		sc.SetErrLevels(errLevels)
	}

	sc.SetTypeFlags(sc.TypeFlags().
		WithSkipUTF8Check(vars.SkipUTF8Check).
		WithSkipSACIICheck(vars.SkipASCIICheck).
		WithSkipUTF8MB4Check(!globalConfig.Instance.CheckMb4ValueInUTF8.Load()).
		// WithAllowNegativeToUnsigned with false value indicates values less than 0 should be clipped to 0 for unsigned integer types.
		// This is the case for `insert`, `update`, `alter table`, `create table` and `load data infile` statements, when not in strict SQL mode.
		// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		WithAllowNegativeToUnsigned(!sc.InInsertStmt && !sc.InLoadDataStmt && !sc.InUpdateStmt && !sc.InCreateOrAlterStmt),
	)

	vars.PlanCacheParams.Reset()
	if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
		sc.Priority = priority
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt || vars.StmtCtx.InSetSessionStatesStmt {
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedRows = -1
	}
	if globalConfig.Instance.EnableCollectExecutionInfo.Load() {
		// In ExplainFor case, RuntimeStatsColl should not be reset for reuse,
		// because ExplainFor need to display the last statement information.
		reuseObj := vars.StmtCtx.RuntimeStatsColl
		if _, ok := s.(*ast.ExplainForStmt); ok {
			reuseObj = nil
		}
		sc.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(reuseObj)

		// also enable index usage collector
		if sc.IndexUsageCollector == nil {
			sc.IndexUsageCollector = ctx.NewStmtIndexUsageCollector()
		} else {
			sc.IndexUsageCollector.Reset()
		}
	} else {
		// turn off the index usage collector
		sc.IndexUsageCollector = nil
	}

	sc.SetForcePlanCache(fixcontrol.GetBoolWithDefault(vars.OptimizerFixControl, fixcontrol.Fix49736, false))
	sc.SetAlwaysWarnSkipCache(sc.InExplainStmt && sc.ExplainFormat == "plan_cache")
	sc.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.ExchangeChunkStatus()
	vars.StmtCtx = sc
	vars.PrevFoundInPlanCache = vars.FoundInPlanCache
	vars.FoundInPlanCache = false
	vars.PrevFoundInBinding = vars.FoundInBinding
	vars.FoundInBinding = false
	vars.DurationWaitTS = 0
	vars.CurrInsertBatchExtraCols = nil
	vars.CurrInsertValues = chunk.Row{}

	return
}

// ResetUpdateStmtCtx resets statement context for UpdateStmt.
func ResetUpdateStmtCtx(sc *stmtctx.StatementContext, stmt *ast.UpdateStmt, vars *variable.SessionVars) {
	strictSQLMode := vars.SQLMode.HasStrictMode()
	sc.InUpdateStmt = true
	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDupKey] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
	errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
		!vars.SQLMode.HasErrorForDivisionByZeroMode(),
		!strictSQLMode || stmt.IgnoreErr,
	)
	errLevels[errctx.ErrGroupNoMatchedPartition] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	sc.SetErrLevels(errLevels)
	sc.Priority = stmt.Priority
	sc.SetTypeFlags(sc.TypeFlags().
		WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() ||
			!strictSQLMode || stmt.IgnoreErr || vars.SQLMode.HasAllowInvalidDatesMode()))
}

// ResetDeleteStmtCtx resets statement context for DeleteStmt.
func ResetDeleteStmtCtx(sc *stmtctx.StatementContext, stmt *ast.DeleteStmt, vars *variable.SessionVars) {
	strictSQLMode := vars.SQLMode.HasStrictMode()
	sc.InDeleteStmt = true
	errLevels := sc.ErrLevels()
	errLevels[errctx.ErrGroupDupKey] = errctx.ResolveErrLevel(false, stmt.IgnoreErr)
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !strictSQLMode || stmt.IgnoreErr)
	errLevels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(
		!vars.SQLMode.HasErrorForDivisionByZeroMode(),
		!strictSQLMode || stmt.IgnoreErr,
	)
	sc.SetErrLevels(errLevels)
	sc.Priority = stmt.Priority
	sc.SetTypeFlags(sc.TypeFlags().
		WithTruncateAsWarning(!strictSQLMode || stmt.IgnoreErr).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!vars.SQLMode.HasNoZeroInDateMode() || !vars.SQLMode.HasNoZeroDateMode() ||
			!strictSQLMode || stmt.IgnoreErr || vars.SQLMode.HasAllowInvalidDatesMode()))
}

func setOptionForTopSQL(sc *stmtctx.StatementContext, snapshot kv.Snapshot) {
	if snapshot == nil {
		return
	}
	// pipelined dml may already flush in background, don't touch it to avoid race.
	if txn, ok := snapshot.(kv.Transaction); ok && txn.IsPipelined() {
		return
	}
	snapshot.SetOption(kv.ResourceGroupTagger, sc.GetResourceGroupTagger())
	if sc.KvExecCounter != nil {
		snapshot.SetOption(kv.RPCInterceptor, sc.KvExecCounter.RPCInterceptor())
	}
}

func isWeakConsistencyRead(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	return sessionVars.ConnectionID > 0 && sessionVars.ReadConsistency.IsWeak() &&
		plannercore.IsAutoCommitTxn(sessionVars) && plannercore.IsReadOnly(node, sessionVars)
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

// Open implements the Executor Open interface.
func (e *FastCheckTableExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	e.done = false
	e.contextCtx = ctx
	return nil
}

type checkIndexTask struct {
	indexOffset int
}

type checkIndexWorker struct {
	sctx       sessionctx.Context
	dbName     string
	table      table.Table
	indexInfos []*model.IndexInfo
	e          *FastCheckTableExec
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
				Tbl:  w.table.Meta(),
				Idx:  idxInfo,
				Sctx: w.sctx,
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

func (e *FastCheckTableExec) createWorker() workerpool.Worker[checkIndexTask, workerpool.None] {
	return &checkIndexWorker{sctx: e.Ctx(), dbName: e.dbName, table: e.table, indexInfos: e.indexInfos, e: e}
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
		workerPool.AddTask(checkIndexTask{indexOffset: i})
	}

	e.wg.Wait()
	workerPool.ReleaseAndWait()

	p := e.err.Load()
	if p == nil {
		return nil
	}
	return *p
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

// AdminShowBDRRoleExec represents a show BDR role executor.
type AdminShowBDRRoleExec struct {
	exec.BaseExecutor

	done bool
}

// Next implements the Executor Next interface.
func (e *AdminShowBDRRoleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	return kv.RunInNewTxn(kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin), e.Ctx().GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		role, err := meta.NewMeta(txn).GetBDRRole()
		if err != nil {
			return err
		}

		req.AppendString(0, role)
		e.done = true
		return nil
	})
}
