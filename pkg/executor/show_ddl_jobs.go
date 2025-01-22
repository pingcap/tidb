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
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
)

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	exec.BaseExecutor
	DDLJobRetriever

	jobNumber int
	is        infoschema.InfoSchema
	sess      sessionctx.Context
}

var _ exec.Executor = &ShowDDLJobsExec{}

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
			e.appendJobToChunk(req, e.runningJobs[i], nil, true)
		}
		e.cursor += numCurBatch
		count += numCurBatch
	}

	// Append history ddl jobs.
	var err error
	if count < req.Capacity() && e.historyJobIter != nil {
		num := req.Capacity() - count
		remainNum := e.jobNumber - (e.cursor - len(e.runningJobs))
		num = min(num, remainNum)
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(num, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, nil, true)
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
	extractor      base.MemTablePredicateExtractor
}

func (e *DDLJobRetriever) initial(txn kv.Transaction, sess sessionctx.Context) error {
	skipRunningJobs := false
	skipHistoryJobs := false
	schemaNames := set.NewStringSet()
	tableNames := set.NewStringSet()

	ex, ok := e.extractor.(*plannercore.InfoSchemaDDLExtractor)
	if ok {
		// Using state to determine whether we can skip checking running/history ddl jobs
		if states, ok := ex.ColPredicates["state"]; ok {
			skipHistoryJobs = true
			skipRunningJobs = true
			states.IterateWith(func(s string) {
				ss := strings.ToLower(s)
				if ss == "cancelled" || ss == "synced" {
					skipHistoryJobs = false
				} else {
					skipRunningJobs = false
				}
			})
		}

		schemaNames = ex.ColPredicates["db_name"]
		tableNames = ex.ColPredicates["table_name"]
	}

	var err error

	if !skipRunningJobs {
		// We cannot use table_id and schema_id to construct predicates for the tidb_ddl_job table.
		// For instance, in the case of the SQL like `create table t(id int)`,
		// the tableInfo for 't' will not be available in the infoschema until the job is completed.
		// As a result, we cannot retrieve its table_id.
		e.runningJobs, err = ddl.GetAllDDLJobs(context.Background(), sess)
		if err != nil {
			return err
		}
	}

	if !skipHistoryJobs {
		// For the similar reason, we can only use schema_name and table_name to do filtering here.
		m := meta.NewMutator(txn)
		e.historyJobIter, err = m.GetLastHistoryDDLJobsIteratorWithFilter(schemaNames, tableNames)
		if err != nil {
			return err
		}
	}

	e.cursor = 0
	return nil
}

func (e *DDLJobRetriever) appendJobToChunk(req *chunk.Chunk, job *model.Job, checker privilege.Manager, inShowStmt bool) {
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
	if job.Type == model.ActionMultiSchemaChange {
		var useDXF, isCloud bool
		if job.ReorgMeta != nil {
			useDXF = job.ReorgMeta.IsDistReorg
			isCloud = job.ReorgMeta.UseCloudStorage
		}
		for _, subJob := range job.MultiSchemaInfo.SubJobs {
			req.AppendInt64(0, job.ID)
			req.AppendString(1, schemaName)
			req.AppendString(2, tableName)
			req.AppendString(3, subJob.Type.String()+" /* subjob */")
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
			if inShowStmt {
				req.AppendString(12, showCommentsFromSubjob(subJob, useDXF, isCloud))
			} else {
				req.AppendString(12, job.Query)
			}
		}
	}
	if inShowStmt {
		req.AppendString(12, showCommentsFromJob(job))
	} else {
		req.AppendString(12, job.Query)
	}
}

func showCommentsFromJob(job *model.Job) string {
	m := job.ReorgMeta
	if m == nil {
		return ""
	}
	var labels []string
	if job.Type == model.ActionAddIndex ||
		job.Type == model.ActionAddPrimaryKey {
		switch m.ReorgTp {
		case model.ReorgTypeTxn:
			labels = append(labels, model.ReorgTypeTxn.String())
		case model.ReorgTypeLitMerge:
			labels = append(labels, model.ReorgTypeLitMerge.String())
			if m.IsDistReorg {
				labels = append(labels, "DXF")
			}
			if m.UseCloudStorage {
				labels = append(labels, "cloud")
			}
		case model.ReorgTypeTxnMerge:
			labels = append(labels, model.ReorgTypeTxnMerge.String())
		}
	}
	if job.MayNeedReorg() {
		concurrency := m.GetConcurrencyOrDefault(int(variable.GetDDLReorgWorkerCounter()))
		batchSize := m.GetBatchSizeOrDefault(int(variable.GetDDLReorgBatchSize()))
		maxWriteSpeed := m.GetMaxWriteSpeedOrDefault()
		if concurrency != variable.DefTiDBDDLReorgWorkerCount {
			labels = append(labels, fmt.Sprintf("thread=%d", concurrency))
		}
		if batchSize != variable.DefTiDBDDLReorgBatchSize {
			labels = append(labels, fmt.Sprintf("batch_size=%d", batchSize))
		}
		if maxWriteSpeed != variable.DefTiDBDDLReorgMaxWriteSpeed {
			labels = append(labels, fmt.Sprintf("max_write_speed=%d", maxWriteSpeed))
		}
		if m.TargetScope != "" {
			labels = append(labels, fmt.Sprintf("service_scope=%s", m.TargetScope))
		}
	}
	return strings.Join(labels, ", ")
}

func showCommentsFromSubjob(sub *model.SubJob, useDXF, useCloud bool) string {
	var labels []string
	if sub.ReorgTp == model.ReorgTypeNone {
		return ""
	}
	labels = append(labels, sub.ReorgTp.String())
	if useDXF {
		labels = append(labels, "DXF")
	}
	if useDXF && useCloud {
		labels = append(labels, "cloud")
	}
	return strings.Join(labels, ", ")
}

func ts2Time(timestamp uint64, loc *time.Location) types.Time {
	duration := time.Duration(math.Pow10(9-types.DefaultFsp)) * time.Nanosecond
	t := model.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t.In(loc)), mysql.TypeDatetime, types.MaxFsp)
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
	table, ok := is.TableByID(context.Background(), id)
	if ok {
		tableName = table.Meta().Name.O
		return tableName
	}

	return tableName
}
