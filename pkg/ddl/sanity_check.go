// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
)

func (d *ddl) checkDeleteRangeCnt(job *model.Job) {
	actualCnt, err := queryDeleteRangeCnt(d.sessPool, job.ID)
	if err != nil {
		if strings.Contains(err.Error(), "Not Supported") {
			return // For mock session, we don't support executing SQLs.
		}
		logutil.DDLLogger().Error("query delete range count failed", zap.Error(err))
		panic(err)
	}
	expectedCnt, err := expectedDeleteRangeCnt(delRangeCntCtx{idxIDs: map[int64]struct{}{}}, job)
	if err != nil {
		logutil.DDLLogger().Error("decode job's delete range count failed", zap.Error(err))
		panic(err)
	}
	if actualCnt != expectedCnt {
		panic(fmt.Sprintf("expect delete range count %d, actual count %d", expectedCnt, actualCnt))
	}
}

func queryDeleteRangeCnt(sessPool *sess.Pool, jobID int64) (int, error) {
	sctx, _ := sessPool.Get()
	s := sctx.GetSQLExecutor()
	defer func() {
		sessPool.Put(sctx)
	}()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	query := `select sum(cnt) from
	(select count(1) cnt from mysql.gc_delete_range where job_id = %? union all
	select count(1) cnt from mysql.gc_delete_range_done where job_id = %?) as gdr;`
	rs, err := s.ExecuteInternal(ctx, query, jobID, jobID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer func() {
		_ = rs.Close()
	}()
	req := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	cnt, _ := req.GetRow(0).GetMyDecimal(0).ToInt()
	return int(cnt), nil
}

func expectedDeleteRangeCnt(ctx delRangeCntCtx, job *model.Job) (int, error) {
	if job.State == model.JobStateCancelled {
		// Cancelled job should not have any delete range.
		return 0, nil
	}
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return 0, errors.Trace(err)
		}
		return len(tableIDs), nil
	case model.ActionDropTable, model.ActionTruncateTable:
		var startKey kv.Key
		var physicalTableIDs []int64
		var ruleIDs []string
		if err := job.DecodeArgs(&startKey, &physicalTableIDs, &ruleIDs); err != nil {
			return 0, errors.Trace(err)
		}
		return len(physicalTableIDs) + 1, nil
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition,
		model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		var physicalTableIDs []int64
		if err := job.DecodeArgs(&physicalTableIDs); err != nil {
			return 0, errors.Trace(err)
		}
		return len(physicalTableIDs), nil
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		indexID := make([]int64, 1)
		ifExists := make([]bool, 1)
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID[0], &ifExists[0], &partitionIDs); err != nil {
			if err := job.DecodeArgs(&indexID, &ifExists, &partitionIDs); err != nil {
				var unique bool
				if err := job.DecodeArgs(&unique); err == nil {
					// The first argument is bool means nothing need to be added to delete-range table.
					return 0, nil
				}
				return 0, errors.Trace(err)
			}
		}
		idxIDNumFactor := len(indexID) // Add temporary index to del-range table.
		if job.State == model.JobStateRollbackDone {
			idxIDNumFactor = 2 * len(indexID) // Add origin index to del-range table.
		}
		return mathutil.Max(len(partitionIDs)*idxIDNumFactor, idxIDNumFactor), nil
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		var indexName any
		ifNotExists := make([]bool, 1)
		indexID := make([]int64, 1)
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &ifNotExists[0], &indexID[0], &partitionIDs); err != nil {
			if err := job.DecodeArgs(&indexName, &ifNotExists, &indexID, &partitionIDs); err != nil {
				return 0, errors.Trace(err)
			}
		}
		return mathutil.Max(len(partitionIDs), 1), nil
	case model.ActionDropColumn:
		var colName model.CIStr
		var ifExists bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colName, &ifExists, &indexIDs, &partitionIDs); err != nil {
			return 0, errors.Trace(err)
		}
		physicalCnt := mathutil.Max(len(partitionIDs), 1)
		return physicalCnt * len(indexIDs), nil
	case model.ActionModifyColumn:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexIDs, &partitionIDs); err != nil {
			return 0, errors.Trace(err)
		}
		physicalCnt := mathutil.Max(len(partitionIDs), 1)
		return physicalCnt * ctx.deduplicateIdxCnt(indexIDs), nil
	case model.ActionMultiSchemaChange:
		totalExpectedCnt := 0
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			p := sub.ToProxyJob(job, i)
			cnt, err := expectedDeleteRangeCnt(ctx, &p)
			if err != nil {
				return 0, err
			}
			totalExpectedCnt += cnt
		}
		return totalExpectedCnt, nil
	}
	return 0, nil
}

type delRangeCntCtx struct {
	idxIDs map[int64]struct{}
}

func (ctx *delRangeCntCtx) deduplicateIdxCnt(indexIDs []int64) int {
	cnt := 0
	for _, id := range indexIDs {
		if _, ok := ctx.idxIDs[id]; !ok {
			ctx.idxIDs[id] = struct{}{}
			cnt++
		}
	}
	return cnt
}

// checkHistoryJobInTest does some sanity check to make sure something is correct after DDL complete.
// It's only check during the test environment, so it would panic directly.
// These checks may be controlled by configuration in the future.
func (d *ddl) checkHistoryJobInTest(ctx sessionctx.Context, historyJob *model.Job) {
	if !intest.InTest {
		return
	}

	// Check delete range.
	if JobNeedGC(historyJob) {
		d.checkDeleteRangeCnt(historyJob)
	}

	// Check binlog.
	if historyJob.BinlogInfo.FinishedTS == 0 {
		panic(fmt.Sprintf("job ID %d, BinlogInfo.FinishedTS is 0", historyJob.ID))
	}

	// Check DDL query.
	switch historyJob.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		if historyJob.Query != "" {
			panic(fmt.Sprintf("job ID %d, type %s, query %s", historyJob.ID, historyJob.Type.String(), historyJob.Query))
		}
		return
	default:
		if historyJob.Query == "skip" {
			// Skip the check if the test explicitly set the query.
			return
		}
	}
	p := parser.New()
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	p.SetParserConfig(ctx.GetSessionVars().BuildParserConfig())
	stmt, _, err := p.ParseSQL(historyJob.Query)
	if err != nil {
		panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s, err %s", historyJob.ID, historyJob.Query, err.Error()))
	}
	if len(stmt) != 1 && historyJob.Type != model.ActionCreateTables {
		panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
	}
	for _, st := range stmt {
		switch historyJob.Type {
		case model.ActionCreatePlacementPolicy:
			if _, ok := st.(*ast.CreatePlacementPolicyStmt); !ok {
				panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
			}
		case model.ActionCreateTable:
			if _, ok := st.(*ast.CreateTableStmt); !ok {
				panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
			}
		case model.ActionCreateSchema:
			if _, ok := st.(*ast.CreateDatabaseStmt); !ok {
				panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
			}
		case model.ActionCreateTables:
			_, isCreateTable := st.(*ast.CreateTableStmt)
			_, isCreateSeq := st.(*ast.CreateSequenceStmt)
			_, isCreateView := st.(*ast.CreateViewStmt)
			if !isCreateTable && !isCreateSeq && !isCreateView {
				panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
			}
		default:
			if _, ok := st.(ast.DDLNode); !ok {
				panic(fmt.Sprintf("job ID %d, parse ddl job failed, query %s", historyJob.ID, historyJob.Query))
			}
		}
	}
}
