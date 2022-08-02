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
	"flag"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

func checkRangeCntByTableIDs(physicalTableIDs []int64, cnt int64) {
	if len(physicalTableIDs) > 0 {
		if len(physicalTableIDs) != int(cnt) {
			panic("should not happened" + fmt.Sprintf("expect count: %d, real count: %d", len(physicalTableIDs), cnt))
		}
	} else if cnt != 1 {
		panic("should not happened" + fmt.Sprintf("expect count: %d, real count: %d", 1, cnt))
	}
}

func checkRangeCntByTableIDsAndIndexIDs(partitionTableIDs []int64, indexIDs []int64, cnt int64) {
	if len(indexIDs) == 0 {
		return
	}
	expectedCnt := len(indexIDs)
	if len(partitionTableIDs) > 0 {
		expectedCnt *= len(partitionTableIDs)
	}
	if expectedCnt != int(cnt) {
		panic("should not happened" + fmt.Sprintf("expect count: %d, real count: %d", expectedCnt, cnt))
	}
}

func (d *ddl) checkDeleteRangeCnt(job *model.Job) {
	sctx, _ := d.sessPool.get()
	s, _ := sctx.(sqlexec.SQLExecutor)
	defer func() {
		d.sessPool.put(sctx)
	}()

	query := `select sum(cnt) from
	(select count(1) cnt from mysql.gc_delete_range where job_id = %? union all
	select count(1) cnt from mysql.gc_delete_range_done where job_id = %?) as gdr;`
	rs, err := s.ExecuteInternal(context.TODO(), query, job.ID, job.ID)
	if err != nil {
		if strings.Contains(err.Error(), "Not Supported") {
			return
		}
		panic(err)
	}
	defer func() {
		_ = rs.Close()
	}()
	req := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), req)
	if err != nil {
		panic("should not happened, err:" + err.Error())
	}
	cnt, _ := req.GetRow(0).GetMyDecimal(0).ToInt()

	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			panic("should not happened")
		}
		if len(tableIDs) != int(cnt) {
			panic("should not happened" + fmt.Sprintf("expect count: %d, real count: %d", len(tableIDs), cnt))
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		var startKey kv.Key
		var physicalTableIDs []int64
		var ruleIDs []string
		if err := job.DecodeArgs(&startKey, &physicalTableIDs, &ruleIDs); err != nil {
			panic("Error in drop/truncate table, please report a bug with this stack trace and how it happened")
		}
		checkRangeCntByTableIDs(physicalTableIDs, cnt)
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		var physicalTableIDs []int64
		if err := job.DecodeArgs(&physicalTableIDs); err != nil {
			panic("should not happened")
		}
		if len(physicalTableIDs) != int(cnt) {
			panic("should not happened" + fmt.Sprintf("expect count: %d, real count: %d", len(physicalTableIDs), cnt))
		}
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDs(partitionIDs, cnt)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		var indexName interface{}
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &indexID, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDsAndIndexIDs(partitionIDs, []int64{indexID}, cnt)
	case model.ActionDropIndexes:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&[]model.CIStr{}, &[]bool{}, &indexIDs, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDsAndIndexIDs(partitionIDs, indexIDs, cnt)
	case model.ActionDropColumn:
		var colName model.CIStr
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colName, &indexIDs, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDsAndIndexIDs(partitionIDs, indexIDs, cnt)
	case model.ActionDropColumns:
		var colNames []model.CIStr
		var ifExists []bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colNames, &ifExists, &indexIDs, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDsAndIndexIDs(partitionIDs, indexIDs, cnt)
	case model.ActionModifyColumn:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexIDs, &partitionIDs); err != nil {
			panic("should not happened")
		}
		checkRangeCntByTableIDsAndIndexIDs(partitionIDs, indexIDs, cnt)
	}
}

// checkHistoryJobInTest does some sanity check to make sure something is correct after DDL complete.
// It's only check during the test environment, so it would panic directly.
// These checks may be controlled by configuration in the future.
func (d *ddl) checkHistoryJobInTest(ctx sessionctx.Context, historyJob *model.Job) {
	if !(flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil) {
		return
	}

	// Check delete range.
	if jobNeedGC(historyJob) {
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
