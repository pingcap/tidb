// Copyright 2025 PingCAP, Inc.
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

package addindextest

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestMergeTempIndexBasic(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")

	testCases := []struct {
		name          string
		createTable   string
		createIndex   string
		adminCheck    string
		initOp        []string
		incrOp        []string
		readIdxRowCnt []string
		mergeIdxCnt   []string
		expectedErr   string
	}{
		{
			name:          "basic",
			createTable:   "create table t (a int primary key, b int);",
			createIndex:   "create index idx on t(b);",
			adminCheck:    "admin check index t idx;",
			initOp:        []string{"insert into t values (1, 1);"},
			incrOp:        []string{"insert into t values (2, 2), (3, 3);"},
			readIdxRowCnt: []string{"1"},
			mergeIdxCnt:   []string{"2"},
		},
		{
			name:          "unique index",
			createTable:   "create table t (a int primary key, b int);",
			createIndex:   "create unique index idx on t(b);",
			adminCheck:    "admin check index t idx;",
			initOp:        []string{"insert into t values (1, 1);"},
			incrOp:        []string{"insert into t values (2, 1);"},
			expectedErr:   "[kv:1062]Duplicate entry '1' for key 't.idx'",
			readIdxRowCnt: []string{"1"},
			mergeIdxCnt:   []string{"0"},
		},
		{
			name:          "partitioned table",
			createTable:   "create table t (a int primary key, b int) partition by hash(a) partitions 3;",
			createIndex:   "create index idx on t(b);",
			adminCheck:    "admin check index t idx;",
			initOp:        []string{"insert into t values (1, 1), (2, 2), (3, 3), (4, 4);"},
			incrOp:        []string{"insert into t values (5, 5), (6, 6), (7, 7);"},
			readIdxRowCnt: []string{"1", "2", "1"},
			mergeIdxCnt:   []string{"1", "1", "1"},
		},
		{
			name:          "global index on partitioned table",
			createTable:   "create table t (a int primary key, b int) partition by hash(a) partitions 3;",
			createIndex:   "create index idx on t(b) global;",
			adminCheck:    "admin check index t idx;",
			initOp:        []string{"insert into t values (1, 1), (2, 2), (3, 3), (4, 4);"},
			incrOp:        []string{"insert into t values (5, 5), (6, 6), (7, 7);"},
			readIdxRowCnt: []string{"1", "2", "1"},
			mergeIdxCnt:   []string{"3"},
		},
		{
			name:          "multi-schema change",
			createTable:   "create table t (a int primary key, b int, c int);",
			createIndex:   "alter table t add index idx(b), add index idx2(c);",
			adminCheck:    "admin check table t;",
			initOp:        []string{"insert into t values (1, 1, 1);"},
			incrOp:        []string{"insert into t values (2, 2, 2), (3, 3, 3);"},
			readIdxRowCnt: []string{"1"},
			mergeIdxCnt:   []string{"2", "2"},
		},
		{
			name:          "modify column with index covered",
			createTable:   "create table t (a int primary key, b int, c int, index idx(b));",
			createIndex:   "alter table t modify column b smallint;",
			adminCheck:    "admin check table t;",
			initOp:        []string{"insert into t values (1, 1, 1);"},
			incrOp:        []string{"insert into t values (2, 2, 2), (3, 3, 3);"},
			readIdxRowCnt: []string{"1"},
			mergeIdxCnt:   []string{"2"},
		},
	}

	for _, tc := range testCases {
		tk.WithComments(tc.name)
		tk.MustExec("drop database if exists test;")
		tk.MustExec("create database test;")
		tk.MustExec("use test;")
		tk.MustExec("create view all_global_tasks as select * from mysql.tidb_global_task union all select * from mysql.tidb_global_task_history;")
		tk.MustExec("create view all_subtasks as select * from mysql.tidb_background_subtask union all select * from mysql.tidb_background_subtask_history;")

		tk.MustExec(tc.createTable)
		for _, data := range tc.initOp {
			tk.MustExec(data)
		}

		var jobID int64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
			if jobID == 0 ||
				job.Type == model.ActionAddIndex ||
				job.Type == model.ActionModifyColumn ||
				job.Type == model.ActionMultiSchemaChange {
				jobID = job.ID
			}
		})
		runInsert := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/ingest/beforeBackendIngest", func() {
			if !runInsert {
				tk2 := testkit.NewTestKit(t, store)
				tk2.MustExec("use test")
				for _, data := range tc.incrOp {
					tk2.MustExec(data)
				}
				runInsert = true
			}
		})
		if tc.expectedErr != "" {
			tk.MustGetErrMsg(tc.createIndex, tc.expectedErr)
		} else {
			tk.MustExec(tc.createIndex)
			tk.MustExec(tc.adminCheck)
		}

		tkBuilder := ddl.NewTaskKeyBuilder()
		taskKey := tkBuilder.Build(jobID)

		require.True(t, runInsert, "%s: never ran incrOp: %s", tc.name, tc.incrOp[0])
		query := fmt.Sprintf(`select id from all_global_tasks where task_key like '%s' order by id`, fmt.Sprintf("%%%s%%", taskKey))
		t.Log(query)
		taskIDRows := tk.MustQuery(query).Rows()
		require.Len(t, taskIDRows, 2)
		taskID := taskIDRows[0][0].(string)
		mergeTaskID := taskIDRows[1][0].(string)
		readIdxCntSQL := fmt.Sprintf("select json_extract(summary, '$.row_count') from all_subtasks where task_key = %s and step = 1", taskID)
		tk.MustQuery(readIdxCntSQL).Check(testkit.Rows(tc.readIdxRowCnt...))
		mergeCntSQL := fmt.Sprintf("select json_extract(summary, '$.row_count') from all_subtasks where task_key = %s and step = 4", mergeTaskID)
		tk.MustQuery(mergeCntSQL).Check(testkit.Rows(tc.mergeIdxCnt...))
	}
}

func TestMergeTempIndexStuck(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a bigint)")
	var (
		workerNum = 5
		batchNum  = 10
		pkBegin   = 0
		pkEnd     = 50 // workerNum * batchNum = pkEnd - pkBegin
		rowNum    = 100
		values    = make([]string, 0, rowNum+1)
		execCnt   atomic.Int64
	)
	for i := range rowNum {
		values = append(values, fmt.Sprintf("(%d, %d)", i, i))
	}
	tk.MustExec("insert into t values " + strings.Join(values, ","))
	getSQLStmt := func(pk int, valNum int) string {
		vals := make([]string, 0, valNum)
		for i := pk; i < pk+valNum; i++ {
			a := time.Now().UnixNano()
			vals = append(vals, fmt.Sprintf("(%d, %d)", i, a))
		}
		valsStr := strings.Join(vals, ", ")
		query := fmt.Sprintf("INSERT INTO t (id, a) VALUES %s ON DUPLICATE KEY UPDATE `a` = VALUES(`a`);", valsStr)
		return query
	}
	// start workload
	chPk := make(chan int, workerNum)
	chPkFinish := make(chan int, workerNum)
	done := make(chan struct{})
	var wg util.WaitGroupWrapper
	for i := 0; i < workerNum; i++ {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		wg.Run(func() {
			for {
				select {
				case pk, ok := <-chPk:
					if !ok {
						return
					}
					query := getSQLStmt(pk, batchNum)
					tk.MustExec(query)
					select {
					case chPkFinish <- pk:
						execCnt.Add(1)
					case <-done:
						return
					}

				case <-done:
					return
				}
			}
		})
	}
	for i := pkBegin; i < pkEnd; i += batchNum {
		chPk <- i
	}
	wg.Run(func() {
		for {
			select {
			case pk := <-chPkFinish:
				chPk <- pk
			case <-done:
				return
			}
		}
	})

	require.Eventually(t, func() bool {
		return execCnt.Load() >= 5000
	}, 30*time.Second, 300*time.Millisecond)
	tk.MustExec("alter table t add index idx_a(a);")
	tk.MustExec("admin check index t idx_a;")
	close(done)
	wg.Wait()
	tk.MustExec("drop table t")
}
