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

package importintotest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

var fmap = plannercore.ImportIntoFieldMap

func (s *mockGCSSuite) runTaskToAwaitingState() (context.Context, int64, *proto.Task) {
	s.T().Helper()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "resolution", Name: "a.csv"},
		Content:     []byte("aaa,bbb"),
	})
	s.prepareAndUseDB("resolution")
	// use default sql_mode
	s.tk.MustExec("set @@sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	s.tk.MustExec("create table t (a int primary key, b int);")
	importSQL := fmt.Sprintf(`import into t FROM 'gs://resolution/a.csv?endpoint=%s' with detached, __manual_recovery`, gcsEndpoint)
	rows := s.tk.MustQuery(importSQL).Rows()
	s.Len(rows, 1)
	jobID, err := strconv.Atoi(rows[0][0].(string))
	s.NoError(err)
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(int64(jobID)))
	s.NoError(err)
	_, err = handle.WaitTask(ctx, task.ID, func(t *proto.TaskBase) bool {
		return t.State == proto.TaskStateAwaitingResolution
	})
	s.NoError(err)
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID)).Rows()
	s.Len(rows, 1)
	s.EqualValues("awaiting-resolution", rows[0][fmap["Status"]])
	s.Contains(rows[0][fmap["ResultMessage"]].(string), "incorrect DOUBLE value")
	return ctx, int64(jobID), task
}

func (s *mockGCSSuite) TestResolutionFailTheTask() {
	ctx, _, task := s.runTaskToAwaitingState()
	s.tk.MustExec(fmt.Sprintf("update mysql.tidb_global_task set state='reverting' where id=%d", task.ID))
	_, err := handle.WaitTask(ctx, task.ID, func(t *proto.TaskBase) bool {
		return t.State == proto.TaskStateReverted
	})
	s.NoError(err)
	s.checkMode(s.tk, "SELECT * FROM t", "t", true)
	s.tk.MustQuery("select * from t").Check(testkit.Rows())
}

func (s *mockGCSSuite) TestResolutionCancelTheTask() {
	ctx, jobID, task := s.runTaskToAwaitingState()
	s.tk.MustExec(fmt.Sprintf("cancel import job %d", jobID))
	_, err := handle.WaitTask(ctx, task.ID, func(t *proto.TaskBase) bool {
		return t.State == proto.TaskStateReverted
	})
	s.NoError(err)
	s.checkMode(s.tk, "SELECT * FROM t", "t", true)
	s.tk.MustQuery("select * from t").Check(testkit.Rows())
}

func (s *mockGCSSuite) TestResolutionSuccessAfterManualChangeData() {
	ctx, _, task := s.runTaskToAwaitingState()
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "resolution", Name: "a.csv"},
		Content:     []byte("1,2"),
	})
	s.tk.MustExec(fmt.Sprintf("update mysql.tidb_background_subtask set state='pending' where state='failed' and task_key='%d'", task.ID))
	s.tk.MustExec(fmt.Sprintf("update mysql.tidb_global_task set state='running' where id=%d", task.ID))
	s.NoError(handle.WaitTaskDoneOrPaused(ctx, task.ID))
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
}

func (s *mockGCSSuite) checkMode(tk *testkit.TestKit, sql, tableName string, expect bool) {
	require.Eventually(s.T(), func() bool {
		err := tk.QueryToErr(sql)
		if err != nil {
			s.ErrorContains(err, "Table "+tableName+" is in mode Import")
			return !expect
		}
		return expect
	}, 10*time.Second, 100*time.Millisecond)
}
