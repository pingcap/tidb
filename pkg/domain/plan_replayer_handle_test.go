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

package domain_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
)

func TestPlanReplayerHandleCollectTask(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	prHandle := dom.GetPlanReplayerHandle()

	// assert 1 task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	err := prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	// assert no task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 0)

	// assert 1 unhandled task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('345','345');")
	tk.MustExec("insert into mysql.plan_replayer_status(sql_digest, plan_digest, token, instance) values ('123','123','123','123')")
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	// assert 2 unhandled task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('345','345');")
	tk.MustExec("insert into mysql.plan_replayer_status(sql_digest, plan_digest, fail_reason, instance) values ('123','123','123','123')")
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 2)
}

func TestPlanReplayerHandleDumpTask(t *testing.T) {
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	prHandle := dom.GetPlanReplayerHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("select * from t;")
	_, d := tk.Session().GetSessionVars().StmtCtx.SQLDigest()
	_, pd := tk.Session().GetSessionVars().StmtCtx.GetPlanDigest()
	sqlDigest := d.String()
	planDigest := pd.String()

	// register task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec(fmt.Sprintf("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('%v','%v');", sqlDigest, planDigest))
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	tk.MustExec("SET @@tidb_enable_plan_replayer_capture = ON;")

	// capture task and dump
	tk.MustQuery("select * from t;")
	task := prHandle.DrainTask()
	require.NotNil(t, task)
	worker := prHandle.GetWorker()
	success := worker.HandleTask(task)
	require.True(t, success)
	require.Equal(t, prHandle.GetTaskStatus().GetRunningTaskStatusLen(), 0)
	// assert memory task consumed
	require.Len(t, prHandle.GetTasks(), 0)

	// assert collect task again and no more memory task
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 0)

	// clean the task and register task
	prHandle.GetTaskStatus().CleanFinishedTaskStatus()
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec(fmt.Sprintf("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('%v','%v');", sqlDigest, "*"))
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)
	tk.MustQuery("select * from t;")
	task = prHandle.DrainTask()
	require.NotNil(t, task)
	worker = prHandle.GetWorker()
	success = worker.HandleTask(task)
	require.True(t, success)
	require.Equal(t, prHandle.GetTaskStatus().GetRunningTaskStatusLen(), 0)
	// assert capture * task still remained
	require.Len(t, prHandle.GetTasks(), 1)
}

func TestPlanReplayerGC(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	handler := dom.GetDumpFileGCChecker()

	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	startTime := time.Now()
	time := startTime.UnixNano()
	fileName := fmt.Sprintf("replayer_single_xxxxxx_%v.zip", time)
	tk.MustExec("insert into mysql.plan_replayer_status(sql_digest, plan_digest, token, instance) values" +
		"('123','123','" + fileName + "','123')")
	path := filepath.Join(replayer.GetPlanReplayerDirName(), fileName)
	writer, err := storage.Create(ctx, path, nil)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)
	handler.GCDumpFiles(ctx, 0, 0)
	tk.MustQuery("select count(*) from mysql.plan_replayer_status").Check(testkit.Rows("0"))

	exists, err := storage.FileExists(ctx, path)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestInsertPlanReplayerStatus(t *testing.T) {
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	prHandle := dom.GetPlanReplayerHandle()
	tk.MustExec("use test")
	tk.MustExec(`
	CREATE TABLE tableA (
		columnA VARCHAR(255),
		columnB DATETIME,
		columnC VARCHAR(255)
	)`)

	// This is a single quote in the sql.
	// We should escape it correctly.
	sql := `
SELECT * from tableA where SUBSTRING_INDEX(tableA.columnC, '_', 1) = tableA.columnA
`

	tk.MustQuery(sql)
	_, d := tk.Session().GetSessionVars().StmtCtx.SQLDigest()
	_, pd := tk.Session().GetSessionVars().StmtCtx.GetPlanDigest()
	sqlDigest := d.String()
	planDigest := pd.String()

	// Register task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec(fmt.Sprintf("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('%v','%v');", sqlDigest, planDigest))
	err = prHandle.CollectPlanReplayerTask()
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	tk.MustExec("SET @@tidb_enable_plan_replayer_capture = ON;")

	// Capture task and dump
	tk.MustQuery(sql)
	task := prHandle.DrainTask()
	require.NotNil(t, task)
	worker := prHandle.GetWorker()
	success := worker.HandleTask(task)
	require.True(t, success)
	require.Equal(t, prHandle.GetTaskStatus().GetRunningTaskStatusLen(), 0)
	// assert memory task consumed
	require.Len(t, prHandle.GetTasks(), 0)

	// Check the plan_replayer_status.
	// We should store the origin sql correctly.
	rows := tk.MustQuery(
		"select * from mysql.plan_replayer_status where sql_digest = ? and plan_digest = ? and origin_sql is not null",
		sqlDigest,
		planDigest,
	).Rows()
	require.Len(t, rows, 1)
}
