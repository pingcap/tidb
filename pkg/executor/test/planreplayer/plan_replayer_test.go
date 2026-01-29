// Copyright 2021 PingCAP, Inc.
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

package planreplayer

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
)

func checkFileName(s string) bool {
	files := []string{
		"config.toml",
		"debug_trace/debug_trace0.json",
		"meta.txt",
		"stats/test.t_dump_single.json",
		"schema/test.t_dump_single.schema.txt",
		"schema/schema_meta.txt",
		"table_tiflash_replica.txt",
		"variables.toml",
		"session_bindings.sql",
		"global_bindings.sql",
		"sql/sql0.sql",
		"explain.txt",
		"statsMem/test.t_dump_single.txt",
		"sql_meta.toml",
	}
	for _, f := range files {
		if strings.Compare(f, s) == 0 {
			return true
		}
	}
	return false
}

func TestPlanReplayer(t *testing.T) {
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustQuery("plan replayer dump explain select * from t where a=10")
	tk.MustQuery("plan replayer dump explain select /*+ read_from_storage(tiflash[t]) */ * from t")

	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v2 as select * from v1")
	tk.MustQuery("plan replayer dump explain with tmp as (select a from t1 group by t1.a) select * from tmp, t2 where t2.a=tmp.a;")
	tk.MustQuery("plan replayer dump explain select * from t1 where t1.a > (with cte1 as (select 1) select count(1) from cte1);")
	tk.MustQuery("plan replayer dump explain select * from v1")
	tk.MustQuery("plan replayer dump explain select * from v2")
	require.True(t, len(tk.Session().GetSessionVars().LastPlanReplayerToken) > 0)

	// clear the status table and assert
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustQuery("plan replayer dump explain select * from v2")
	token := tk.Session().GetSessionVars().LastPlanReplayerToken
	rows := tk.MustQuery(fmt.Sprintf("select * from mysql.plan_replayer_status where token = '%v'", token)).Rows()
	require.Len(t, rows, 1)
}

func TestPlanReplayerCaptureSEM(t *testing.T) {
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	originSEM := config.GetGlobalConfig().Security.EnableSEM
	defer func() {
		config.GetGlobalConfig().Security.EnableSEM = originSEM
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("plan replayer capture '123' '123';")
	tk.MustExec("create table t(id int)")
	tk.MustQuery("plan replayer dump explain select * from t")
	tk.MustQuery("select count(*) from mysql.plan_replayer_status").Check(testkit.Rows("1"))
}

func TestPlanReplayerCapture(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("plan replayer capture '123' '123';")
	tk.MustQuery("select sql_digest, plan_digest from mysql.plan_replayer_task;").Check(testkit.Rows("123 123"))
	tk.MustGetErrMsg("plan replayer capture '123' '123';", "plan replayer capture task already exists")
	tk.MustExec("plan replayer capture remove '123' '123'")
	tk.MustQuery("select count(*) from mysql.plan_replayer_task;").Check(testkit.Rows("0"))
	tk.MustExec("create table t(id int)")
	tk.MustExec("prepare stmt from 'update t set id = ?  where id = ? + 1';")
	tk.MustExec("SET @number = 5;")
	tk.MustExec("execute stmt using @number,@number")
	_, sqlDigest := tk.Session().GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := tk.Session().GetSessionVars().StmtCtx.GetPlanDigest()
	tk.MustExec("SET @@tidb_enable_plan_replayer_capture = ON;")
	tk.MustExec("SET @@global.tidb_enable_historical_stats_for_capture='ON'")
	tk.MustExec(fmt.Sprintf("plan replayer capture '%v' '%v'", sqlDigest.String(), planDigest.String()))
	err := dom.GetPlanReplayerHandle().CollectPlanReplayerTask()
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/shouldDumpStats", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/shouldDumpStats"))
	tk.MustExec("execute stmt using @number,@number")
	task := dom.GetPlanReplayerHandle().DrainTask()
	require.NotNil(t, task)
}

func TestPlanReplayerContinuesCapture(t *testing.T) {
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

	tk.MustExec("set @@global.tidb_enable_historical_stats='OFF'")
	_, err = tk.Exec("set @@global.tidb_enable_plan_replayer_continuous_capture='ON'")
	require.Error(t, err)
	require.Equal(t, err.Error(), "tidb_enable_historical_stats should be enabled before enabling tidb_enable_plan_replayer_continuous_capture")

	tk.MustExec("set @@global.tidb_enable_historical_stats='ON'")
	tk.MustExec("set @@global.tidb_enable_plan_replayer_continuous_capture='ON'")

	prHandle := dom.GetPlanReplayerHandle()
	tk.MustExec("delete from mysql.plan_replayer_status;")
	tk.MustExec("use test")
	tk.MustExec("create table t(id int);")
	tk.MustExec("set @@tidb_enable_plan_replayer_continuous_capture = 'ON'")
	tk.MustQuery("select * from t;")
	task := prHandle.DrainTask()
	require.NotNil(t, task)
	worker := prHandle.GetWorker()
	success := worker.HandleTask(task)
	require.True(t, success)
	tk.MustQuery("select count(*) from mysql.plan_replayer_status").Check(testkit.Rows("1"))
}

func TestPlanReplayerDumpSingle(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(tempDir, "", nil)
	require.NoError(t, err)
	extstore.SetGlobalExtStorage(storage)
	defer func() {
		extstore.SetGlobalExtStorage(nil)
		storage.Close()
	}()

	dir := t.TempDir()
	logFile := filepath.Join(dir, "tidb.log")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = logFile
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_dump_single")
	tk.MustExec("create table t_dump_single(a int)")
	res := tk.MustQuery("plan replayer dump explain select * from t_dump_single")
	path := testdata.ConvertRowsToStrings(res.Rows())

	filePath := filepath.Join(replayer.GetPlanReplayerDirName(), path[0])
	fileReader, err := storage.Open(ctx, filePath, nil)
	require.NoError(t, err)
	defer fileReader.Close()

	content, err := io.ReadAll(fileReader)
	require.NoError(t, err)

	readerAt := bytes.NewReader(content)
	reader, err := zip.NewReader(readerAt, int64(len(content)))
	require.NoError(t, err)
	for _, file := range reader.File {
		require.True(t, checkFileName(file.Name), file.Name)
	}
}
