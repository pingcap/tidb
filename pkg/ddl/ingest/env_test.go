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

package ingest_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestGenLightningDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	port, iPort := "5678", uint(5678)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempDir = tmpDir
		conf.Port = iPort
	})
	sPath, err := ingest.GenIngestTempDataDir()
	require.NoError(t, err)
	require.Equal(t, tmpDir+"/tmp_ddl-"+port, sPath)
}

func TestLitBackendCtxMgr(t *testing.T) {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	store := testkit.CreateMockStore(t)
	sortPath := t.TempDir()
	staleJobDir := filepath.Join(sortPath, "100")
	staleJobDir2 := filepath.Join(sortPath, "101")
	staleJobDir3 := filepath.Join(sortPath, "102")
	err := os.MkdirAll(staleJobDir, 0o700)
	require.NoError(t, err)
	err = os.MkdirAll(staleJobDir2, 0o700)
	require.NoError(t, err)
	err = os.MkdirAll(staleJobDir3, 0o700)
	require.NoError(t, err)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewSession(t, store), nil
	}, 10, 10, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})
	taskManager := storage.NewTaskManager(pool)
	storage.SetTaskManager(taskManager)
	t.Cleanup(func() {
		storage.SetTaskManager(nil)
	})

	runningJobs := map[int64]struct{}{
		100: {},
	}
	taskKey101 := ddlutil.DistTaskKey(proto.Backfill, 101)
	_, err = taskManager.CreateTask(
		ctx,
		taskKey101,
		proto.Backfill,
		1,
		"",
		nil,
	)
	require.NoError(t, err)
	// multi-schema change task key
	taskKey102 := ddlutil.DistTaskKey(proto.Backfill, 102) + "/1"
	_, err = taskManager.CreateTask(
		ctx,
		taskKey102,
		proto.Backfill,
		1,
		"",
		nil,
	)
	require.NoError(t, err)

	ingest.CleanUpTempDir(ctx, sortPath, runningJobs)
	require.DirExists(t, staleJobDir)
	require.DirExists(t, staleJobDir2)
	require.DirExists(t, staleJobDir3)

	ingest.CleanUpTempDir(ctx, sortPath, nil)
	require.NoDirExists(t, staleJobDir)
	require.DirExists(t, staleJobDir2)
	require.DirExists(t, staleJobDir3)

	// mimic task is moved to history table
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_global_task where task_key = '%s'", taskKey101))

	ingest.CleanUpTempDir(ctx, sortPath, nil)
	require.NoDirExists(t, staleJobDir)
	require.NoDirExists(t, staleJobDir2)
	require.DirExists(t, staleJobDir3)

	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_global_task where task_key = '%s'", taskKey102))
	ingest.CleanUpTempDir(ctx, sortPath, nil)
	require.NoDirExists(t, staleJobDir)
	require.NoDirExists(t, staleJobDir2)
	require.NoDirExists(t, staleJobDir3)
}
