// Copyright 2026 PingCAP, Inc.
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

package brietest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceBackupUsesGCBarrier(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run this test with real TiKV")
	}
	if !kerneltype.IsNextGen() {
		t.Skip("keyspace GC barrier requires nextgen kernel")
	}

	const keyspaceName = "keyspace1"
	store := realtikvtest.CreateMockStoreAndSetup(t, realtikvtest.WithKeyspaceName(keyspaceName))

	cfg := config.GetGlobalConfig()
	cfg.Store = config.StoreTypeTiKV
	cfg.Path = realtikvtest.PDAddr
	config.StoreGlobalConfig(cfg)

	tk := testkit.NewTestKit(t, store)

	const dbName = "br_gc_keyspace_full_backup"
	tk.MustExec("drop database if exists " + dbName)
	tk.MustExec("create database " + dbName)
	tk.MustExec("create table " + dbName + ".t(id int primary key, v int)")
	tk.MustExec("insert into " + dbName + ".t values (1, 10), (2, 20), (3, 30)")
	t.Cleanup(func() {
		tk.MustExec("drop database if exists " + dbName)
	})

	tmpDir := makeTempDirForBackup(t)
	sigGlobalSet := filepath.Join(tmpDir, "gc_global_set")
	sigGlobalDel := filepath.Join(tmpDir, "gc_global_del")
	sigKSSet := filepath.Join(tmpDir, "gc_keyspace_set")
	sigKSDel := filepath.Join(tmpDir, "gc_keyspace_del")

	enableReturnPath := func(name, path string) {
		require.NoError(t, failpoint.Enable(name, fmt.Sprintf("return(%q)", path)))
		t.Cleanup(func() { _ = failpoint.Disable(name) })
	}
	enableReturnPath("github.com/pingcap/tidb/br/pkg/gc/hint-gc-global-set-safepoint", sigGlobalSet)
	enableReturnPath("github.com/pingcap/tidb/br/pkg/gc/hint-gc-global-delete-safepoint", sigGlobalDel)
	enableReturnPath("github.com/pingcap/tidb/br/pkg/gc/hint-gc-keyspace-set-barrier", sigKSSet)
	enableReturnPath("github.com/pingcap/tidb/br/pkg/gc/hint-gc-keyspace-delete-barrier", sigKSDel)

	backupCfg := task.DefaultBackupConfig(task.DefaultConfig())
	backupCfg.Storage = "local://" + filepath.Join(tmpDir, "backup")
	backupCfg.KeyspaceName = keyspaceName
	backupCfg.CheckRequirements = false
	backupCfg.UseCheckpoint = false
	backupCfg.GCTTL = 120
	backupCfg.FilterStr = []string{dbName + ".*"}
	var err error
	backupCfg.TableFilter, err = filter.Parse(backupCfg.FilterStr)
	require.NoError(t, err)

	require.NoError(t, task.RunBackup(context.Background(), &TestKitGlue{tk: tk}, task.FullBackupCmd, &backupCfg))

	ksSetContent, err := os.ReadFile(sigKSSet)
	require.NoError(t, err)
	require.Contains(t, string(ksSetContent), "keyspace=")
	require.Contains(t, string(ksSetContent), "id=")
	_, err = os.Stat(sigKSDel)
	require.NoError(t, err)

	_, err = os.Stat(sigGlobalSet)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(sigGlobalDel)
	require.ErrorIs(t, err, os.ErrNotExist)
}
