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

package brietest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/brhelper/workload"
	"github.com/stretchr/testify/require"
)

func TestSegmentedRestoreWorkload(t *testing.T) {
	kit := NewLogBackupKit(t)
	taskName := "segmented_restore_" + t.Name()
	kit.StopTaskIfExists(taskName)

	db := testkit.CreateMockDB(kit.tk)
	t.Cleanup(func() {
		_ = db.Close()
	})

	store := workload.NewMemoryStore()
	cases := []workload.Case{
		&workload.NexusDDLDestructiveCase{},
		&workload.NexusDDLCase{},
		&workload.AddIndexCase{},
	}
	if tiflashCount := tiflashStoreCount(t, kit.tk); tiflashCount > 0 {
		cases = append(cases, &workload.ModifyTiFlashCase{NAP: tiflashCount})
	} else {
		t.Log("TiFlash not found in environment, won't run tiflash related cases.")
	}
	runner, err := workload.NewRunner(db, store, cases...)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = runner.Prepare(ctx)
	require.NoError(t, err)

	kit.RunFullBackup(func(cfg *task.BackupConfig) {
		cfg.Storage = kit.LocalURI("full")
	})
	backupTS := readBackupEndTS(t, kit.LocalURI("full"))

	kit.RunLogStart(taskName, func(cfg *task.StreamConfig) {
		cfg.StartTS = backupTS
	})
	t.Cleanup(func() {
		kit.StopTaskIfExists(taskName)
	})

	checkpoints := make([]uint64, 0, 5)
	runCfg := workload.RunConfig{
		TickCount:    100,
		TickInterval: 0,
		Seed:         1,
		Parallel:     true,
	}

	for i := 0; i < 4; i++ {
		_, err := runner.Run(ctx, runCfg)
		require.NoError(t, err)
		kit.forceFlushAndWait(taskName)
		checkpoints = append(checkpoints, kit.CheckpointTSOf(taskName))
	}
	kit.forceFlushAndWait(taskName)
	checkpoints = append(checkpoints, kit.CheckpointTSOf(taskName))
	kit.StopTaskIfExists(taskName)

	cleanupWorkloadSchemas(t, kit.tk)
	cleanupRestoreRegistry(t, kit.tk)

	checkpointDir := filepath.Join(kit.base, "checkpoint")
	require.NoError(t, os.RemoveAll(checkpointDir))

	for i, restoreTS := range checkpoints {
		idx := i
		rcTS := restoreTS
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.RestoreTS = rcTS
			rc.IsRestoredTSUserSpecified = true
			rc.LastRestore = idx == len(checkpoints)-1
			rc.IsLastRestoreUserSpecified = true
			rc.UseCheckpoint = true
			rc.CheckpointStorage = kit.LocalURI("checkpoint")
			if idx > 0 {
				rc.StartTS = checkpoints[idx-1]
				rc.FullBackupStorage = ""
			}
			kit.SetFilter(&rc.Config, "test*.*")
			if idx != len(checkpoints)-1 {
				rc.ExplicitFilter = false
			}
		})
	}

	require.NoError(t, runner.Verify(ctx))
}

func readBackupEndTS(t *testing.T, storage string) uint64 {
	cfg := task.DefaultConfig()
	cfg.Storage = storage
	_, _, backupMeta, err := task.ReadBackupMeta(context.Background(), metautil.MetaFile, &cfg)
	require.NoError(t, err)
	return backupMeta.GetEndVersion()
}

func cleanupWorkloadSchemas(t *testing.T, tk *testkit.TestKit) {
	t.Helper()

	droppedAt := make(map[string]time.Time)
	var lastLog time.Time
	require.Eventuallyf(t, func() bool {
		rows := tk.MustQuery("SELECT schema_name FROM information_schema.schemata").Rows()
		remaining := make([]string, 0, len(rows))
		now := time.Now()

		for _, row := range rows {
			name := fmt.Sprint(row[0])
			if isSystemSchema(name) {
				continue
			}
			key := strings.ToLower(name)
			remaining = append(remaining, name)
			if last, ok := droppedAt[key]; !ok || now.Sub(last) > 5*time.Second {
				tk.MustExec("DROP DATABASE IF EXISTS " + workload.QIdent(name))
				droppedAt[key] = now
			}
		}

		if len(remaining) == 0 {
			return true
		}
		if now.Sub(lastLog) > 10*time.Second {
			t.Logf("waiting for schemas to drop: %v", remaining)
			lastLog = now
		}
		return false
	}, 2*time.Minute, 500*time.Millisecond, "user schemas still exist")

	tk.MustExec("CREATE DATABASE IF NOT EXISTS test")
}

func cleanupRestoreRegistry(t *testing.T, tk *testkit.TestKit) {
	t.Helper()

	rows := tk.MustQuery(fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		registry.RestoreRegistryDBName,
		registry.RestoreRegistryTableName,
	)).Rows()
	require.Len(t, rows, 1)
	count, err := parseCount(rows[0][0])
	require.NoError(t, err)
	if count == 0 {
		return
	}
	tk.MustExec(fmt.Sprintf("DELETE FROM %s.%s", registry.RestoreRegistryDBName, registry.RestoreRegistryTableName))
}

func isSystemSchema(name string) bool {
	switch strings.ToLower(name) {
	case "mysql",
		"information_schema",
		"performance_schema",
		"sys",
		"metrics_schema":
		return true
	default:
		return false
	}
}

func tiflashStoreCount(t *testing.T, tk *testkit.TestKit) int {
	rows := tk.MustQuery("SELECT COUNT(*) FROM information_schema.tikv_store_status WHERE JSON_SEARCH(LABEL, 'one', 'tiflash') IS NOT NULL").Rows()
	require.Len(t, rows, 1)
	count, err := parseCount(rows[0][0])
	require.NoError(t, err)
	return count
}

func parseCount(raw any) (int, error) {
	switch v := raw.(type) {
	case string:
		var out int
		_, err := fmt.Sscanf(v, "%d", &out)
		return out, err
	case int:
		return v, nil
	case int64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("unexpected count type %T", raw)
	}
}
