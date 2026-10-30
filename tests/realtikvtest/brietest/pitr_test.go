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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/printer"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type TestKitGlue struct {
	tk *testkit.TestKit
}

func (tk TestKitGlue) GetDomain(_ kv.Storage) (*domain.Domain, error) {
	return domain.GetDomain(tk.tk.Session()), nil
}

func (tk TestKitGlue) CreateSession(_ kv.Storage) (glue.Session, error) {
	se, err := session.CreateSession(tk.tk.Session().GetStore())
	if err != nil {
		return nil, err
	}
	return gluetidb.WrapSession(se), nil
}

func (tk TestKitGlue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	return tk.tk.Session().GetStore(), nil
}

// OwnsStorage returns whether the storage returned by Open() is owned
// If this method returns false, the connection manager will never close the storage.
func (tk TestKitGlue) OwnsStorage() bool {
	return false
}

func (tk TestKitGlue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return &CounterProgress{}
}

// Record records some information useful for log-less summary.
func (tk TestKitGlue) Record(name string, value uint64) {}

// GetVersion gets BR package version to run backup/restore job
func (tk TestKitGlue) GetVersion() string {
	return "In Test\n" + printer.GetTiDBInfo()
}

// UseOneShotSession temporary creates session from store when run backup job.
// because we don't have to own domain/session during the whole backup.
// we can close domain as soon as possible.
// and we must reuse the exists session and don't close it in SQL backup job.
func (tk TestKitGlue) UseOneShotSession(_ kv.Storage, _ bool, fn func(se glue.Session) error) error {
	return fn(gluetidb.WrapSession(tk.tk.Session()))
}

// GetClient returns the client type of the glue
func (tk TestKitGlue) GetClient() glue.GlueClient {
	return glue.ClientSql
}

type CounterProgress struct {
	Counter atomic.Int64
}

func (c *CounterProgress) Inc() {
	c.Counter.Add(1)
}

func (c *CounterProgress) IncBy(cnt int64) {
	c.Counter.Add(cnt)
}

func (c *CounterProgress) GetCurrent() int64 {
	return c.Counter.Load()
}

func (c *CounterProgress) Close() {
}

type LogBackupKit struct {
	t       *testing.T
	tk      *testkit.TestKit
	metaCli *streamhelper.MetaDataClient
	base    string

	checkerF func(err error)
}

// getTestTempDir returns a temporary directory for tests.
// If BRIETEST_TMPDIR is set, it creates a subdirectory there (useful for CI environments
// where TiKV and test processes need to share the same filesystem).
// Otherwise, it falls back to t.TempDir().
func getTestTempDir(t *testing.T) string {
	if baseDir := os.Getenv("BRIETEST_TMPDIR"); baseDir != "" {
		dir := filepath.Join(baseDir, t.Name())
		require.NoError(t, os.MkdirAll(dir, 0755))
		t.Cleanup(func() {
			os.RemoveAll(dir)
		})
		return dir
	}
	return t.TempDir()
}

func NewLogBackupKit(t *testing.T) *LogBackupKit {
	tk := initTestKit(t)
	metaCli := streamhelper.NewMetaDataClient(domain.GetDomain(tk.Session()).GetEtcdClient())
	begin := time.Now()
	// So the cases can finish faster...
	tk.MustExec("set config tikv `log-backup.max-flush-interval` = '30s';")
	t.Cleanup(func() {
		if !t.Failed() {
			log.Info("[TEST.LogBackupKit] success", zap.String("case", t.Name()), zap.Stringer("takes", time.Since(begin)))
		}
		log.Info("[TEST.LogBackupKit] fail", zap.String("case", t.Name()), zap.Stringer("takes", time.Since(begin)))
	})
	return &LogBackupKit{
		tk:      tk,
		t:       t,
		metaCli: metaCli,
		base:    getTestTempDir(t),
		checkerF: func(err error) {
			require.NoError(t, err)
		},
	}
}

func (kit *LogBackupKit) tempFile(name string, content []byte) string {
	path := filepath.Join(kit.base, name)
	require.NoError(kit.t, os.WriteFile(path, content, 0o666))
	return path
}

func (kit *LogBackupKit) RunFullRestore(extConfig func(*task.RestoreConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultRestoreConfig(task.DefaultConfig())
		task.ApplyTiDBRuntimeConfig(&cfg.Config)
		cfg.Storage = kit.LocalURI("full")
		cfg.FilterStr = []string{"test.*"}
		var err error
		cfg.TableFilter, err = filter.Parse(cfg.FilterStr)
		cfg.CheckRequirements = false
		cfg.WithSysTable = false
		require.NoError(kit.t, err)
		cfg.UseCheckpoint = false

		extConfig(&cfg)
		return task.RunRestore(ctx, kit.Glue(), task.FullRestoreCmd, &cfg)
	})
}

func (kit *LogBackupKit) RunStreamRestore(extConfig func(*task.RestoreConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultRestoreConfig(task.DefaultConfig())
		task.ApplyTiDBRuntimeConfig(&cfg.Config)
		cfg.Storage = kit.LocalURI("incr")
		cfg.FullBackupStorage = kit.LocalURI("full")
		cfg.CheckRequirements = false
		cfg.UseCheckpoint = false
		cfg.WithSysTable = false

		extConfig(&cfg)
		return task.RunRestore(ctx, kit.Glue(), task.PointRestoreCmd, &cfg)
	})
}

func (kit *LogBackupKit) SetFilter(cfg *task.Config, f ...string) {
	var err error
	cfg.TableFilter, err = filter.Parse(f)
	require.NoError(kit.t, err)
	cfg.FilterStr = f
	cfg.ExplicitFilter = true
}

func (kit *LogBackupKit) RunFullBackup(extConfig func(*task.BackupConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultBackupConfig(task.DefaultConfig())
		task.ApplyTiDBRuntimeConfig(&cfg.Config)
		cfg.Storage = kit.LocalURI("full")

		extConfig(&cfg)
		return task.RunBackup(ctx, kit.Glue(), "backup full[intest]", &cfg)
	})
}

func (kit *LogBackupKit) StopTaskIfExists(taskName string) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamCommonFlags)
		cfg.TaskName = taskName
		err := task.RunStreamStop(ctx, kit.Glue(), "stream stop[intest]", &cfg)
		if err != nil && strings.Contains(err.Error(), "task not found") {
			return nil
		}
		return err
	})
}

func (kit *LogBackupKit) RunLogStart(taskName string, extConfig func(*task.StreamConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamStartFlags)
		cfg.Storage = kit.LocalURI("incr")
		cfg.TaskName = taskName
		cfg.EndTS = math.MaxUint64
		cfg.TableFilter = filter.All()
		cfg.FilterStr = []string{"*.*"}
		extConfig(&cfg)
		err := task.RunStreamStart(ctx, kit.Glue(), "stream start[intest]", &cfg)
		return err
	})
	kit.t.Cleanup(func() { kit.StopTaskIfExists(taskName) })
}

func (kit *LogBackupKit) RunLogPause(taskName string, extConfig func(*task.StreamConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamPauseFlags)
		cfg.TaskName = taskName
		extConfig(&cfg)
		return task.RunStreamPause(ctx, kit.Glue(), "stream pause[intest]", &cfg)
	})
}

func (kit *LogBackupKit) RunLogStatus(extConfig func(*task.StreamConfig)) (tasks []stream.TaskStatus) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamStatusCommonFlags)
		cfg.DumpStatusTo = &tasks
		cfg.TaskName = "*"
		extConfig(&cfg)
		return task.RunStreamStatus(ctx, kit.Glue(), "stream status[intest]", &cfg)
	})
	return tasks
}

func (kit *LogBackupKit) ctx() context.Context {
	return context.Background()
}

func (kit *LogBackupKit) TSO() uint64 {
	ts, err := kit.tk.Session().GetStore().(tikv.Storage).GetOracle().GetTimestamp(kit.ctx(), &oracle.Option{})
	require.NoError(kit.t, err)
	return ts
}

func (kit *LogBackupKit) LocalURI(rel ...string) string {
	return "local://" + kit.base + "/" + filepath.Join(rel...)
}

func (kit *LogBackupKit) CheckpointTSOf(taskName string) uint64 {
	task, err := kit.metaCli.GetTask(kit.ctx(), taskName)
	require.NoError(kit.t, err)
	ts, err := task.GetGlobalCheckPointTS(kit.ctx())
	require.NoError(kit.t, err)
	return ts
}

func (kit *LogBackupKit) Glue() glue.Glue {
	return &TestKitGlue{tk: kit.tk}
}

func (kit *LogBackupKit) WithChecker(checker func(v error), f func()) {
	oldExpected := kit.checkerF
	defer func() {
		kit.checkerF = oldExpected
	}()
	kit.checkerF = checker

	f()
}

func (kit *LogBackupKit) runAndCheck(f func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	begin := time.Now()
	summary.SetSuccessStatus(false)
	err := f(ctx)
	cancel()
	kit.checkerF(err)
	log.Info("[TEST.runAndCheck] A task finished.", zap.StackSkip("caller", 1), zap.Stringer("take", time.Since(begin)))
}

func (kit *LogBackupKit) forceFlush() {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultConfig()
		task.ApplyTiDBRuntimeConfig(&cfg)
		err := operator.RunForceFlush(ctx, &operator.ForceFlushConfig{
			Config:        cfg,
			StoresPattern: regexp.MustCompile(".*"),
		})
		if err != nil {
			log.Warn("[TEST.forceFlush] It seems this version of TiKV doesn't support force flush, the test may be much more slower.",
				logutil.ShortError(err))
		}
		return nil
	})
}

func (kit *LogBackupKit) forceFlushAndWait(taskName string) {
	ts := kit.TSO()
	start := time.Now()
	kit.forceFlush()
	require.Eventually(kit.t, func() bool {
		ckpt := kit.CheckpointTSOf(taskName)
		log.Info("[TEST.forceFlushAndWait] checkpoint", zap.Uint64("checkpoint", ckpt), zap.Uint64("ts", ts))
		return ckpt >= ts
	}, 300*time.Second, 1*time.Second)
	time.Sleep(6 * time.Second) // Wait the storage checkpoint uploaded...
	log.Info("[TEST.forceFlushAndWait] done", zap.Stringer("take", time.Since(start)))
}

func (kit *LogBackupKit) simpleWorkload() simpleWorkload {
	return simpleWorkload{
		tbl: kit.t.Name(),
	}
}

type simpleWorkload struct {
	tbl string
}

func (s simpleWorkload) createSimpleTableWithData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("DROP TABLE IF EXISTs test.%s", s.tbl))
	kit.tk.MustExec(fmt.Sprintf("CREATE TABLE test.%s(t text)", s.tbl))
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Ear'), ('Eye'), ('Nose')", s.tbl))
}

func (s simpleWorkload) insertSimpleIncreaseData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Body')", s.tbl))
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Mind')", s.tbl))
}

func (s simpleWorkload) verifySimpleData(kit *LogBackupKit) {
	kit.tk.MustQuery(fmt.Sprintf("SELECT * FROM test.%s", s.tbl)).Check([][]any{{"Ear"}, {"Eye"}, {"Nose"}, {"Body"}, {"Mind"}})
}

func (s simpleWorkload) cleanSimpleData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("DROP TABLE IF EXISTS test.%s", s.tbl))
}

func TestPiTRAndBackupInSQL(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	s.insertSimpleIncreaseData(kit)

	taskName := t.Name()
	kit.RunFullBackup(func(bc *task.BackupConfig) {})
	s.cleanSimpleData(kit)

	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.Storage = kit.LocalURI("full2")
		bc.BackupTS = ts
	})
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {
		sc.StartTS = ts
	})
	_ = kit.tk.MustQuery(fmt.Sprintf("RESTORE TABLE test.%s FROM '%s'", t.Name(), kit.LocalURI("full")))
	s.verifySimpleData(kit)
	kit.forceFlushAndWait(taskName)

	s.cleanSimpleData(kit)
	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		rc.FullBackupStorage = kit.LocalURI("full2")
	})
	s.verifySimpleData(kit)
}

func TestPiTRAndRestoreFromMid(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	s.insertSimpleIncreaseData(kit)

	taskName := t.Name()

	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s.tbl))
		bc.Storage = kit.LocalURI("fulla")
	})
	s.cleanSimpleData(kit)

	s2 := kit.simpleWorkload()
	s2.tbl += "2"
	s2.createSimpleTableWithData(kit)
	s2.insertSimpleIncreaseData(kit)
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s2.tbl))
		bc.Storage = kit.LocalURI("fullb")
	})
	s2.cleanSimpleData(kit)

	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {
		rc.Storage = kit.LocalURI("fulla")
		kit.SetFilter(&rc.Config, fmt.Sprintf("test.%s", s.tbl))
	})
	s.cleanSimpleData(kit)

	ts2 := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.Storage = kit.LocalURI("pitr_base_2")
		bc.BackupTS = ts2
	})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {
		rc.Storage = kit.LocalURI("fullb")
		kit.SetFilter(&rc.Config, fmt.Sprintf("test.%s", s2.tbl))
	})

	kit.forceFlushAndWait(taskName)
	s.cleanSimpleData(kit)
	s2.cleanSimpleData(kit)
	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		rc.FullBackupStorage = kit.LocalURI("pitr_base_2")
	})
	s2.verifySimpleData(kit)
	kit.tk.MustQuery("SELECT * FROM information_schema.tables WHERE table_name = ?", s.tbl).Check([][]any{})
}

// TestPiTRRestoreWithRename covers the PiTR --rename feature end to end:
// snapshot-only routes, independent log-only continuations after replacement
// DDL, legacy source identity recovery, and conflicts between a routed and an
// identity table.
func TestPiTRRestoreWithRename(t *testing.T) {
	t.Run("snapshot routes", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-rename"

		drop := func() {
			kit.tk.MustExec("DROP DATABASE IF EXISTS rn_src")
			kit.tk.MustExec("DROP DATABASE IF EXISTS rn_schema")
			kit.tk.MustExec("DROP DATABASE IF EXISTS rn_dst")
			kit.tk.MustExec("DROP DATABASE IF EXISTS rn_renamed")
		}
		drop()
		t.Cleanup(drop)

		kit.tk.MustExec("CREATE DATABASE rn_src")
		kit.tk.MustExec("CREATE DATABASE rn_schema")
		kit.tk.MustExec("CREATE TABLE rn_src.t_log(id INT PRIMARY KEY, v VARCHAR(20))")
		kit.tk.MustExec("CREATE TABLE rn_src.t_snapshot(id INT PRIMARY KEY, v VARCHAR(20))")
		kit.tk.MustExec("CREATE TABLE rn_schema.t(id INT PRIMARY KEY, v VARCHAR(20))")
		kit.tk.MustExec("INSERT INTO rn_src.t_log VALUES (1, 'before')")
		kit.tk.MustExec("INSERT INTO rn_src.t_snapshot VALUES (1, 'snapshot')")
		kit.tk.MustExec("INSERT INTO rn_schema.t VALUES (1, 'before')")

		// Start the log task before the full backup so the full backup falls inside
		// the log range. Every table predates the log range, so only DML is logged.
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("INSERT INTO rn_src.t_log VALUES (2, 'after')")
		kit.tk.MustExec("INSERT INTO rn_schema.t VALUES (2, 'after')")
		kit.forceFlushAndWait(taskName)

		// Stop the task before dropping the sources so the drops stay outside the
		// restored log range and the schemas have to come from the full backup.
		kit.StopTaskIfExists(taskName)
		kit.tk.MustExec("DROP DATABASE rn_src")
		kit.tk.MustExec("DROP DATABASE rn_schema")

		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.Rename = []string{
				"rn_src.t_snapshot:rn_dst.copy",
				"rn_schema:rn_renamed",
			}
			kit.SetFilter(&rc.Config, "rn_src.*", "rn_schema.*")
		})

		// The routed table is restored under the target schema and is queryable,
		// which also proves the restore reset it from restore mode. It is created
		// before the log task starts, so it has no log event and its downstream
		// schema identity can only come from the snapshot phase.
		kit.tk.MustQuery("SELECT id, v FROM rn_dst.copy").Check(testkit.Rows("1 snapshot"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'rn_src'").
			Check(testkit.Rows("t_log"))
		// The identity table keeps its source name and receives the replayed log rows.
		kit.tk.MustQuery("SELECT id, v FROM rn_src.t_log ORDER BY id").Check(testkit.Rows("1 before", "2 after"))
		// The schema-level route moves the whole schema.
		kit.tk.MustQuery("SELECT id, v FROM rn_renamed.t ORDER BY id").Check(testkit.Rows("1 before", "2 after"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'rn_schema'").
			Check([][]any{})
	})

	t.Run("shared schema survives its source owner DROP", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-shared-drop"
		drop := func() {
			for _, name := range []string{"rn_check", "rn_owner", "rn_guest", "rn_shared"} {
				kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
			}
		}
		drop()
		t.Cleanup(drop)
		kit.tk.MustExec("CREATE DATABASE rn_owner")
		kit.tk.MustExec("CREATE DATABASE rn_guest")
		kit.tk.MustExec("CREATE TABLE rn_owner.gone(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_guest.t(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_guest.t VALUES (1)")
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("DROP DATABASE rn_owner")
		kit.tk.MustExec("INSERT INTO rn_guest.t VALUES (2)")
		kit.forceFlushAndWait(taskName)
		kit.StopTaskIfExists(taskName)
		kit.tk.MustExec("DROP DATABASE rn_guest")
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.Rename = []string{"rn_owner:rn_shared", "rn_guest.t:rn_shared.copy"}
			kit.SetFilter(&rc.Config, "rn_owner.*", "rn_guest.*")
		})
		kit.tk.MustQuery("SELECT * FROM rn_shared.copy ORDER BY id").Check(testkit.Rows("1", "2"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='rn_shared'").Check(testkit.Rows("copy"))
		dbInfo, exists := domain.GetDomain(kit.tk.Session()).InfoSchema().SchemaByName(ast.NewCIStr("rn_shared"))
		require.True(t, exists)
		require.NoError(t, kv.RunInNewTxn(context.Background(), kit.tk.Session().GetStore(), false, func(_ context.Context, txn kv.Transaction) error {
			persisted, err := meta.NewMutator(txn).GetDatabase(dbInfo.ID)
			if err != nil {
				return err
			}
			require.NotNil(t, persisted)
			require.Equal(t, model.StatePublic, persisted.State)
			return nil
		}))
		kit.tk.MustExec("CREATE DATABASE rn_check")
		kit.tk.MustExec("CREATE TABLE rn_check.child(id INT PRIMARY KEY, FOREIGN KEY (id) REFERENCES rn_shared.copy(id))")
		require.Error(t, kit.tk.ExecToErr("DROP DATABASE rn_shared"))
		kit.tk.MustQuery("SELECT * FROM rn_shared.copy ORDER BY id").Check(testkit.Rows("1", "2"))
	})

	t.Run("upstream move with an unrelated route", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-unrelated-route-move"
		drop := func() {
			for _, name := range []string{"rn_before", "rn_after", "rn_other", "rn_copy"} {
				kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
			}
		}
		drop()
		t.Cleanup(drop)
		kit.tk.MustExec("CREATE DATABASE rn_before CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
		kit.tk.MustExec("CREATE DATABASE rn_after CHARACTER SET latin1 COLLATE latin1_bin")
		kit.tk.MustExec("CREATE DATABASE rn_other CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
		kit.tk.MustExec("CREATE TABLE rn_before.t(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_other.u(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_before.t VALUES (1)")
		kit.tk.MustExec("INSERT INTO rn_other.u VALUES (1)")
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("RENAME TABLE rn_before.t TO rn_after.t")
		kit.tk.MustExec("INSERT INTO rn_after.t VALUES (2)")
		kit.forceFlushAndWait(taskName)
		kit.StopTaskIfExists(taskName)
		drop()
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.Rename = []string{"rn_other.u:rn_copy.copy"}
			kit.SetFilter(&rc.Config, "rn_before.*", "rn_after.*", "rn_other.*")
		})
		kit.tk.MustQuery("SELECT * FROM rn_after.t ORDER BY id").Check(testkit.Rows("1", "2"))
		kit.tk.MustQuery("SELECT * FROM rn_copy.copy").Check(testkit.Rows("1"))
		kit.tk.MustQuery("SELECT default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name='rn_after'").Check(testkit.Rows("latin1 latin1_bin"))
		kit.tk.MustQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name='rn_before'").Check(testkit.Rows("rn_before"))
	})

	t.Run("recreated destination with an unrelated route", func(t *testing.T) {
		for _, testCase := range []struct {
			name     string
			rules    []string
			targetDB string
		}{
			{name: "identity", rules: []string{"rn_recreate_other.u:rn_recreate_copy.copy"}, targetDB: "rn_recreate_after"},
			{name: "schema route", rules: []string{"rn_recreate_other.u:rn_recreate_copy.copy", "rn_recreate_after:rn_recreate_target"}, targetDB: "rn_recreate_target"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				kit := NewLogBackupKit(t)
				taskName := "test-pitr-recreated-schema-move"
				drop := func() {
					for _, name := range []string{"rn_recreate_before", "rn_recreate_after", "rn_recreate_other", "rn_recreate_copy", "rn_recreate_target"} {
						kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
					}
				}
				drop()
				t.Cleanup(drop)
				kit.tk.MustExec("CREATE DATABASE rn_recreate_before CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
				kit.tk.MustExec("CREATE DATABASE rn_recreate_after CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
				kit.tk.MustExec("CREATE DATABASE rn_recreate_other CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
				kit.tk.MustExec("CREATE TABLE rn_recreate_before.t(id INT PRIMARY KEY)")
				kit.tk.MustExec("CREATE TABLE rn_recreate_other.u(id INT PRIMARY KEY)")
				kit.tk.MustExec("INSERT INTO rn_recreate_before.t VALUES (1)")
				kit.tk.MustExec("INSERT INTO rn_recreate_other.u VALUES (1)")
				kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
				kit.RunFullBackup(func(bc *task.BackupConfig) {})
				kit.tk.MustExec("DROP DATABASE rn_recreate_after")
				kit.tk.MustExec("CREATE DATABASE rn_recreate_after CHARACTER SET latin1 COLLATE latin1_bin")
				kit.tk.MustExec("RENAME TABLE rn_recreate_before.t TO rn_recreate_after.t")
				kit.tk.MustExec("INSERT INTO rn_recreate_after.t VALUES (2)")
				kit.tk.MustExec("DROP DATABASE rn_recreate_before")
				kit.forceFlushAndWait(taskName)
				kit.StopTaskIfExists(taskName)
				drop()
				kit.RunStreamRestore(func(rc *task.RestoreConfig) {
					rc.Rename = testCase.rules
					kit.SetFilter(&rc.Config, "rn_recreate_before.*", "rn_recreate_after.*", "rn_recreate_other.*")
				})
				kit.tk.MustQuery(fmt.Sprintf("SELECT * FROM %s.t ORDER BY id", testCase.targetDB)).Check(testkit.Rows("1", "2"))
				kit.tk.MustQuery("SELECT * FROM rn_recreate_copy.copy").Check(testkit.Rows("1"))
				kit.tk.MustQuery(fmt.Sprintf("SELECT default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name='%s'", testCase.targetDB)).Check(testkit.Rows("latin1 latin1_bin"))
				kit.tk.MustQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name='rn_recreate_before'").Check(testkit.Rows())
			})
		}
	})

	t.Run("routed move drops its historical parent", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-routed-historical-settings"
		drop := func() {
			for _, name := range []string{"rn_alias_before", "rn_alias_after", "rn_alias_target"} {
				kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
			}
		}
		drop()
		t.Cleanup(drop)
		kit.tk.MustExec("CREATE DATABASE rn_alias_before CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
		kit.tk.MustExec("CREATE TABLE rn_alias_before.t(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_alias_before.t VALUES (1)")
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("CREATE DATABASE rn_alias_after CHARACTER SET latin1 COLLATE latin1_bin")
		kit.tk.MustExec("RENAME TABLE rn_alias_before.t TO rn_alias_after.t")
		kit.tk.MustExec("INSERT INTO rn_alias_after.t VALUES (2)")
		kit.tk.MustExec("DROP DATABASE rn_alias_before")
		kit.forceFlushAndWait(taskName)
		kit.StopTaskIfExists(taskName)
		drop()
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			rc.Rename = []string{"rn_alias_after.t:rn_alias_target.copy"}
			kit.SetFilter(&rc.Config, "rn_alias_before.*", "rn_alias_after.*")
		})
		kit.tk.MustQuery("SELECT * FROM rn_alias_target.copy ORDER BY id").Check(testkit.Rows("1", "2"))
		kit.tk.MustQuery("SELECT default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name='rn_alias_target'").Check(testkit.Rows("latin1 latin1_bin"))
		kit.tk.MustQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name='rn_alias_before'").Check(testkit.Rows())
	})

	t.Run("schema route ownership", func(t *testing.T) {
		for _, testCase := range []struct {
			name  string
			empty bool
		}{
			{name: "empty schema recreation", empty: true},
			{name: "all tables override"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				kit := NewLogBackupKit(t)
				drop := func() {
					for _, name := range []string{"rn_owner_source", "rn_owner_target", "rn_owner_other", "rn_owner_copy"} {
						kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
					}
				}
				drop()
				t.Cleanup(drop)
				kit.tk.MustExec("CREATE DATABASE rn_owner_source CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
				kit.tk.MustExec("CREATE DATABASE rn_owner_other")
				kit.tk.MustExec("CREATE TABLE rn_owner_other.u(id INT PRIMARY KEY)")
				kit.tk.MustExec("INSERT INTO rn_owner_other.u VALUES (1)")
				if !testCase.empty {
					kit.tk.MustExec("CREATE TABLE rn_owner_source.t(id INT PRIMARY KEY)")
					kit.tk.MustExec("INSERT INTO rn_owner_source.t VALUES (1)")
				}
				taskName := "test-pitr-schema-route-owner"
				kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
				kit.RunFullBackup(func(bc *task.BackupConfig) {})
				if testCase.empty {
					kit.tk.MustExec("DROP DATABASE rn_owner_source")
					kit.tk.MustExec("CREATE DATABASE rn_owner_source CHARACTER SET latin1 COLLATE latin1_bin")
				} else {
					kit.tk.MustExec("INSERT INTO rn_owner_source.t VALUES (2)")
				}
				kit.tk.MustExec("INSERT INTO rn_owner_other.u VALUES (2)")
				kit.forceFlushAndWait(taskName)
				kit.StopTaskIfExists(taskName)
				drop()
				rules := []string{"rn_owner_source:rn_owner_target"}
				if !testCase.empty {
					rules = append(rules, "rn_owner_source.t:rn_owner_copy.copy")
				}
				configure := func(rc *task.RestoreConfig) {
					rc.Rename = rules
					kit.SetFilter(&rc.Config, "rn_owner_source.*", "rn_owner_other.*")
				}
				// An older task claims the independently restored schema. Use a
				// real registration so the rejection is deterministic before DDL.
				ctx := context.Background()
				reg, err := registry.NewRestoreRegistry(ctx, kit.Glue(), domain.GetDomain(kit.tk.Session()))
				require.NoError(t, err)
				t.Cleanup(reg.Close)
				ownerID, _, err := reg.ResumeOrCreateRegistration(ctx, registry.RegistrationInfo{
					FilterStrings: []string{"rn_claim.u"}, RouteStrings: []string{"rn_claim.u:rn_owner_target.u"},
					StartTS: 1, RestoredTS: 2, UpstreamClusterID: 1, Cmd: task.PointRestoreCmd,
				}, true)
				require.NoError(t, err)
				t.Cleanup(func() {
					if ownerID != 0 {
						require.NoError(t, reg.Unregister(ctx, ownerID))
					}
				})
				kit.WithChecker(func(err error) {
					require.ErrorContains(t, err, "database rn_owner_target cannot be restored concurrently")
				}, func() { kit.RunStreamRestore(configure) })
				kit.tk.MustQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('rn_owner_target', 'rn_owner_copy')").Check(testkit.Rows())
				require.NoError(t, reg.Unregister(ctx, ownerID))
				ownerID = 0
				kit.RunStreamRestore(configure)
				kit.tk.MustQuery("SELECT * FROM rn_owner_other.u ORDER BY id").Check(testkit.Rows("1", "2"))
				settings := "utf8mb4 utf8mb4_bin"
				if testCase.empty {
					settings = "latin1 latin1_bin"
					kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='rn_owner_target'").Check(testkit.Rows())
				} else {
					kit.tk.MustQuery("SELECT * FROM rn_owner_copy.copy ORDER BY id").Check(testkit.Rows("1", "2"))
				}
				kit.tk.MustQuery("SELECT default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name='rn_owner_target'").Check(testkit.Rows(settings))
				kit.tk.MustQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name='rn_owner_source'").Check(testkit.Rows())
			})
		}
	})

	t.Run("independent log-only schema target collision", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-schema-target-collision"
		drop := func() {
			for _, name := range []string{"rn_collision_source", "rn_collision_target"} {
				kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
			}
		}
		drop()
		t.Cleanup(drop)
		kit.tk.MustExec("CREATE DATABASE rn_collision_source CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
		kit.tk.MustExec("CREATE TABLE rn_collision_source.t(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_collision_source.t VALUES (1)")
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("INSERT INTO rn_collision_source.t VALUES (2)")
		firstTS := kit.TSO()
		// This is a new upstream schema, not the downstream container that the
		// first task will create. Both source schemas survive at the endpoint.
		kit.tk.MustExec("CREATE DATABASE rn_collision_target CHARACTER SET latin1 COLLATE latin1_bin")
		kit.tk.MustExec("CREATE TABLE rn_collision_target.u(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_collision_target.u VALUES (3)")
		secondTS := kit.TSO()
		kit.forceFlushAndWait(taskName)
		kit.StopTaskIfExists(taskName)
		drop()
		configure := func(rc *task.RestoreConfig) {
			rc.Rename = []string{"rn_collision_source:rn_collision_target"}
			kit.SetFilter(&rc.Config, "rn_collision_source.*", "rn_collision_target.*")
		}
		// Complete the snapshot-backed task before starting a separate log-only
		// task, so this does not rely on an unsupported map-only baseline.
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			configure(rc)
			rc.RestoreTS = firstTS
		})
		kit.tk.MustQuery("SELECT * FROM rn_collision_target.t ORDER BY id").Check(testkit.Rows("1", "2"))
		kit.WithChecker(func(err error) {
			require.ErrorContains(t, err, "same restore target database rn_collision_target")
		}, func() {
			kit.RunStreamRestore(func(rc *task.RestoreConfig) {
				configure(rc)
				rc.FullBackupStorage = ""
				rc.StartTS, rc.RestoreTS = firstTS, secondTS
			})
		})
		kit.tk.MustQuery("SELECT * FROM rn_collision_target.t ORDER BY id").Check(testkit.Rows("1", "2"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='rn_collision_target'").Check(testkit.Rows("t"))
		kit.tk.MustQuery("SELECT default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name='rn_collision_target'").Check(testkit.Rows("utf8mb4 utf8mb4_bin"))
		kit.tk.MustQuery("SELECT COUNT(*) FROM mysql.tidb_pitr_id_map WHERE restored_ts = ?", secondTS).Check(testkit.Rows("0"))
	})

	t.Run("independent log-only tasks", func(t *testing.T) {
		kit := NewLogBackupKit(t)
		taskName := "test-pitr-rename-split"
		drop := func() {
			for _, name := range []string{"rn_split", "rn_move", "rn_target", "rn_split_schema"} {
				kit.tk.MustExec("DROP DATABASE IF EXISTS " + name)
			}
		}
		drop()
		t.Cleanup(drop)
		kit.tk.MustExec("CREATE DATABASE rn_split")
		kit.tk.MustExec("CREATE DATABASE rn_move")
		kit.tk.MustExec("CREATE TABLE rn_split.t(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_split.rebuild(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_split.keep(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_move.anchor(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_move.moved(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_split.c1(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_split.t VALUES (1)")
		kit.tk.MustExec("INSERT INTO rn_split.rebuild VALUES (1)")
		kit.tk.MustExec("INSERT INTO rn_split.keep VALUES (1)")
		kit.tk.MustExec("INSERT INTO rn_move.moved VALUES (1)")
		kit.tk.MustExec("INSERT INTO rn_split.c1 VALUES (9)")
		kit.RunLogStart(taskName, func(sc *task.StreamConfig) {})
		kit.RunFullBackup(func(bc *task.BackupConfig) {})
		kit.tk.MustExec("INSERT INTO rn_split.keep VALUES (2)")
		firstTS := kit.TSO()

		kit.tk.MustExec("TRUNCATE TABLE rn_split.t")
		kit.tk.MustExec("INSERT INTO rn_split.t VALUES (2)")
		kit.tk.MustExec("DROP TABLE rn_split.rebuild")
		kit.tk.MustExec("CREATE TABLE rn_split.rebuild(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_split.rebuild VALUES (2)")
		// This new ID has no persisted table route. Both historical buckets
		// must use the final source location when binding after the base merge.
		kit.tk.MustExec("DROP TABLE rn_move.moved")
		kit.tk.MustExec("CREATE TABLE rn_split.moving(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_split.moving VALUES (3)")
		kit.tk.MustExec("RENAME TABLE rn_split.moving TO rn_move.moved")
		secondTS := kit.TSO()

		kit.tk.MustExec("DROP TABLE rn_split.c1")
		kit.tk.MustExec("CREATE TABLE rn_split.c1(id INT PRIMARY KEY)")
		kit.tk.MustExec("CREATE TABLE rn_move.conflict(id INT PRIMARY KEY)")
		kit.tk.MustExec("INSERT INTO rn_split.c1 VALUES (10)")
		kit.tk.MustExec("INSERT INTO rn_move.conflict VALUES (20)")
		conflictTS := kit.TSO()
		kit.forceFlushAndWait(taskName)
		kit.StopTaskIfExists(taskName)
		kit.tk.MustExec("DROP DATABASE rn_split")
		kit.tk.MustExec("DROP DATABASE rn_move")

		rename := []string{
			"rn_split:rn_split_schema",
			"rn_split.t:rn_target.copy",
			"rn_split.rebuild:rn_target.recreated",
			"rn_move.moved:rn_target.alias",
			"rn_split.c1:rn_move.conflict",
		}
		configure := func(rc *task.RestoreConfig) {
			rc.Rename = rename
			kit.SetFilter(&rc.Config, "rn_split.*", "rn_move.*")
		}
		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			configure(rc)
			rc.RestoreTS = firstTS
		})
		kit.tk.MustQuery("SELECT * FROM rn_target.copy").Check(testkit.Rows("1"))
		kit.tk.MustQuery("SELECT * FROM rn_target.recreated").Check(testkit.Rows("1"))
		kit.tk.MustQuery("SELECT * FROM rn_target.alias").Check(testkit.Rows("1"))
		kit.tk.MustQuery("SELECT * FROM rn_move.conflict").Check(testkit.Rows("9"))
		kit.tk.MustQuery("SELECT * FROM rn_split_schema.keep ORDER BY id").Check(testkit.Rows("1", "2"))

		// A persisted Name is not proof of source identity, even without a
		// schema route. Reject the legacy map before replay, then reinstate
		// the verified baseline for supported continuation.
		rows := kit.tk.MustQuery("SELECT restore_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = ? ORDER BY segment_id", firstTS).Rows()
		require.Len(t, rows, 1)
		originalMap := []byte(rows[0][1].(string))
		var meta backup.BackupMeta
		require.NoError(t, meta.Unmarshal([]byte(rows[0][1].(string))))
		found := false
		for _, db := range meta.DbMaps {
			if db.SourceName == "rn_move" {
				require.False(t, db.SchemaRouted)
				db.SourceName = ""
				found = true
			}
		}
		require.True(t, found)
		data, err := meta.Marshal()
		require.NoError(t, err)
		kit.tk.MustExec("UPDATE mysql.tidb_pitr_id_map SET id_map = ? WHERE restored_ts = ? AND restore_id = ? AND segment_id = 0", data, firstTS, rows[0][0])
		kit.WithChecker(func(err error) {
			require.ErrorContains(t, err, "cannot resolve source identity")
		}, func() {
			kit.RunStreamRestore(func(rc *task.RestoreConfig) {
				configure(rc)
				rc.FullBackupStorage = ""
				rc.StartTS, rc.RestoreTS = firstTS, secondTS
			})
		})
		kit.tk.MustQuery("SELECT * FROM rn_target.alias").Check(testkit.Rows("1"))
		kit.tk.MustExec("UPDATE mysql.tidb_pitr_id_map SET id_map = ? WHERE restored_ts = ? AND restore_id = ? AND segment_id = 0", originalMap, firstTS, rows[0][0])

		kit.RunStreamRestore(func(rc *task.RestoreConfig) {
			configure(rc)
			rc.FullBackupStorage = ""
			rc.StartTS, rc.RestoreTS = firstTS, secondTS
		})
		kit.tk.MustQuery("SELECT * FROM rn_target.copy").Check(testkit.Rows("2"))
		kit.tk.MustQuery("SELECT * FROM rn_target.recreated").Check(testkit.Rows("2"))
		kit.tk.MustQuery("SELECT * FROM rn_target.alias").Check(testkit.Rows("3"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'rn_split_schema'").Check(testkit.Rows("keep"))
		kit.tk.MustQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'rn_target' ORDER BY table_name").Check(testkit.Rows("alias", "copy", "recreated"))
		kit.tk.MustQuery("SELECT COUNT(DISTINCT restore_id) FROM mysql.tidb_pitr_id_map WHERE restored_ts IN (?, ?)", firstTS, secondTS).Check(testkit.Rows("2"))

		kit.WithChecker(func(err error) {
			require.ErrorContains(t, err, "same restore target")
		}, func() {
			kit.RunStreamRestore(func(rc *task.RestoreConfig) {
				configure(rc)
				rc.FullBackupStorage = ""
				rc.StartTS, rc.RestoreTS = secondTS, conflictTS
			})
		})
		// The failed interval must leave the previously restored table intact.
		kit.tk.MustQuery("SELECT * FROM rn_move.conflict").Check(testkit.Rows("9"))
		kit.tk.MustQuery("SELECT * FROM rn_target.copy").Check(testkit.Rows("2"))
	})
}

func TestPiTRAndManyBackups(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	s.insertSimpleIncreaseData(kit)

	taskName := t.Name()

	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s.tbl))
		bc.Storage = kit.LocalURI("fulla")
	})
	s.cleanSimpleData(kit)

	s2 := kit.simpleWorkload()
	s2.tbl += "2"
	s2.createSimpleTableWithData(kit)
	s2.insertSimpleIncreaseData(kit)
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s2.tbl))
		bc.Storage = kit.LocalURI("fullb")
	})
	s2.cleanSimpleData(kit)

	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.Storage = kit.LocalURI("pitr_base")
		bc.BackupTS = ts
	})
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {
		sc.StartTS = ts
	})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {
		rc.Storage = kit.LocalURI("fulla")
		kit.SetFilter(&rc.Config, fmt.Sprintf("test.%s", s.tbl))
	})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {
		rc.Storage = kit.LocalURI("fullb")
		kit.SetFilter(&rc.Config, fmt.Sprintf("test.%s", s2.tbl))
	})

	kit.forceFlushAndWait(taskName)
	s.cleanSimpleData(kit)
	s2.cleanSimpleData(kit)
	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		rc.FullBackupStorage = kit.LocalURI("pitr_base")
	})
	s.verifySimpleData(kit)
	s2.verifySimpleData(kit)
}

func TestPiTRAndEncryptedFullBackup(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	keyContent, err := hex.DecodeString("9d4cf8f268514d2c38836197008eded1050a5806afa632f7ab1e313bb6697da2")
	require.NoError(t, err)

	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.CipherInfo = backup.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  keyContent,
		}
	})

	s.cleanSimpleData(kit)
	kit.RunLogStart(t.Name(), func(sc *task.StreamConfig) {})
	chk := func(err error) { require.ErrorContains(t, err, "the data you want to restore is encrypted") }
	kit.WithChecker(chk, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {
			rc.CipherInfo = backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  keyContent,
			}
		})
	})
}

func TestPiTRAndEncryptedLogBackup(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)

	keyContent, err := hex.DecodeString("0ae31c060ff933cabe842430e1716185cc9c6b5cdde8e56976afaff41b92528f")
	require.NoError(t, err)
	keyFile := kit.tempFile("KEY", keyContent)

	kit.RunFullBackup(func(bc *task.BackupConfig) {})
	s.cleanSimpleData(kit)

	kit.RunLogStart(t.Name(), func(sc *task.StreamConfig) {
		sc.MasterKeyConfig.EncryptionType = encryptionpb.EncryptionMethod_AES256_CTR
		sc.MasterKeyConfig.MasterKeys = append(sc.MasterKeyConfig.MasterKeys, &encryptionpb.MasterKey{
			Backend: &encryptionpb.MasterKey_File{
				File: &encryptionpb.MasterKeyFile{
					Path: keyFile,
				},
			},
		})
	})

	chk := func(err error) { require.ErrorContains(t, err, "the running log backup task is encrypted") }
	kit.WithChecker(chk, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {})
	})
}

func TestPiTRAndBothEncrypted(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)

	keyContent, err := hex.DecodeString("319b4a104651746f1bf1ad67c9ba7d635d8c4769b03f3e5c63f1da93891ce4f9")
	require.NoError(t, err)
	keyFile := kit.tempFile("KEY", keyContent)

	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.CipherInfo = backup.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  keyContent,
		}
	})
	s.cleanSimpleData(kit)

	kit.RunLogStart(t.Name(), func(sc *task.StreamConfig) {
		sc.MasterKeyConfig.EncryptionType = encryptionpb.EncryptionMethod_AES256_CTR
		sc.MasterKeyConfig.MasterKeys = append(sc.MasterKeyConfig.MasterKeys, &encryptionpb.MasterKey{
			Backend: &encryptionpb.MasterKey_File{
				File: &encryptionpb.MasterKeyFile{
					Path: keyFile,
				},
			},
		})
	})

	chk := func(err error) { require.ErrorContains(t, err, "encrypted") }
	kit.WithChecker(chk, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {
			rc.CipherInfo = backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  keyContent,
			}
		})
	})
}

func TestPiTRAndFailureRestore(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	s.insertSimpleIncreaseData(kit)

	taskName := t.Name()
	kit.RunFullBackup(func(bc *task.BackupConfig) {})
	s.cleanSimpleData(kit)

	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.Storage = kit.LocalURI("full2")
		bc.BackupTS = ts
	})
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {
		sc.StartTS = ts
	})
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/task/run-snapshot-restore-about-to-finish", func(e *error) {
		*e = errors.New("not my fault")
	}))
	checker := func(e error) { require.Error(t, e) }
	kit.WithChecker(checker, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {
			rc.UseCheckpoint = false
		})
	})
	kit.forceFlushAndWait(taskName)

	s.cleanSimpleData(kit)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/run-snapshot-restore-about-to-finish"))

	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		rc.FullBackupStorage = kit.LocalURI("full2")
	})
	res := kit.tk.MustQuery(fmt.Sprintf("SELECT COUNT(*) FROM test.%s", t.Name()))
	res.Check([][]any{{"0"}})
}

func TestPiTRAndIncrementalRestore(t *testing.T) {
	kit := NewLogBackupKit(t)
	s := kit.simpleWorkload()
	s.createSimpleTableWithData(kit)
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s.tbl))
	})
	s.insertSimpleIncreaseData(kit)
	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		kit.SetFilter(&bc.Config, fmt.Sprintf("test.%s", s.tbl))
		bc.Storage = kit.LocalURI("incr-legacy")
		bc.LastBackupTS = ts
	})
	s.cleanSimpleData(kit)

	kit.RunLogStart("dummy", func(sc *task.StreamConfig) {})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {})
	chk := func(err error) { require.ErrorContains(t, err, "BR:Stream:ErrStreamLogTaskExist") }
	kit.WithChecker(chk, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {
			rc.Storage = kit.LocalURI("incr-legacy")
		})
	})
}

func TestPiTRPauseMessage(t *testing.T) {
	kit := NewLogBackupKit(t)
	kit.RunLogStart("nothing", func(sc *task.StreamConfig) {})
	kit.RunLogPause("nothing", func(sc *task.StreamConfig) {
		sc.Message = "nothing paused"
	})
	s := kit.RunLogStatus(func(sc *task.StreamConfig) {})
	require.Len(t, s, 1)
	pl, err := s[0].PauseV2.GetPayload()
	require.NoError(t, err)
	hn, err := os.Hostname()
	require.NoError(t, err)
	require.Equal(t, s[0].StatusString(), "PAUSE")
	require.Equal(t, s[0].PauseV2.OperatorHostName, hn)
	require.Equal(t, pl.(string), "nothing paused")

	kit.StopTaskIfExists("nothing")
	kit.RunLogStart("nothing2", func(sc *task.StreamConfig) {})
	kit.RunLogPause("nothing2", func(sc *task.StreamConfig) {
		sc.AsError = true
		sc.Message = "nothing is on fire"
	})
	s = kit.RunLogStatus(func(sc *task.StreamConfig) {})
	require.Len(t, s, 1)
	pl, err = s[0].PauseV2.GetPayload()
	require.NoError(t, err)
	require.Equal(t, s[0].StatusString(), "ERROR")
	require.Equal(t, s[0].PauseV2.OperatorHostName, hn)
	require.Equal(t, pl.(string), "nothing is on fire")
}
