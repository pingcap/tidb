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
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
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

func NewLogBackupKit(t *testing.T) *LogBackupKit {
	tk := initTestKit(t)
	metaCli := streamhelper.NewMetaDataClient(domain.GetDomain(tk.Session()).EtcdClient())
	begin := time.Now()
	// So the cases can finish faster...
	tk.MustExec("set config tikv `log-backup.max-flush-interval` = '30s';")
	t.Cleanup(func() {
		if !t.Failed() {
			log.Info("[TEST.LogBackupKit] success", zap.String("case", t.Name()), zap.Stringer("takes", time.Since(begin)))
		}
	})
	return &LogBackupKit{
		tk:      tk,
		t:       t,
		metaCli: metaCli,
		base:    t.TempDir(),
		checkerF: func(err error) {
			require.NoError(t, err)
		},
	}
}

func (kit *LogBackupKit) tempFile(name string, content []byte) string {
	path := filepath.Join(kit.t.TempDir(), name)
	require.NoError(kit.t, os.WriteFile(path, content, 0o666))
	return path
}

func (kit *LogBackupKit) RunFullRestore(extConfig func(*task.RestoreConfig)) {
	kit.runAndCheck(func(ctx context.Context) error {
		cfg := task.DefaultRestoreConfig(task.DefaultConfig())
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
		cfg.PD = append(cfg.PD, config.GetGlobalConfig().Path)
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
