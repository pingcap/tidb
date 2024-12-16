package brietest

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
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
	return gluetidb.WrapSession(tk.tk.Session()), nil
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
	return &glue.CounterProgress{}
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

type LogBackupKit struct {
	t       *testing.T
	tk      *testkit.TestKit
	metaCli *streamhelper.MetaDataClient
	base    string
}

func NewLogBackupKit(t *testing.T) *LogBackupKit {
	tk := initTestKit(t)
	metaCli := streamhelper.NewMetaDataClient(domain.GetDomain(tk.Session()).EtcdClient())
	// So the cases can finish faster...
	tk.MustExec("set config tikv `log-backup.max-flush-interval` = '30s';")
	return &LogBackupKit{
		tk:      tk,
		t:       t,
		metaCli: metaCli,
		base:    t.TempDir(),
	}
}

func (kit *LogBackupKit) RunFullRestore(extConfig func(*task.RestoreConfig)) {
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultRestoreConfig(task.DefaultConfig())
		cfg.Storage = "local://" + kit.base + "/full"
		cfg.FilterStr = []string{"test.*"}
		var err error
		cfg.TableFilter, err = filter.Parse(cfg.FilterStr)
		cfg.CheckRequirements = false
		require.NoError(kit.t, err)

		extConfig(&cfg)
		return task.RunRestore(ctx, kit.Glue(), task.FullRestoreCmd, &cfg)
	})
}

func (kit *LogBackupKit) RunStreamRestore(extConfig func(*task.RestoreConfig)) {
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultRestoreConfig(task.DefaultConfig())
		cfg.Storage = "local://" + kit.base + "/incr"
		cfg.FullBackupStorage = "local://" + kit.base + "/full"
		cfg.CheckRequirements = false

		extConfig(&cfg)
		return task.RunRestore(ctx, kit.Glue(), task.PointRestoreCmd, &cfg)
	})
}

func (kit *LogBackupKit) RunFullBackup(extConfig func(*task.BackupConfig)) {
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultBackupConfig(task.DefaultConfig())
		cfg.Storage = "local://" + kit.base + "/full"
		extConfig(&cfg)
		return task.RunBackup(ctx, kit.Glue(), "backup full[intest]", &cfg)
	})
}

func (kit *LogBackupKit) StopTaskIfExists(taskName string) {
	kit.mustExec(func(ctx context.Context) error {
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
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamStartFlags)
		cfg.Storage = "local://" + kit.base + "/incr"
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

func (kit *LogBackupKit) shouldPanic(checker func(v any), f func()) {
	defer func() {
		v := recover()
		require.NotNil(kit.t, v, "should panic not panic")
		checker(v)
	}()

	f()
}

func (kit *LogBackupKit) mustExec(f func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	err := f(ctx)
	cancel()
	require.NoError(kit.t, err)
}

func (kit *LogBackupKit) forceFlush() {
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultConfig()
		cfg.PD = append(cfg.PD, config.GetGlobalConfig().Path)
		err := operator.RunForceFlush(ctx, &operator.ForceFlushConfig{
			Config: cfg,
		})
		if err != nil {
			log.Warn("It seems this version of TiKV doesn't support force flush, the test may be much more slower.",
				logutil.ShortError(err))
		}
		return nil
	})
}

func (kit *LogBackupKit) forceFlushAndWait(taskName string) {
	ts := kit.TSO()
	kit.forceFlush()
	require.Eventually(kit.t, func() bool {
		ckpt := kit.CheckpointTSOf(taskName)
		log.Info("checkpoint", zap.Uint64("checkpoint", ckpt), zap.Uint64("ts", ts))
		return ckpt >= ts
	}, 300*time.Second, 1*time.Second)
	time.Sleep(6 * time.Second) // Wait the storage checkpoint uploaded...
}

func createSimpleTableWithData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("DROP TABLE IF EXISTs test.%s", kit.t.Name()))
	kit.tk.MustExec(fmt.Sprintf("CREATE TABLE test.%s(t text)", kit.t.Name()))
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Ear'), ('Eye'), ('Nose')", kit.t.Name()))
}

func insertSimpleIncreaseData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Body')", kit.t.Name()))
	kit.tk.MustExec(fmt.Sprintf("INSERT INTO test.%s VALUES ('Mind')", kit.t.Name()))
}

func verifySimpleData(kit *LogBackupKit) {
	kit.tk.MustQuery(fmt.Sprintf("SELECT * FROM test.%s", kit.t.Name())).Check([][]any{{"Ear"}, {"Eye"}, {"Nose"}, {"Body"}, {"Mind"}})
}

func cleanSimpleData(kit *LogBackupKit) {
	kit.tk.MustExec(fmt.Sprintf("DROP TABLE test.%s", kit.t.Name()))
}

func TestPiTR(t *testing.T) {
	kit := NewLogBackupKit(t)

	taskName := "simple"
	createSimpleTableWithData(kit)

	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) { bc.BackupTS = ts })
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) { sc.StartTS = ts })

	insertSimpleIncreaseData(kit)

	kit.forceFlushAndWait(taskName)
	cleanSimpleData(kit)

	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {})
	verifySimpleData(kit)
}

func TestPiTRAndBackup(t *testing.T) {
	kit := NewLogBackupKit(t)
	createSimpleTableWithData(kit)
	insertSimpleIncreaseData(kit)

	taskName := t.Name()

	kit.RunFullBackup(func(bc *task.BackupConfig) {})
	cleanSimpleData(kit)

	ts := kit.TSO()
	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.Storage = "local://" + kit.base + "/full2"
		bc.BackupTS = ts
	})
	kit.RunLogStart(taskName, func(sc *task.StreamConfig) {
		sc.StartTS = ts
	})
	kit.RunFullRestore(func(rc *task.RestoreConfig) {})

	kit.forceFlushAndWait(taskName)
	cleanSimpleData(kit)
	kit.StopTaskIfExists(taskName)
	kit.RunStreamRestore(func(rc *task.RestoreConfig) {
		rc.FullBackupStorage = "local://" + kit.base + "/full2"
	})
	verifySimpleData(kit)
}

func TestEncryptedBackup(t *testing.T) {
	kit := NewLogBackupKit(t)
	createSimpleTableWithData(kit)
	keyContent, _ := hex.DecodeString("9d4cf8f268514d2c38836197008eded1050a5806afa632f7ab1e313bb6697da2")

	kit.RunFullBackup(func(bc *task.BackupConfig) {
		bc.CipherInfo = backup.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  keyContent,
		}
	})

	cleanSimpleData(kit)
	kit.RunLogStart("something", func(sc *task.StreamConfig) {})
	kit.shouldPanic(func(v any) { fmt.Println(v) }, func() {
		kit.RunFullRestore(func(rc *task.RestoreConfig) {
			rc.CipherInfo = backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  keyContent,
			}
		})
	})
}
