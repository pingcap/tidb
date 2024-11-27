package brietest

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/printer"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
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
func (tk TestKitGlue) Record(name string, value uint64) {
}

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
	return &LogBackupKit{
		tk:      tk,
		t:       t,
		metaCli: metaCli,
		base:    t.TempDir(),
	}
}

func (kit *LogBackupKit) RunLogStart(taskName string) {
	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamCommonFlags)
		cfg.TaskName = taskName
		return task.RunStreamStop(ctx, kit.Glue(), "stream stop[intest]", &cfg)
	})

	kit.mustExec(func(ctx context.Context) error {
		cfg := task.DefaultStreamConfig(task.DefineStreamStartFlags)
		cfg.Storage = "local://" + kit.base
		cfg.TaskName = taskName
		cfg.EndTS = math.MaxUint64
		err := task.RunStreamStart(ctx, kit.Glue(), "stream start[intest]", &cfg)
		return err
	})
}

func (kit *LogBackupKit) RunLogStatus(taskName string) {

}

func (kit *LogBackupKit) Glue() glue.Glue {
	return &TestKitGlue{tk: kit.tk}
}

func (kit *LogBackupKit) mustExec(f func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	err := f(ctx)
	cancel()
	require.NoError(kit.t, err)
}

func TestPiTR(t *testing.T) {
	kit := NewLogBackupKit(t)
	kit.RunLogStart("fiolvit")
}
