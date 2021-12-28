package sst

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"io/ioutil"
	"sync/atomic"
	"time"
)

var (
	limit       = int64(1024)
	tblId int64 = time.Now().Unix()
)

func genNextTblId() int64 {
	return atomic.AddInt64(&tblId, 1)
}

func init() {
	var rLimit local.Rlim_t
	rLimit, err := local.GetSystemRLimit()
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("GetSystemRLimit err:%s;use default 1024.", err.Error()))
	} else {
		limit = int64(rLimit)
	}
}

type glue_ struct{}

func (_ glue_) OwnsSQLExecutor() bool {
	return false
}
func (_ glue_) GetSQLExecutor() glue.SQLExecutor {
	return nil
}
func (_ glue_) GetDB() (*sql.DB, error) {
	return nil, nil
}
func (_ glue_) GetParser() *parser.Parser {
	return nil
}
func (_ glue_) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}
func (_ glue_) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}
func (_ glue_) OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently
func (_ glue_) Record(string, uint64) {

}

func makeLogger(tag string, engineUUID uuid.UUID) log.Logger {
	obj := logutil.BgLogger().With(
		zap.String("engineTag", tag),
		zap.Stringer("engineUUID", engineUUID),
	)
	return log.Logger{obj}
}

func generateLightningConfig(info ClusterInfo) *config.Config {
	cfg := config.Config{}
	cfg.DefaultVarsForImporterAndLocalBackend()
	name, err := ioutil.TempDir("/tmp/", "lightning")
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("TempDir err:%s.", err.Error()))
		name = "/tmp/lightning"
	}
	// cfg.TikvImporter.RangeConcurrency = 32
	cfg.Checkpoint.Enable = false
	cfg.TikvImporter.SortedKVDir = name
	cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	cfg.TiDB.PdAddr = info.PdAddr
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(info.Status)
	return &cfg
}

func createLocalBackend(ctx context.Context, info ClusterInfo) (backend.Backend, error) {
	cfg := generateLightningConfig(info)
	tls, err := cfg.ToTLS()
	if err != nil {
		return backend.Backend{}, err
	}
	var g glue_
	return local.NewLocalBackend(ctx, tls, cfg, &g, int(limit), nil)
}
