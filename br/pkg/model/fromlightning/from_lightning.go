package fromlightning

import (
	"context"
	"database/sql"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	lightcommon "github.com/pingcap/tidb/br/pkg/lightning/common"
	lightcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	lightlog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
)

func RefLightning() (backend.Backend, error) {
	// refer lightning.
	lightlog.SetAppLogger(logutil.BgLogger())
	var minState tikv.StoreState
	_ = minState
	cfg := lightcfg.NewConfig()
	var tls *lightcommon.TLS
	return local.NewLocalBackend(nil, tls, cfg, nil, 0, nil)
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
func (_ glue_) OpenCheckpointsDB(context.Context, *lightcfg.Config) (checkpoints.DB, error) {
	return nil, nil
}
