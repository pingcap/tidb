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

package lightning

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

// BackendContext store a backend info for add index reorg task.
type BackendContext struct {
	key         string // Currently, backend key used ddl job id string
	backend     *backend.Backend
	ctx         context.Context
	cfg         *config.Config
	engineCache map[string]*engineInfo
	sysVars     map[string]string
	enabled     bool
	needRestore bool
}

func newBackendContext(ctx context.Context, key string, be *backend.Backend, cfg *config.Config, vars map[string]string) *BackendContext {
	return &BackendContext{
		key:         key,
		backend:     be,
		ctx:         ctx,
		cfg:         cfg,
		engineCache: make(map[string]*engineInfo, 10),
		sysVars:     vars,
	}
}

func createLocalBackend(ctx context.Context, cfg *config.Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		log.L().Error(LitErrCreateBackendFail, zap.Error(err))
		return backend.Backend{}, err
	}

	errorMgr := errormanager.New(nil, cfg, log.FromContext(ctx))
	return local.NewLocalBackend(ctx, tls, cfg, glue, int(GlobalEnv.limit), errorMgr)
}

// GenBackendContextKey generate a backend key from job id for a DDL job.
func GenBackendContextKey(jobID int64) string {
	return strconv.FormatInt(jobID, 10)
}

type glueLit struct{}

// OwnsSQLExecutor Implement interface OwnsSQLExecutor.
func (g glueLit) OwnsSQLExecutor() bool {
	return false
}

// GetSQLExecutor Implement interface GetSQLExecutor.
func (g glueLit) GetSQLExecutor() glue.SQLExecutor {
	return nil
}

// GetDB Implement interface GetDB.
func (g glueLit) GetDB() (*sql.DB, error) {
	return nil, nil
}

// GetParser Implement interface GetParser.
func (g glueLit) GetParser() *parser.Parser {
	return nil
}

// GetTables Implement interface GetTables.
func (g glueLit) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

// GetSession Implement interface GetSession.
func (g glueLit) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}

// OpenCheckpointsDB Implement interface OpenCheckpointsDB.
func (g glueLit) OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently.
func (g glueLit) Record(string, uint64) {
}

// IsEngineLightningBackfill show if lightning backend env is set up.
func IsEngineLightningBackfill(id int64) bool {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, false)
	if !exist {
		return false
	}
	return bc.enabled
}

// SetEnable set backend status.
func SetEnable(id int64, value bool) {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, false)
	if exist {
		bc.enabled = value
	}
}

// NeedRestore shows if engine is created.
func NeedRestore(id int64) bool {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, false)
	if !exist {
		return false
	}
	return bc.needRestore
}

// SetNeedRestore set engine status.
func SetNeedRestore(id int64, value bool) {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, false)
	if exist {
		bc.needRestore = value
	}
}
