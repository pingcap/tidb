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
	"path/filepath"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbconf "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

type BackendContext struct {
	Key         string // Currently, backend key used ddl job id string
	Backend     *backend.Backend
	Ctx         context.Context
	cfg         *config.Config
	EngineCache map[string]*engineInfo
	sysVars     map[string]string
	enabled     bool
	needRestore bool
}

func newBackendContext(key string, be *backend.Backend, ctx context.Context, cfg *config.Config, vars map[string]string) *BackendContext {
    return &BackendContext{
		Key: key,
		Backend: be,
		Ctx: ctx,
		cfg: cfg,
		EngineCache: make(map[string]*engineInfo, 10),
		sysVars: vars,
	}
}

func generateLightningConfig(ctx context.Context, unique bool, bcKey string) (*config.Config, error) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	// Each backend will build an single dir in linghtning dir.
	cfg.TikvImporter.SortedKVDir = filepath.Join(GlobalLightningEnv.SortPath, bcKey)
	// Should not output err, after go through cfg.adjust function.
	_, err := cfg.AdjustCommon()
	if err != nil {
		log.L().Warn(LWAR_CONFIG_ERROR, zap.Error(err))
		return nil, err
	}
	adjustImportMemory(cfg)
	cfg.Checkpoint.Enable = true
	if unique {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	} else {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	}
	cfg.TiDB.PdAddr = GlobalLightningEnv.PdAddr
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(GlobalLightningEnv.Status)
	cfg.Security.CAPath = tidbconf.GetGlobalConfig().Security.ClusterSSLCA
	cfg.Security.CertPath = tidbconf.GetGlobalConfig().Security.ClusterSSLCert
	cfg.Security.KeyPath = tidbconf.GetGlobalConfig().Security.ClusterSSLKey

	return cfg, err
}

func createLocalBackend(ctx context.Context, cfg *config.Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		log.L().Error(LERR_CREATE_BACKEND_FAILED, zap.Error(err))
		return backend.Backend{}, err
	}

	return local.NewLocalBackend(ctx, tls, cfg, glue, int(GlobalLightningEnv.limit), nil)
}

func CloseBackend(bcKey string) {
	log.L().Info(LINFO_CLOSE_BACKEND, zap.String("backend key", bcKey))
	GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	return
}

func GenBackendContextKey(jobId int64) string {
	return strconv.FormatInt(jobId, 10)
}

// Adjust lightning memory parameters according memory root's max limitation
func adjustImportMemory(cfg *config.Config) {
	var scale int64
	defaultMemSize := int64(cfg.TikvImporter.LocalWriterMemCacheSize) * int64(cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += 4 * int64(cfg.TikvImporter.EngineMemCacheSize)
	log.L().Info(LINFO_INIT_MEM_SETTING,
		zap.String("LocalWriterMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("EngineMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("rangecounrrency:", strconv.Itoa(cfg.TikvImporter.RangeConcurrency)))

	if defaultMemSize > GlobalLightningEnv.LitMemRoot.maxLimit {
		scale = defaultMemSize / GlobalLightningEnv.LitMemRoot.maxLimit
	}

	// scale equal to 1 means there is no need to adjust memory settings for lightning.
	// 0 means defaultMemSize is less than memory maxLimit for Lightning, no need to adjust.
	if scale == 1 || scale == 0 {
		return
	}

	cfg.TikvImporter.LocalWriterMemCacheSize /= config.ByteSize(scale)
	cfg.TikvImporter.EngineMemCacheSize /= config.ByteSize(scale)
	// ToDo adjust rangecourrency nubmer to control total concurrency in future.
	log.L().Info(LINFO_CHG_MEM_SETTING,
		zap.String("LocalWriterMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("EngineMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("rangecounrrency:", strconv.Itoa(cfg.TikvImporter.RangeConcurrency)))
	return
}

type glue_lit struct{}

func (_ glue_lit) OwnsSQLExecutor() bool {
	return false
}
func (_ glue_lit) GetSQLExecutor() glue.SQLExecutor {
	return nil
}
func (_ glue_lit) GetDB() (*sql.DB, error) {
	return nil, nil
}
func (_ glue_lit) GetParser() *parser.Parser {
	return nil
}
func (_ glue_lit) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}
func (_ glue_lit) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}
func (_ glue_lit) OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently
func (_ glue_lit) Record(string, uint64) {

}

func IsEngineLightningBackfill(id int64) bool {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalLightningEnv.LitMemRoot.getBackendContext(bcKey)
	if !exist {
		return false 
	} else {
		return bc.enabled
	}
}

func SetEnable(id int64, value bool) {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalLightningEnv.LitMemRoot.getBackendContext(bcKey)
	if exist {
		bc.enabled = value
	}
}

func NeedRestore(id int64) bool {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalLightningEnv.LitMemRoot.getBackendContext(bcKey)
	if !exist {
		return false 
	} else {
		return bc.needRestore
	}
}

func SetNeedRestore(id int64, value bool) {
	bcKey := GenBackendContextKey(id)
	bc, exist := GlobalLightningEnv.LitMemRoot.getBackendContext(bcKey)
	if exist {
		bc.needRestore = value
	}
}