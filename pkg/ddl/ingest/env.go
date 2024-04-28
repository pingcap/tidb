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

package ingest

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

var (
	// LitBackCtxMgr is the entry for the lightning backfill process.
	LitBackCtxMgr BackendCtxMgr
	// LitMemRoot is used to track the memory usage of the lightning backfill process.
	LitMemRoot MemRoot
	// litDiskRoot is used to track the disk usage of the lightning backfill process.
	litDiskRoot DiskRoot
	// litRLimit is the max open file number of the lightning backfill process.
	litRLimit uint64
	// LitInitialized is the flag indicates whether the lightning backfill process is initialized.
	LitInitialized bool
)

const defaultMemoryQuota = 2 * size.GB

// InitGlobalLightningEnv initialize Lightning backfill environment.
func InitGlobalLightningEnv(filterProcessingJobIDs FilterProcessingJobIDsFunc) {
	log.SetAppLogger(logutil.DDLIngestLogger())
	globalCfg := config.GetGlobalConfig()
	if globalCfg.Store != "tikv" {
		logutil.DDLIngestLogger().Warn(LitWarnEnvInitFail,
			zap.String("storage limitation", "only support TiKV storage"),
			zap.String("current storage", globalCfg.Store),
			zap.Bool("lightning is initialized", LitInitialized))
		return
	}
	sortPath, err := genLightningDataDir()
	if err != nil {
		logutil.DDLIngestLogger().Warn(LitWarnEnvInitFail,
			zap.Error(err),
			zap.Bool("lightning is initialized", LitInitialized))
		return
	}
	memTotal, err := memory.MemTotal()
	if err != nil {
		logutil.DDLIngestLogger().Warn("get total memory fail", zap.Error(err))
		memTotal = defaultMemoryQuota
	} else {
		memTotal = memTotal / 2
	}
	LitBackCtxMgr = NewLitBackendCtxMgr(sortPath, memTotal, filterProcessingJobIDs)
	litRLimit = util.GenRLimit("ddl-ingest")
	LitInitialized = true
	logutil.DDLIngestLogger().Info(LitInfoEnvInitSucc,
		zap.Uint64("memory limitation", memTotal),
		zap.String("disk usage info", litDiskRoot.UsageInfo()),
		zap.Uint64("max open file number", litRLimit),
		zap.Bool("lightning is initialized", LitInitialized))
}

// Generate lightning local store dir in TiDB data dir.
// it will append -port to be tmp_ddl suffix.
func genLightningDataDir() (string, error) {
	tidbCfg := config.GetGlobalConfig()
	sortPathSuffix := "/tmp_ddl-" + strconv.Itoa(int(tidbCfg.Port))
	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix)

	if _, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			logutil.DDLIngestLogger().Error(LitErrStatDirFail,
				zap.String("sort path", sortPath), zap.Error(err))
			return "", err
		}
	}
	err := os.MkdirAll(sortPath, 0o700)
	if err != nil {
		logutil.DDLIngestLogger().Error(LitErrCreateDirFail,
			zap.String("sort path", sortPath), zap.Error(err))
		return "", err
	}
	logutil.DDLIngestLogger().Info(LitInfoSortDir, zap.String("data path", sortPath))
	return sortPath, nil
}

// GenLightningDataDirForTest is only used for test.
var GenLightningDataDirForTest = genLightningDataDir
