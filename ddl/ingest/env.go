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

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/size"
	"go.uber.org/zap"
)

var (
	// LitBackCtxMgr is the entry for the lightning backfill process.
	LitBackCtxMgr backendCtxManager
	// LitMemRoot is used to track the memory usage of the lightning backfill process.
	LitMemRoot MemRoot
	// LitDiskRoot is used to track the disk usage of the lightning backfill process.
	LitDiskRoot DiskRoot
	// LitRLimit is the max open file number of the lightning backfill process.
	LitRLimit uint64
	// LitSortPath is the sort path for the lightning backfill process.
	LitSortPath string
	// LitInitialized is the flag indicates whether the lightning backfill process is initialized.
	LitInitialized bool
)

const maxMemoryQuota = 2 * size.GB

// InitGlobalLightningEnv initialize Lightning backfill environment.
func InitGlobalLightningEnv() {
	log.SetAppLogger(logutil.BgLogger())
	globalCfg := config.GetGlobalConfig()
	if globalCfg.Store != "tikv" {
		logutil.BgLogger().Warn(LitWarnEnvInitFail,
			zap.String("storage limitation", "only support TiKV storage"),
			zap.String("current storage", globalCfg.Store),
			zap.Bool("lightning is initialized", LitInitialized))
		return
	}
	sPath, err := genLightningDataDir()
	if err != nil {
		logutil.BgLogger().Warn(LitWarnEnvInitFail, zap.Error(err),
			zap.Bool("lightning is initialized", LitInitialized))
		return
	}
	LitSortPath = sPath
	LitMemRoot = NewMemRootImpl(int64(maxMemoryQuota), &LitBackCtxMgr)
	LitDiskRoot = NewDiskRootImpl(LitSortPath, &LitBackCtxMgr)
	err = LitDiskRoot.UpdateUsageAndQuota()
	if err != nil {
		logutil.BgLogger().Warn(LitErrUpdateDiskStats, zap.Error(err),
			zap.Bool("lightning is initialized", LitInitialized))
		return
	}
	LitBackCtxMgr.init(LitMemRoot, LitDiskRoot)
	LitRLimit = util.GenRLimit()
	LitInitialized = true
	logutil.BgLogger().Info(LitInfoEnvInitSucc,
		zap.Uint64("memory limitation", maxMemoryQuota),
		zap.Uint64("sort path disk quota", LitDiskRoot.MaxQuota()),
		zap.Uint64("max open file number", LitRLimit),
		zap.Bool("lightning is initialized", LitInitialized))
}

// Generate lightning local store dir in TiDB data dir.
// it will append -port to be tmp_ddl suffix.
func genLightningDataDir() (string, error) {
	tidbCfg := config.GetGlobalConfig()
	sortPathSuffix := "/tmp_ddl-" + strconv.Itoa(int(tidbCfg.Port))
	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix)

	if info, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Error(LitErrStatDirFail, zap.String("sort path", sortPath), zap.Error(err))
			return "", err
		}
	} else if info.IsDir() {
		// Currently remove all dir to clean garbage data.
		// TODO: when do checkpoint should change follow logic.
		err := os.RemoveAll(sortPath)
		if err != nil {
			logutil.BgLogger().Error(LitErrDeleteDirFail, zap.String("sort path", sortPath), zap.Error(err))
		}
	}

	err := os.MkdirAll(sortPath, 0o700)
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateDirFail, zap.String("sort path", sortPath), zap.Error(err))
		return "", err
	}
	logutil.BgLogger().Info(LitInfoSortDir, zap.String("data path:", sortPath))
	return sortPath, nil
}

// GenLightningDataDirForTest is only used for test.
var GenLightningDataDirForTest = genLightningDataDir
