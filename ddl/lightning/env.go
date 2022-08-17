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
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	importThreshold float32 = 0.15
)

// ClusterInfo store cluster info struct
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   uint
	Status uint
}

// Env store lightning global environment.
type Env struct {
	limit int64
	ClusterInfo
	SortPath   string
	diskQuota  int64
	IsInited   bool
	isTestMode bool
}

var (
	// GlobalEnv global lightning environment var.
	GlobalEnv Env
	// BackCtxMgr is the entry for the lightning backfill process.
	BackCtxMgr  backendCtxManager
	maxMemLimit uint64 = 1 * _gb
)

func init() {
	GlobalEnv.limit = 1024 // Init a default value 1024 for limit.
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logutil.BgLogger().Warn(LitErrGetSysLimitErr, zap.String("OS error:", err.Error()), zap.String("Default: ", "1024."))
	} else {
		GlobalEnv.limit = int64(rLimit.Cur)
	}
	GlobalEnv.IsInited = false
	GlobalEnv.diskQuota = variable.DDLDiskQuota.Load()
}

// InitGlobalLightningBackendEnv initialize Lightning execution environment.
func InitGlobalLightningBackendEnv() {
	var (
		bufferSize uint64
		err        error
		diskQuota  int64
	)
	log.SetAppLogger(logutil.BgLogger())
	GlobalEnv.IsInited = false

	cfg := config.GetGlobalConfig()
	GlobalEnv.Port = cfg.Port
	GlobalEnv.Status = cfg.Status.StatusPort
	GlobalEnv.PdAddr = cfg.Path

	// Set Memory usage limitation to 1 GB
	sbz := variable.GetSysVar("sort_buffer_size")
	bufferSize, err = strconv.ParseUint(sbz.Value, 10, 64)
	// If get bufferSize err, then maxMemLimitation is 128 MB
	// Otherwise, the ddl maxMemLimitation is 2 GB
	if err == nil {
		maxMemLimit = bufferSize * 8 * _kb
		logutil.BgLogger().Info(LitInfoSetMemLimit,
			zap.String("Memory limitation set to:", strconv.FormatUint(maxMemLimit, 10)))
	} else {
		logutil.BgLogger().Info(LitWarnGenMemLimit,
			zap.Error(err),
			zap.String("will use default memory limitation:", strconv.FormatUint(maxMemLimit, 10)))
	}
	memRoot := NewMemRootImpl(int64(maxMemLimit), &BackCtxMgr)
	BackCtxMgr.init(memRoot)
	// If Generated sortPath failed, lightning will initial failed.
	// also if the disk quota is not a proper value
	GlobalEnv.SortPath, err = genLightningDataDir(cfg.TempDir, cfg.Port)
	if err != nil {
		logutil.BgLogger().Warn(LitWarnEnvInitFail,
			zap.String("Sort Path Error:", err.Error()),
			zap.String("Lightning is initialized:", strconv.FormatBool(GlobalEnv.IsInited)))
		return
	}

	// If the default path is /tmp/tidb, then set min disk quota to 10 GB.
	if cfg.TempDir == "/tmp/tidb" {
		GlobalEnv.SetMinQuota()
	}

	diskQuota, err = GlobalEnv.parseDiskQuota(variable.DDLDiskQuota.Load())
	if err != nil {
		logutil.BgLogger().Warn(LitWarnEnvInitFail,
			zap.String("Sort Path disk quota:", err.Error()),
			zap.String("Lightning is initialized:", strconv.FormatBool(GlobalEnv.IsInited)),
			zap.String("Return disk quota:", strconv.FormatInt(diskQuota, 10)))
		return
	}

	GlobalEnv.IsInited = true
	logutil.BgLogger().Info(LitInfoEnvInitSucc,
		zap.String("Current memory usage:", strconv.FormatInt(BackCtxMgr.MemRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation set to:", strconv.FormatUint(maxMemLimit, 10)),
		zap.String("Sort Path disk quota:", strconv.FormatInt(GlobalEnv.diskQuota, 10)),
		zap.String("Max open file number:", strconv.FormatInt(GlobalEnv.limit, 10)),
		zap.String("Lightning is initialized:", strconv.FormatBool(GlobalEnv.IsInited)))
}

// DiskStat check total lightning disk usage and storage available space.
func (*Env) DiskStat() (total uint64, available uint64) {
	totalDiskUsed := BackCtxMgr.CheckDiskQuota(GlobalEnv.diskQuota)
	sz, err := lcom.GetStorageSize(GlobalEnv.SortPath)
	if err != nil {
		logutil.BgLogger().Error(LitErrGetStorageQuota,
			zap.String("OS error:", err.Error()),
			zap.String("default disk quota", strconv.FormatInt(GlobalEnv.diskQuota, 10)))
		return uint64(totalDiskUsed), uint64(GlobalEnv.diskQuota)
	}
	return uint64(totalDiskUsed), sz.Available
}

// parseDiskQuota init dist quota for lightning execution environment. it will
// return 0 on err occurs, the quota value when there is no err.
func (l *Env) parseDiskQuota(val int64) (int64, error) {
	sz, err := lcom.GetStorageSize(l.SortPath)
	if err != nil {
		logutil.BgLogger().Error(LitErrGetStorageQuota,
			zap.String("Os error:", err.Error()),
			zap.String("default disk quota", strconv.FormatInt(l.diskQuota, 10)))
		return 0, err
	}

	// If the disk quota is less than 100 GB, then disable lightning.
	if sz.Available < uint64(GlobalEnv.diskQuota) {
		logutil.BgLogger().Error(LitErrDiskQuotaLess,
			zap.String("disk quota", strconv.FormatInt(int64(sz.Available), 10)))
		return 0, errors.New(LitErrDiskQuotaLess)
	}

	// The Dist quota should be 100 GB to 1 PB.
	if val > int64(sz.Available) {
		l.diskQuota = int64(sz.Available)
	} else {
		l.diskQuota = val
	}

	return l.diskQuota, nil
}

// Generate lightning local store dir in TiDB data dir.
// it will append -port to be tmp_ddl suffix.
func genLightningDataDir(sortPath string, port uint) (string, error) {
	sortPathSuffix := "/tmp_ddl-" + strconv.Itoa(int(port))
	sortPath = filepath.Join(sortPath, sortPathSuffix)
	defaultPath := filepath.Join("/tmp/tidb", sortPathSuffix)

	if info, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Error(LitErrStatDirFail, zap.String("Sort path:", sortPath),
				zap.String("Error:", err.Error()))
			return defaultPath, err
		}
	} else if info.IsDir() {
		// Currently remove all dir to clean garbage data.
		// Todo when do checkpoint should change follow logic.
		err := os.RemoveAll(sortPath)
		if err != nil {
			logutil.BgLogger().Error(LitErrDeleteDirFail, zap.String("Sort path:", sortPath),
				zap.String("Error:", err.Error()))
		}
	}

	err := os.MkdirAll(sortPath, 0o700)
	if err != nil {
		err := os.MkdirAll(defaultPath, 0o700)
		if err != nil {
			logutil.BgLogger().Error(LitErrCreateDirFail, zap.String("Sort path:", sortPath),
				zap.String("Error:", err.Error()))
			return defaultPath, err
		}
		return defaultPath, nil
	}
	logutil.BgLogger().Info(LitInfoSortDir, zap.String("data path:", sortPath))
	return sortPath, nil
}

// NeedImportEngineData check whether need import data into TiKV, because disk available space is not enough.
func (l *Env) NeedImportEngineData(usedStorage, availDisk uint64) bool {
	// If Lightning used 85% of diskQuota or there is less than 15% diskQuota left, then should ingest data to TiKV.
	if usedStorage >= uint64((1-importThreshold)*float32(l.diskQuota)) {
		logutil.BgLogger().Info(LitInfoDiskMaxLimit, zap.String("Disk used", strconv.FormatUint(usedStorage, 10)))
		return true
	}
	if availDisk <= uint64(importThreshold*float32(l.diskQuota)) {
		logutil.BgLogger().Info(LitWarnDiskShortage, zap.String("Disk available", strconv.FormatUint(availDisk, 10)))
		return true
	}
	return false
}

// checkAndResetQuota check whether sysVar disk quota is set to a smaller value and adjust according.
func (l *Env) checkAndResetQuota() {
	var newQuota int64 = variable.DDLDiskQuota.Load()
	if newQuota == l.diskQuota {
		return
	}

	sz, err := lcom.GetStorageSize(l.SortPath)
	// 1, When storage has enough volume and also there at least 10% available space.
	// 2, Set a small quota than before.
	if (err != nil && sz.Capacity > uint64(newQuota) && sz.Available > uint64(newQuota/10)) || l.diskQuota >= newQuota {
		logutil.BgLogger().Info(LitInfoDiskQuotaChg, zap.String("Sort Path disk quota change from", strconv.FormatInt(GlobalEnv.diskQuota, 10)),
			zap.String("To:", strconv.FormatInt(newQuota, 10)))
		l.diskQuota = newQuota
	}
}

// SetMinQuota set disk Quota to a low value to let unit test pass.
// Only used for test.
func (l *Env) SetMinQuota() {
	if !l.isTestMode {
		l.diskQuota = 10 * _gb
	}
}

// CheckPiTR check if PiTR enabled or not.
func CheckPiTR(ctx sessionctx.Context) bool {
	return utils.CheckLogBackupEnabled(ctx)
}
