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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
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
func InitGlobalLightningEnv(path string) (ok bool) {
	log.SetAppLogger(logutil.DDLIngestLogger())
	globalCfg := config.GetGlobalConfig()
	if globalCfg.Store != config.StoreTypeTiKV {
		logutil.DDLIngestLogger().Warn(LitWarnEnvInitFail,
			zap.String("storage limitation", "only support TiKV storage"),
			zap.Stringer("current storage", globalCfg.Store),
			zap.Bool("lightning is initialized", LitInitialized))
		return false
	}
	memTotal, err := memory.MemTotal()
	if err != nil {
		logutil.DDLIngestLogger().Warn("get total memory fail", zap.Error(err))
		memTotal = defaultMemoryQuota
	} else {
		memTotal = memTotal / 2
	}
	failpoint.Inject("setMemTotalInMB", func(val failpoint.Value) {
		//nolint: forcetypeassert
		i := val.(int)
		memTotal = uint64(i) * size.MB
	})
	LitBackCtxMgr = NewLitBackendCtxMgr(path, memTotal)
	litRLimit = util.GenRLimit("ddl-ingest")
	LitInitialized = true
	logutil.DDLIngestLogger().Info(LitInfoEnvInitSucc,
		zap.Uint64("memory limitation", memTotal),
		zap.String("disk usage info", litDiskRoot.UsageInfo()),
		zap.Uint64("max open file number", litRLimit),
		zap.Bool("lightning is initialized", LitInitialized))
	return true
}

// GenIngestTempDataDir generates a path for DDL ingest.
// Format: ${temp-dir}/tmp_ddl-{port}
func GenIngestTempDataDir() (string, error) {
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

// CleanUpTempDir is used to remove the stale index data.
// This function gets running DDL jobs from `mysql.tidb_ddl_job` and
// it only removes the folders that related to finished jobs.
func CleanUpTempDir(ctx context.Context, se sessionctx.Context, path string) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if strings.Contains(err.Error(), "no such file") {
			return
		}
		logutil.DDLIngestLogger().Warn(LitErrCleanSortPath, zap.Error(err))
		return
	}
	toCheckJobIDs := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		jobID, err := decodeBackendTag(entry.Name())
		if err != nil {
			logutil.DDLIngestLogger().Error(LitErrCleanSortPath, zap.Error(err))
			continue
		}
		toCheckJobIDs[jobID] = struct{}{}
	}

	if len(toCheckJobIDs) == 0 {
		return
	}

	idSlice := maps.Keys(toCheckJobIDs)
	slices.Sort(idSlice)
	processing, err := filterProcessingJobIDs(ctx, sess.NewSession(se), idSlice)
	if err != nil {
		logutil.DDLIngestLogger().Error(LitErrCleanSortPath, zap.Error(err))
		return
	}

	for _, id := range processing {
		delete(toCheckJobIDs, id)
	}

	if len(toCheckJobIDs) == 0 {
		return
	}

	for id := range toCheckJobIDs {
		logutil.DDLIngestLogger().Info("remove stale temp index data",
			zap.Int64("jobID", id))
		p := filepath.Join(path, encodeBackendTag(id))
		err = os.RemoveAll(p)
		if err != nil {
			logutil.DDLIngestLogger().Error(LitErrCleanSortPath, zap.Error(err))
		}
	}
}

func filterProcessingJobIDs(ctx context.Context, se *sess.Session, jobIDs []int64) ([]int64, error) {
	var sb strings.Builder
	for i, id := range jobIDs {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatInt(id, 10))
	}
	sql := fmt.Sprintf(
		"SELECT job_id FROM mysql.tidb_ddl_job WHERE job_id IN (%s)",
		sb.String())
	rows, err := se.Execute(ctx, sql, "filter_processing_job_ids")
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make([]int64, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.GetInt64(0))
	}
	return ret, nil
}
