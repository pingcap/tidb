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
	"net"
	"runtime"
	"strconv"
	"sync/atomic"

	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lightning "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// ImporterRangeConcurrencyForTest is only used for test.
var ImporterRangeConcurrencyForTest *atomic.Int32

func genConfig(
	ctx context.Context,
	jobSortPath string,
	memRoot MemRoot,
	unique bool,
	resourceGroup string,
	concurrency int,
) (*local.BackendConfig, error) {
	cfg := &local.BackendConfig{
		LocalStoreDir:     jobSortPath,
		ResourceGroupName: resourceGroup,
		MaxConnPerStore:   concurrency,
		WorkerConcurrency: concurrency * 2,
		KeyspaceName:      tidb.GetGlobalKeyspaceName(),
		// We disable the switch TiKV mode feature for now, because the impact is not
		// fully tested.
		ShouldCheckWriteStall: true,

		// lighting default values
		CheckpointEnabled:           true,
		BlockSize:                   lightning.DefaultBlockSize,
		KVWriteBatchSize:            lightning.KVWriteBatchSize,
		RegionSplitBatchSize:        lightning.DefaultRegionSplitBatchSize,
		RegionSplitConcurrency:      runtime.GOMAXPROCS(0),
		MemTableSize:                lightning.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize:     lightning.DefaultLocalWriterMemCacheSize,
		ShouldCheckTiKV:             true,
		MaxOpenFiles:                int(litRLimit),
		PausePDSchedulerScope:       lightning.PausePDSchedulerScopeTable,
		TaskType:                    kvutil.ExplicitTypeDDL,
		DisableAutomaticCompactions: true,
	}
	// Each backend will build a single dir in lightning dir.
	if ImporterRangeConcurrencyForTest != nil {
		cfg.WorkerConcurrency = int(ImporterRangeConcurrencyForTest.Load()) * 2
	}
	adjustImportMemory(ctx, memRoot, cfg)
	if unique {
		cfg.DupeDetectEnabled = true
		cfg.DuplicateDetectOpt = common.DupDetectOpt{ReportErrOnDup: true}
	} else {
		cfg.DupeDetectEnabled = false
	}

	return cfg, nil
}

// CopReadBatchSize is the batch size of coprocessor read.
// It multiplies the tidb_ddl_reorg_batch_size by 10 to avoid
// sending too many cop requests for the same handle range.
func CopReadBatchSize(hintSize int) int {
	if hintSize > 0 {
		return hintSize
	}
	return 10 * int(variable.GetDDLReorgBatchSize())
}

// CopReadChunkPoolSize is the size of chunk pool, which
// represents the max concurrent ongoing coprocessor requests.
// It multiplies the tidb_ddl_reorg_worker_cnt by 10.
func CopReadChunkPoolSize(hintConc int) int {
	if hintConc > 0 {
		return 10 * hintConc
	}
	return 10 * int(variable.GetDDLReorgWorkerCounter())
}

// NewDDLTLS creates a common.TLS from the tidb config for DDL.
func NewDDLTLS() (*common.TLS, error) {
	tidbCfg := tidb.GetGlobalConfig()
	hostPort := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(tidbCfg.Status.StatusPort)))
	return common.NewTLS(
		tidbCfg.Security.ClusterSSLCA,
		tidbCfg.Security.ClusterSSLCert,
		tidbCfg.Security.ClusterSSLKey,
		hostPort,
		nil, nil, nil,
	)
}

var (
	compactMemory      = 1 * size.GB
	compactConcurrency = 4
)

func generateLocalEngineConfig(ts uint64) *backend.EngineConfig {
	return &backend.EngineConfig{
		Local: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   int64(compactMemory),
			CompactConcurrency: compactConcurrency,
			BlockSize:          16 * 1024, // using default for DDL
		},
		TableInfo:   &checkpoints.TidbTableInfo{},
		KeepSortDir: true,
		TS:          ts,
	}
}

// adjustImportMemory adjusts the lightning memory parameters according to the memory root's max limitation.
func adjustImportMemory(ctx context.Context, memRoot MemRoot, cfg *local.BackendConfig) {
	var scale int64
	// Try aggressive resource usage successful.
	if tryAggressiveMemory(ctx, memRoot, cfg) {
		return
	}

	defaultMemSize := int64(int(cfg.LocalWriterMemCacheSize) * cfg.WorkerConcurrency / 2)
	defaultMemSize += 4 * int64(cfg.MemTableSize)
	logutil.Logger(ctx).Info(LitInfoInitMemSetting,
		zap.Int64("local writer memory cache size", cfg.LocalWriterMemCacheSize),
		zap.Int("engine memory cache size", cfg.MemTableSize),
		zap.Int("worker concurrency", cfg.WorkerConcurrency))

	maxLimit := memRoot.MaxMemoryQuota()
	scale = defaultMemSize / maxLimit

	if scale == 1 || scale == 0 {
		return
	}

	cfg.LocalWriterMemCacheSize /= scale
	cfg.MemTableSize /= int(scale)

	logutil.Logger(ctx).Info(LitInfoChgMemSetting,
		zap.Int64("local writer memory cache size", cfg.LocalWriterMemCacheSize),
		zap.Int("engine memory cache size", cfg.MemTableSize),
		zap.Int("worker concurrency", cfg.WorkerConcurrency))
}

// tryAggressiveMemory lightning memory parameters according memory root's max limitation.
func tryAggressiveMemory(ctx context.Context, memRoot MemRoot, cfg *local.BackendConfig) bool {
	var defaultMemSize int64
	defaultMemSize = int64(int(cfg.LocalWriterMemCacheSize) * cfg.WorkerConcurrency / 2)
	defaultMemSize += int64(cfg.MemTableSize)

	if (defaultMemSize + memRoot.CurrentUsage()) > memRoot.MaxMemoryQuota() {
		return false
	}
	logutil.Logger(ctx).Info(LitInfoChgMemSetting,
		zap.Int64("local writer memory cache size", cfg.LocalWriterMemCacheSize),
		zap.Int("engine memory cache size", cfg.MemTableSize),
		zap.Int("worker concurrency", cfg.WorkerConcurrency))
	return true
}

// defaultImportantVariables is used in obtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var defaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864", // 64MB
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
	"tidb_row_format_version": "1",
}
