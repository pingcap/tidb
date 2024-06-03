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
	"strconv"
	"sync/atomic"

	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lightning "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// ImporterRangeConcurrencyForTest is only used for test.
var ImporterRangeConcurrencyForTest *atomic.Int32

// litConfig is the configuration for the lightning local backend used in DDL.
type litConfig struct {
	lightning     *lightning.Config
	keyspaceName  string
	isRaftKV2     bool
	resourceGroup string
}

func genConfig(
	ctx context.Context,
	jobSortPath string,
	memRoot MemRoot,
	unique bool,
	resourceGroup string,
) (*litConfig, error) {
	tidbCfg := tidb.GetGlobalConfig()
	cfg := lightning.NewConfig()
	cfg.TikvImporter.Backend = lightning.BackendLocal
	// Each backend will build a single dir in lightning dir.
	cfg.TikvImporter.SortedKVDir = jobSortPath
	if ImporterRangeConcurrencyForTest != nil {
		cfg.TikvImporter.RangeConcurrency = int(ImporterRangeConcurrencyForTest.Load())
	} else {
		cfg.TikvImporter.RangeConcurrency = int(variable.GetDDLReorgWorkerCounter())
	}
	err := cfg.AdjustForDDL()
	if err != nil {
		logutil.Logger(ctx).Warn(LitWarnConfigError, zap.Error(err))
		return nil, err
	}
	adjustImportMemory(ctx, memRoot, cfg)
	cfg.Checkpoint.Enable = true
	if unique {
		cfg.Conflict.Strategy = lightning.ErrorOnDup
		cfg.Conflict.Threshold = lightning.DefaultRecordDuplicateThreshold
	} else {
		cfg.Conflict.Strategy = lightning.NoneOnDup
	}
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(tidbCfg.Status.StatusPort)
	// Set TLS related information
	cfg.Security.CAPath = tidbCfg.Security.ClusterSSLCA
	cfg.Security.CertPath = tidbCfg.Security.ClusterSSLCert
	cfg.Security.KeyPath = tidbCfg.Security.ClusterSSLKey
	// in DDL scenario, we don't switch import mode
	cfg.Cron.SwitchMode = lightning.Duration{Duration: 0}

	c := &litConfig{
		lightning:     cfg,
		keyspaceName:  tidb.GetGlobalKeyspaceName(),
		isRaftKV2:     false,
		resourceGroup: resourceGroup,
	}

	return c, nil
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
func adjustImportMemory(ctx context.Context, memRoot MemRoot, cfg *lightning.Config) {
	var scale int64
	// Try aggressive resource usage successful.
	if tryAggressiveMemory(ctx, memRoot, cfg) {
		return
	}

	defaultMemSize := int64(cfg.TikvImporter.LocalWriterMemCacheSize) * int64(cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += 4 * int64(cfg.TikvImporter.EngineMemCacheSize)
	logutil.Logger(ctx).Info(LitInfoInitMemSetting,
		zap.Int64("local writer memory cache size", int64(cfg.TikvImporter.LocalWriterMemCacheSize)),
		zap.Int64("engine memory cache size", int64(cfg.TikvImporter.EngineMemCacheSize)),
		zap.Int("range concurrency", cfg.TikvImporter.RangeConcurrency))

	maxLimit := memRoot.MaxMemoryQuota()
	scale = defaultMemSize / maxLimit

	if scale == 1 || scale == 0 {
		return
	}

	cfg.TikvImporter.LocalWriterMemCacheSize /= lightning.ByteSize(scale)
	cfg.TikvImporter.EngineMemCacheSize /= lightning.ByteSize(scale)

	logutil.Logger(ctx).Info(LitInfoChgMemSetting,
		zap.Int64("local writer memory cache size", int64(cfg.TikvImporter.LocalWriterMemCacheSize)),
		zap.Int64("engine memory cache size", int64(cfg.TikvImporter.EngineMemCacheSize)),
		zap.Int("range concurrency", cfg.TikvImporter.RangeConcurrency))
}

// tryAggressiveMemory lightning memory parameters according memory root's max limitation.
func tryAggressiveMemory(ctx context.Context, memRoot MemRoot, cfg *lightning.Config) bool {
	var defaultMemSize int64
	defaultMemSize = int64(int(cfg.TikvImporter.LocalWriterMemCacheSize) * cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += int64(cfg.TikvImporter.EngineMemCacheSize)

	if (defaultMemSize + memRoot.CurrentUsage()) > memRoot.MaxMemoryQuota() {
		return false
	}
	logutil.Logger(ctx).Info(LitInfoChgMemSetting,
		zap.Int64("local writer memory cache size", int64(cfg.TikvImporter.LocalWriterMemCacheSize)),
		zap.Int64("engine memory cache size", int64(cfg.TikvImporter.EngineMemCacheSize)),
		zap.Int("range concurrency", cfg.TikvImporter.RangeConcurrency))
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
