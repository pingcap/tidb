// Copyright 2026 PingCAP, Inc.
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

package local

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

// ImportIngestEngine imports an immutable data source without registering a local
// engine or writing local SSTs. The caller owns e, must keep its data alive until
// this call returns (including on cancellation), and closes it afterwards.
func (local *Backend) ImportIngestEngine(ctx context.Context, e common.Engine, regionSplitSize, regionSplitKeys int64) error {
	kvRegionSplitSize, kvRegionSplitKeys, err := GetRegionSplitSizeKeys(ctx, local.pdCli, local.tls)
	if err == nil {
		if kvRegionSplitSize > regionSplitSize {
			regionSplitSize = kvRegionSplitSize
		}
		if kvRegionSplitKeys > regionSplitKeys {
			regionSplitKeys = kvRegionSplitKeys
		}
	} else {
		log.FromContext(ctx).Warn("fail to get region split keys and size", zap.Error(err))
	}

	if localEngine, ok := e.(*Engine); ok {
		localEngine.regionSplitSize = regionSplitSize
		localEngine.regionSplitKeyCnt = regionSplitKeys
	}

	lfTotalSize, lfLength := e.KVStatistics()
	if lfTotalSize == 0 {
		// engine is empty, this is likes because it's a index engine but the table contains no index
		log.FromContext(ctx).Info("engine contains no kv, skip import", zap.String("engine", e.ID()))
		return nil
	}

	// split sorted file into range about regionSplitSize per file
	splitKeys, err := getRegionSplitKeys(ctx, e, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}
	intest.Assert(len(splitKeys) > 0)
	startKey, endKey := splitKeys[0], splitKeys[len(splitKeys)-1]

	if len(startKey) > 0 && len(endKey) > 0 {
		log.FromContext(ctx).Info("force table split range",
			zap.String("startKey", redact.Key(startKey)),
			zap.String("endKey", redact.Key(endKey)))
		stores, err := local.pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
		if err != nil {
			return err
		}
		removeTableSplitRange := local.forceTableSplitRange(ctx, startKey, endKey, stores)
		defer removeTableSplitRange()
	}

	if local.PausePDSchedulerScope == config.PausePDSchedulerScopeTable {
		log.FromContext(ctx).Info("pause pd scheduler of table scope")
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var startKey, endKey []byte
		if len(splitKeys[0]) > 0 {
			startKey = codec.EncodeBytes(nil, splitKeys[0])
		}
		if len(splitKeys[len(splitKeys)-1]) > 0 {
			endKey = codec.EncodeBytes(nil, splitKeys[len(splitKeys)-1])
		}
		done, err := pdutil.PauseSchedulersByKeyRange(subCtx, local.pdHTTPCli, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	if local.BackendConfig.RaftKV2SwitchModeDuration > 0 {
		log.FromContext(ctx).Info("switch import mode of ranges",
			zap.String("startKey", redact.Key(startKey)),
			zap.String("endKey", redact.Key(endKey)))
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done, err := local.switchModeBySplitKeys(subCtx, splitKeys)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			cancel()
			<-done
		}()
	}

	maxReqInFlight := GetMaxIngestConcurrency()
	maxReqPerSec := GetMaxIngestPerSec()
	local.updateIngestLimiter(maxReqInFlight, maxReqPerSec)
	log.FromContext(ctx).Info("start import engine",
		zap.String("uuid", e.ID()),
		zap.Int("region ranges", len(splitKeys)-1),
		zap.Int64("count", lfLength),
		zap.Int64("size", lfTotalSize),
		zap.Int("maxReqInFlight", maxReqInFlight),
		zap.Float64("maxReqPerSec", maxReqPerSec),
	)

	failpoint.Inject("ReadyForImportEngine", func() {})

	err = local.doImport(ctx, e, splitKeys, regionSplitSize, regionSplitKeys)
	if err == nil {
		importedSize, importedLength := e.ImportedStatistics()

		if err := verifyImportedStatistics(e, importedLength); err != nil {
			return err
		}

		log.FromContext(ctx).Info("import engine success",
			zap.String("uuid", e.ID()),
			zap.Int64("size", lfTotalSize),
			zap.Int64("kvs", lfLength),
			zap.Int64("importedSize", importedSize),
			zap.Int64("importedCount", importedLength))
	}
	return err
}
