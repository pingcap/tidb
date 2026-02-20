// Copyright 2020 PingCAP, Inc.
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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// forceTableSplitRange turns on force_partition_range for importing.
// See https://github.com/tikv/tikv/pull/18866 for detail.
// It returns a resetter to turn off.
func (local *Backend) forceTableSplitRange(ctx context.Context,
	startKey, endKey kv.Key, stores []*metapb.Store) (resetter func()) {
	subctx, cancel := context.WithCancel(ctx)
	var wg util.WaitGroupWrapper
	clients := make([]sst.ImportSSTClient, 0, len(stores))
	storeAddrs := make([]string, 0, len(stores))
	const ttlSecond = uint64(3600) // default 1 hour
	addReq := &sst.AddPartitionRangeRequest{
		Range: &sst.Range{
			Start: startKey,
			End:   endKey,
		},
		TtlSeconds: ttlSecond,
	}
	removeReq := &sst.RemovePartitionRangeRequest{
		Range: &sst.Range{
			Start: startKey,
			End:   endKey,
		},
	}

	for _, store := range stores {
		if store.StatusAddress == "" || engine.IsTiFlash(store) {
			continue
		}
		importCli, err := local.importClientFactory.create(subctx, store.Id)
		if err != nil {
			tidblogutil.Logger(subctx).Warn("create import client failed", zap.Error(err), zap.String("store", store.StatusAddress))
			continue
		}
		clients = append(clients, importCli)
		storeAddrs = append(storeAddrs, store.StatusAddress)
	}

	addTableSplitRange := func() {
		var (
			firstErr      error
			mu            sync.Mutex
			successStores = make([]string, 0, len(clients))
			failedStores  = make([]string, 0, len(clients))
		)
		concurrency := int(local.WorkerConcurrency.Load())
		if concurrency <= 0 {
			concurrency = 1
		}
		eg, _ := util.NewErrorGroupWithRecoverWithCtx(subctx)
		eg.SetLimit(concurrency)
		for i, c := range clients {
			client, addr := c, storeAddrs[i]
			eg.Go(func() error {
				failpoint.InjectCall("AddPartitionRangeForTable")
				_, err := client.AddForcePartitionRange(subctx, addReq)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successStores = append(successStores, addr)
				} else {
					failedStores = append(failedStores, addr)
					if firstErr == nil {
						firstErr = err
					}
				}
				return nil
			})
		}
		_ = eg.Wait()
		tidblogutil.Logger(subctx).Info("call AddForcePartitionRange",
			zap.Strings("success stores", successStores),
			zap.Strings("failed stores", failedStores),
			zap.Error(firstErr),
		)
	}

	addTableSplitRange()
	wg.Run(func() {
		timeout := time.Duration(ttlSecond) * time.Second / 5 // 12min
		ticker := time.NewTicker(timeout)
		defer ticker.Stop()
		for {
			select {
			case <-subctx.Done():
				return
			case <-ticker.C:
				addTableSplitRange()
			}
		}
	})

	resetter = func() {
		cancel()
		wg.Wait()

		var (
			firstErr      error
			mu            sync.Mutex
			successStores = make([]string, 0, len(clients))
			failedStores  = make([]string, 0, len(clients))
		)
		concurrency := int(local.WorkerConcurrency.Load())
		if concurrency <= 0 {
			concurrency = 1
		}
		eg, _ := util.NewErrorGroupWithRecoverWithCtx(ctx)
		eg.SetLimit(concurrency)
		for i, c := range clients {
			client, addr := c, storeAddrs[i]
			eg.Go(func() error {
				failpoint.InjectCall("RemovePartitionRangeRequest")
				_, err := client.RemoveForcePartitionRange(ctx, removeReq)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successStores = append(successStores, addr)
				} else {
					failedStores = append(failedStores, addr)
					if firstErr == nil {
						firstErr = err
					}
				}
				return nil
			})
		}
		_ = eg.Wait()
		tidblogutil.Logger(ctx).Info("call RemoveForcePartitionRange",
			zap.Strings("success stores", successStores),
			zap.Strings("failed stores", failedStores),
			zap.Error(firstErr),
		)
	}
	return resetter
}
