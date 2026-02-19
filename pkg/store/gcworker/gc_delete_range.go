// Copyright 2017 PingCAP, Inc.
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

package gcworker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/metrics"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.uber.org/zap"
)

// deleteRanges processes all delete range records whose ts < safePoint in table `gc_delete_range`
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) deleteRanges(
	ctx context.Context,
	safePoint uint64,
	concurrency gcConcurrency,
) error {
	metrics.GCWorkerCounter.WithLabelValues("delete_range").Inc()

	s := createSession(w.store)
	defer s.Close()
	ranges, err := util.LoadDeleteRanges(ctx, s, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	v2, err := util.IsRaftKv2(ctx, s)
	if err != nil {
		return errors.Trace(err)
	}
	// Cache table ids on which placement rules have been GC-ed, to avoid redundantly GC the same table id multiple times.
	var gcPlacementRuleCache sync.Map

	logutil.Logger(ctx).Info("start delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()

	deleteRangeConcurrency := w.calcDeleteRangeConcurrency(concurrency, len(ranges))
	concurrencyLimiter := make(chan struct{}, deleteRangeConcurrency)

	f := func(r util.DelRangeTask) {
		var err error
		defer func() {
			<-concurrencyLimiter
		}()
		se := createSession(w.store)
		defer se.Close()

		startKey, endKey := r.Range()
		if v2 {
			// In raftstore-v2, we use delete range instead to avoid deletion omission
			task := rangetask.NewDeleteRangeTask(w.tikvStore, startKey, endKey, deleteRangeConcurrency)
			err = task.Execute(ctx)
		} else {
			err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey)
		}
		failpoint.Inject("ignoreDeleteRangeFailed", func() {
			err = nil
		})

		if err != nil {
			logutil.Logger(ctx).Warn("delete range failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			return
		}

		err = doGCPlacementRules(se, safePoint, r, &gcPlacementRuleCache)
		if err != nil {
			logutil.Logger(ctx).Warn("gc placement rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			return
		}
		// We only delete rules, so concurrently updating rules should not return errors.
		if err := w.doGCLabelRules(r); err != nil {
			logutil.Logger(ctx).Error("gc label rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			return
		}

		err = util.CompleteDeleteRange(se, r, !v2)
		if err != nil {
			logutil.Logger(ctx).Error("failed to mark delete range task done", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save").Inc()
		}
	}
	var wg util2.WaitGroupWrapper
	for i := range ranges {
		r := ranges[i]
		concurrencyLimiter <- struct{}{}
		wg.Run(func() { f(r) })
	}
	wg.Wait()

	logutil.Logger(ctx).Info("finish delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

const (
	// ConcurrencyDivisor reduces the input concurrency to avoid overwhelming the system
	ConcurrencyDivisor = 4
	// RequestsPerThread is the number of requests handled by a single thread
	RequestsPerThread = 100000
)

// calcDeleteRangeConcurrency calculates the concurrency of deleteRanges.
//
// There was only one concurrency for resolveLocks. When parallelizing deleteRanges, its concurrency is controlled by
// the same variable TiDBGCConcurrency. As requested by PM, the first priority is to ensure the stability of the system,
// so the concurrency of deleteRanges is reduced to avoid overwhelming the system.
//
// Assuming an average request takes 50ms:
// With ideal parallelism and sufficient concurrency,
// the maximum duration for a round of deleteRanges is 100,000 * 50ms = 5,000s.
// These values are conservatively chosen to minimize GC impact on foreground requests
func (w *GCWorker) calcDeleteRangeConcurrency(concurrency gcConcurrency, rangeNum int) int {
	maxConcurrency := max(1, concurrency.v/ConcurrencyDivisor)
	threadsBasedOnRequests := max(1, rangeNum/RequestsPerThread)
	if concurrency.isAuto {
		return min(maxConcurrency, threadsBasedOnRequests)
	}
	return maxConcurrency
}

// redoDeleteRanges checks all deleted ranges whose ts is at least `lifetime + 24h` ago. See TiKV RFC #2.
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) redoDeleteRanges(ctx context.Context, safePoint uint64,
	concurrency gcConcurrency) error {
	metrics.GCWorkerCounter.WithLabelValues("redo_delete_range").Inc()

	// We check delete range records that are deleted about 24 hours ago.
	redoDeleteRangesTs := safePoint - oracle.ComposeTS(int64(gcRedoDeleteRangeDelay.Seconds())*1000, 0)

	se := createSession(w.store)
	ranges, err := util.LoadDoneDeleteRanges(ctx, se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("start redo-delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()

	deleteRangeConcurrency := w.calcDeleteRangeConcurrency(concurrency, len(ranges))
	concurrencyLimiter := make(chan struct{}, deleteRangeConcurrency)

	f := func(r util.DelRangeTask) {
		defer func() {
			<-concurrencyLimiter
		}()
		var err error
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey)
		if err != nil {
			logutil.Logger(ctx).Warn("redo-delete range failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			return
		}
		se := createSession(w.store)
		err = util.DeleteDoneRecord(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("failed to remove delete_range_done record", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save_redo").Inc()
		}
	}
	var wg util2.WaitGroupWrapper
	for i := range ranges {
		r := ranges[i]
		concurrencyLimiter <- struct{}{}
		wg.Run(func() { f(r) })
	}
	wg.Wait()
	logutil.Logger(ctx).Info("finish redo-delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("redo_delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(
	ctx context.Context, startKey []byte, endKey []byte,
) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("delete ranges: got an error while trying to get store list from PD", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return errors.Trace(err)
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}, kvrpcpb.Context{DiskFullOpt: kvrpcpb.DiskFullOpt_AllowedOnAlmostFull})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.tikvStore.GetTiKVClient().SendRequest(ctx, address, req, unsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[gc worker] destroy range finished with errors: %v", errs)
	}

	return nil
}

// needsGCOperationForStore checks if the store-level requests related to GC needs to be sent to the store. The store-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range store.GetLabels() {
		if label.GetKey() == placement.EngineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case placement.EngineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case placement.EngineLabelTiFlashCompute:
		logutil.BgLogger().Debug("will ignore gc tiflash_compute node", zap.String("category", "gc worker"))
		return false, nil

	case placement.EngineLabelTiKV, "":
		// If no engine label is set, it should be a TiKV node.
		return true, nil

	default:
		return true, errors.Errorf("unsupported store engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			store.GetId(),
			store.GetAddress())
	}
}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func (w *GCWorker) getStoresForGC(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := w.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}
