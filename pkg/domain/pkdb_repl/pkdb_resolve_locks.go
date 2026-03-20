package pkdbrepl

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// logReplResolveLocksSafePoint stores the TS obtained during TiDB startup, used
// as the safePoint parameter for resolve-locks after PD disables standby mode.
var logReplResolveLocksSafePoint atomic.Uint64

// SetLogReplResolveLocksSafePoint sets the safe point TS for resolve-locks operation.
func SetLogReplResolveLocksSafePoint(ts uint64) {
	logReplResolveLocksSafePoint.Store(ts)
}

func getLogReplResolveLocksSafePoint() uint64 {
	return logReplResolveLocksSafePoint.Load()
}

const pkdbResolveLocksConcurrency = 2

// TryResolveLocksIfNeeded is expected to be called when a single-node owner is
// elected. If PD has written the marker key before disabling standby mode, this
// function will start an asynchronous resolve-locks task and delete the marker
// key after it finishes successfully.
// TODO(lance6716): reuse GCWorker's resolveLocks.
func TryResolveLocksIfNeeded(ctx context.Context, store kv.Storage, etcdCli *clientv3.Client) {
	if etcdCli == nil {
		return
	}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := etcdCli.Get(checkCtx, constants.PkdbResolveLocksKey)
	if err != nil {
		logutil.BgLogger().Warn("failed to check marker key from etcd", zap.Error(err))
		return
	}
	if len(resp.Kvs) == 0 {
		return
	}

	markerVal := string(resp.Kvs[0].Value)
	safePoint := getLogReplResolveLocksSafePoint()
	if safePoint == 0 {
		logutil.BgLogger().Warn("marker key exists but startup TS is not set, skip",
			zap.String("marker", markerVal))
		return
	}

	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		logutil.BgLogger().Warn("marker key exists but store is not tikv storage, skip",
			zap.String("store", fmt.Sprintf("%T", store)),
			zap.String("marker", markerVal))
		return
	}

	logutil.BgLogger().Info("marker key found, start resolving locks in background",
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", pkdbResolveLocksConcurrency),
		zap.String("marker", markerVal))

	go func() {
		startTime := time.Now()
		err := resolveLocksForWholeCluster(ctx, tikvStore, safePoint, pkdbResolveLocksConcurrency)
		if err != nil {
			logutil.BgLogger().Warn("resolve locks failed",
				zap.Uint64("safePoint", safePoint),
				zap.String("marker", markerVal),
				zap.Error(err))
			return
		}

		// Use a fresh context for the delete; owner ctx might be canceled due to
		// owner transfer, but we still want to clear the marker after success.
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		txnResp, err := etcdCli.Txn(delCtx).
			If(clientv3.Compare(clientv3.Value(constants.PkdbResolveLocksKey), "=", markerVal)).
			Then(clientv3.OpDelete(constants.PkdbResolveLocksKey)).
			Commit()
		if err != nil {
			logutil.BgLogger().Warn("resolve locks succeeded but failed to delete marker key",
				zap.Uint64("safePoint", safePoint),
				zap.String("marker", markerVal),
				zap.Error(err))
			return
		}

		if txnResp.Succeeded {
			logutil.BgLogger().Info("resolved locks and deleted marker key",
				zap.Uint64("safePoint", safePoint),
				zap.String("marker", markerVal),
				zap.Duration("cost", time.Since(startTime)))
		} else {
			logutil.BgLogger().Info("resolved locks but marker key value changed, skip delete",
				zap.Uint64("safePoint", safePoint),
				zap.String("marker", markerVal),
				zap.Duration("cost", time.Since(startTime)))
		}
	}()
}

func resolveLocksForWholeCluster(
	ctx context.Context,
	store tikv.Storage,
	safePoint uint64,
	concurrency int,
) error {
	logutil.Logger(ctx).Info("start resolve locks",
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	resolverID := fmt.Sprintf("pkdb-resolve-locks-%d", time.Now().UnixNano())
	regionLockResolver := tikv.NewRegionLockResolver(resolverID, store)

	handler := func(ctx context.Context, r tikvkv.KeyRange) (rangetask.TaskStat, error) {
		scanLimit := uint32(tikv.GCScanLockLimit)
		return tikv.ResolveLocksForRange(
			ctx,
			regionLockResolver,
			safePoint,
			r.StartKey,
			r.EndKey,
			tikv.NewGcResolveLockMaxBackoffer,
			scanLimit,
		)
	}

	runner := rangetask.NewRangeTaskRunner("pkdb-resolve-locks-runner", store, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	if err := runner.RunOnRange(ctx, []byte(""), []byte("")); err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("finish resolve locks",
		zap.Uint64("safePoint", safePoint),
		zap.Int("regions", runner.CompletedRegions()),
		zap.Duration("cost", time.Since(startTime)))
	return nil
}
