package pkdbrepl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/pd/client/constants"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const pkdbResolveLocksConcurrency = 2
const pkdbResolveLocksSafePointDelay = 6 * time.Second

// TryResolveLocksIfNeeded is expected to be called when a single-node owner is
// elected. If PD has written the marker key when disabling standby and
// restarting TiDB, this function uses the TS stored in the marker value to start
// an asynchronous resolve-locks task and delete the marker key after it finishes
// successfully. TODO(lance6716): reuse GCWorker's resolveLocks.
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
	safePoint, err := resolveLocksSafePointFromMarkerValue(markerVal)
	if err != nil {
		logutil.BgLogger().Warn("marker key value is not a valid resolve-locks TS, skip",
			zap.String("marker", markerVal),
			zap.Error(err))
		return
	}
	if safePoint == 0 {
		logutil.BgLogger().Warn("marker key exists but resolve-locks safe point is zero, skip",
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
		for {
			err := resolveLocksForWholeCluster(ctx, tikvStore, safePoint, pkdbResolveLocksConcurrency)
			if err == nil {
				break
			}
			logutil.BgLogger().Warn("resolve locks failed",
				zap.Uint64("safePoint", safePoint),
				zap.String("marker", markerVal),
				zap.Error(err))
			if strings.HasPrefix(err.Error(), "cannot set read timestamp to a future time") {
				sleep(ctx.Done(), 2*time.Second)
				continue
			}
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

func resolveLocksSafePointFromMarkerValue(markerVal string) (uint64, error) {
	markerTS, err := strconv.ParseUint(markerVal, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if markerTS == 0 {
		return 0, nil
	}
	resolveTime := oracle.GetTimeFromTS(markerTS).Add(pkdbResolveLocksSafePointDelay)
	return oracle.GoTimeToTS(resolveTime), nil
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
	// TODO(lance6716): This touches every region in the cluster. On clusters with
	// huge region counts this can be slow and may increase memory usage (e.g. via
	// region cache growth) even though rangetask itself is streamed. Consider
	// making it more incremental/bounded or reusing GCWorker's resolveLocks
	// implementation which already has operational knobs.
	if err := runner.RunOnRange(ctx, []byte(""), []byte("")); err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("finish resolve locks",
		zap.Uint64("safePoint", safePoint),
		zap.Int("regions", runner.CompletedRegions()),
		zap.Duration("cost", time.Since(startTime)))
	return nil
}
