// Copyright 2019 PingCAP, Inc.
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

package gcutil

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	selectVariableValueSQL = `SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?`

	// GCScanLockLimit We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
	GCScanLockLimit = txnlock.ResolvedCacheSize / 2
)

// GCLockResolver is used for GCWorker and log backup advancer to resolve locks.
// #Note: Put it here to avoid cycle import
type GCLockResolver interface {
	LocateKey(*tikv.Backoffer, []byte) (*tikv.KeyLocation, error)

	ResolveLocks(*tikv.Backoffer, []*txnlock.Lock, tikv.RegionVerID) (bool, error)

	// ScanLocks only used for mock test.
	ScanLocks([]byte, uint64) []*txnlock.Lock

	SendReq(*tikv.Backoffer, *tikvrpc.Request, tikv.RegionVerID, time.Duration) (*tikvrpc.Response, error)

	// We need to get tikvStore to build rangerunner.
	// FIXME: the most code is in client.go and the store is only used to locate end keys of a region.
	// maybe we can move GCLockResolver into client.go.
	GetStore() tikv.Storage
}

// CheckGCEnable is use to check whether GC is enable.
func CheckGCEnable(ctx sessionctx.Context) (enable bool, err error) {
	val, err := ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBGCEnable)
	if err != nil {
		return false, errors.Trace(err)
	}
	return variable.TiDBOptOn(val), nil
}

// DisableGC will disable GC enable variable.
func DisableGC(ctx sessionctx.Context) error {
	return ctx.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBGCEnable, variable.Off)
}

// EnableGC will enable GC enable variable.
func EnableGC(ctx sessionctx.Context) error {
	return ctx.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBGCEnable, variable.On)
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx sessionctx.Context, snapshotTS uint64) error {
	safePointTS, err := GetGCSafePoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// ValidateSnapshotWithGCSafePoint checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshotWithGCSafePoint(snapshotTS, safePointTS uint64) error {
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// GetGCSafePoint loads GC safe point time from mysql.tidb.
func GetGCSafePoint(sctx sessionctx.Context) (uint64, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, selectVariableValueSQL, "tikv_gc_safe_point")
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.New("can not get 'tikv_gc_safe_point'")
	}
	safePointString := rows[0].GetString(0)
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := oracle.GoTimeToTS(safePointTime)
	return ts, nil
}

// ResolveLocksForRange is used for GCWorker and log backup advancer.
func ResolveLocksForRange(
	ctx context.Context,
	uuid string,
	lockResolver GCLockResolver,
	maxVersion uint64,
	startKey []byte,
	endKey []byte,
) (rangetask.TaskStat, error) {
	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: maxVersion,
		Limit:      GCScanLockLimit,
	}, kvrpcpb.Context{
		RequestSource: util.RequestSourceFromCtx(ctx),
	})

	failpoint.Inject("lowScanLockLimit", func() {
		req.ScanLock().Limit = 3
	})

	var stat rangetask.TaskStat
	key := startKey
	bo := tikv.NewGcResolveLockMaxBackoffer(ctx)
	failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
		sleep := v.(int)
		// cooperate with github.com/tikv/client-go/v2/locate/invalidCacheAndRetry
		//nolint: SA1029
		ctx = context.WithValue(ctx, "injectedBackoff", struct{}{}) //nolint
		bo = tikv.NewBackofferWithVars(ctx, sleep, nil)
	})
retryScanAndResolve:
	for {
		select {
		case <-ctx.Done():
			return stat, errors.New("[gc worker] gc job canceled")
		default:
		}

		req.ScanLock().StartKey = key
		loc, err := lockResolver.LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}
		req.ScanLock().EndKey = loc.EndKey
		resp, err := lockResolver.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return stat, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return stat, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.Trace(tikverr.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return stat, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*txnlock.Lock, 0, len(locksInfo))
		for _, li := range locksInfo {
			locks = append(locks, txnlock.NewLock(li))
		}
		locks = append(locks, lockResolver.ScanLocks(key, loc.Region.GetID())...)
		locForResolve := loc
		for {
			ok, err1 := lockResolver.ResolveLocks(bo, locks, locForResolve.Region)
			if err1 != nil {
				return stat, errors.Trace(err1)
			}
			if !ok {
				err = bo.Backoff(tikv.BoTxnLock(), errors.Errorf("remain locks: %d", len(locks)))
				if err != nil {
					return stat, errors.Trace(err)
				}
				stillInSame, refreshedLoc, err := tryRelocateLocksRegion(bo, lockResolver, locks)
				if err != nil {
					return stat, errors.Trace(err)
				}
				if stillInSame {
					locForResolve = refreshedLoc
					continue
				}
				continue retryScanAndResolve
			}
			break
		}
		if len(locks) < GCScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
		} else {
			logutil.Logger(ctx).Info("region has more than limit locks", zap.String("category", "gc worker"),
				zap.String("uuid", uuid),
				zap.Uint64("region", locForResolve.Region.GetID()),
				zap.Int("scan lock limit", GCScanLockLimit))
			metrics.GCRegionTooManyLocksCounter.Inc()
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = tikv.NewGcResolveLockMaxBackoffer(ctx)
		failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
			sleep := v.(int)
			bo = tikv.NewBackofferWithVars(ctx, sleep, nil)
		})
	}
	return stat, nil
}

func tryRelocateLocksRegion(bo *tikv.Backoffer, lockResolver GCLockResolver, locks []*txnlock.Lock) (stillInSameRegion bool, refreshedLoc *tikv.KeyLocation, err error) {
	if len(locks) == 0 {
		return
	}
	refreshedLoc, err = lockResolver.LocateKey(bo, locks[0].Key)
	if err != nil {
		return
	}
	stillInSameRegion = refreshedLoc.Contains(locks[len(locks)-1].Key)
	return
}
