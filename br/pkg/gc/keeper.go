// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/zap"
)

// CheckGCSafePoint checks whether the ts is older than GC safepoint using Manager.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafePoint(ctx context.Context, mgr Manager, ts uint64) error {
	safePoint, err := mgr.GetGCSafePoint(ctx)
	if err != nil {
		log.Warn("fail to get GC safe point", zap.Error(err))
		return nil
	}
	if ts <= safePoint {
		return errors.Annotatef(berrors.ErrBackupGCSafepointExceeded, "GC safepoint %d exceed TS %d", safePoint, ts)
	}
	return nil
}

// StartKeeperWithManager starts a goroutine to periodically update the service safe point.
// It uses the provided Manager to set the safe point.
// The keeper will run until the context is canceled.
func StartKeeperWithManager(
	ctx context.Context,
	sp BRServiceSafePoint,
	mgr Manager,
) error {
	if sp.ID == "" || sp.TTL <= 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid service safe point %v", sp)
	}
	if err := CheckGCSafePoint(ctx, mgr, sp.BackupTS); err != nil {
		return errors.Trace(err)
	}
	// Set service safe point immediately to cover the gap between starting
	// update goroutine and updating service safe point.
	if err := mgr.SetServiceSafePoint(ctx, sp); err != nil {
		return errors.Trace(err)
	}

	// It would be OK since TTL won't be zero, so gapTime should > `0.
	updateGapTime := time.Duration(sp.TTL) * time.Second / preUpdateServiceSafePointFactor
	updateTick := time.NewTicker(updateGapTime)
	checkTick := time.NewTicker(checkGCSafePointGapTime)
	go func() {
		defer updateTick.Stop()
		defer checkTick.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Debug("service safe point keeper exited")
				return
			case <-updateTick.C:
				if err := mgr.SetServiceSafePoint(ctx, sp); err != nil {
					log.Warn("failed to update service safe point, backup may fail if gc triggered",
						zap.Error(err),
					)
				}
			case <-checkTick.C:
				if err := CheckGCSafePoint(ctx, mgr, sp.BackupTS); err != nil {
					log.Panic("cannot pass gc safe point check, aborting",
						zap.Error(err),
						zap.Object("safePoint", sp),
					)
				}
			}
		}
	}()
	return nil
}
