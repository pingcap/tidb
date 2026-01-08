// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	brServiceSafePointIDFormat      = "br-%s"
	preUpdateServiceSafePointFactor = 3
	checkGCSafePointGapTime         = 5 * time.Second
	// DefaultBRGCSafePointTTL means PD keep safePoint limit at least 5min.
	DefaultBRGCSafePointTTL = 5 * 60
	// DefaultCheckpointGCSafePointTTL means PD keep safePoint limit at least 72 minutes.
	DefaultCheckpointGCSafePointTTL = 72 * 60
	// DefaultStreamStartSafePointTTL specifies keeping the server safepoint 30 mins when start task.
	DefaultStreamStartSafePointTTL = 1800
	// DefaultStreamPauseSafePointTTL specifies Keeping the server safePoint at list 24h when pause task.
	DefaultStreamPauseSafePointTTL = 24 * 3600
)

// BRServiceSafePoint is metadata of service safe point from a BR 'instance'.
type BRServiceSafePoint struct {
	ID       string
	TTL      int64
	BackupTS uint64
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (sp BRServiceSafePoint) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("ID", sp.ID)
	ttlDuration := time.Duration(sp.TTL) * time.Second
	encoder.AddString("TTL", ttlDuration.String())
	backupTime := oracle.GetTimeFromTS(sp.BackupTS)
	encoder.AddString("BackupTime", backupTime.String())
	encoder.AddUint64("BackupTS", sp.BackupTS)
	return nil
}

// getGCSafePoint returns the current gc safe point.
// TODO: Some cluster may not enable distributed GC.
func getGCSafePoint(ctx context.Context, pdClient pd.Client) (uint64, error) {
	safePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

// MakeSafePointID makes a unique safe point ID, for reduce name conflict.
func MakeSafePointID() string {
	return fmt.Sprintf(brServiceSafePointIDFormat, uuid.New())
}

// checkGCSafePointByManager checks whether the ts is older than GC safepoint using GCSafePointManager.
func checkGCSafePointByManager(ctx context.Context, mgr GCSafePointManager, ts uint64) error {
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

// StartServiceSafePointKeeperInner starts a goroutine to periodically update the service safe point.
// It uses the provided GCSafePointManager to set the safe point.
// The keeper will run until the context is canceled.
func StartServiceSafePointKeeperInner(
	ctx context.Context,
	sp BRServiceSafePoint,
	mgr GCSafePointManager,
) error {
	if sp.ID == "" || sp.TTL <= 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid service safe point %v", sp)
	}
	if err := checkGCSafePointByManager(ctx, mgr, sp.BackupTS); err != nil {
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
				if err := checkGCSafePointByManager(ctx, mgr, sp.BackupTS); err != nil {
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
