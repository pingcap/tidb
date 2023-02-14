// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
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

// CheckGCSafePoint checks whether the ts is older than GC safepoint.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafePoint(ctx context.Context, pdClient pd.Client, ts uint64) error {
	// TODO: use PDClient.GetGCSafePoint instead once PD client exports it.
	safePoint, err := getGCSafePoint(ctx, pdClient)
	if err != nil {
		log.Warn("fail to get GC safe point", zap.Error(err))
		return nil
	}
	if ts <= safePoint {
		return errors.Annotatef(berrors.ErrBackupGCSafepointExceeded, "GC safepoint %d exceed TS %d", safePoint, ts)
	}
	return nil
}

// UpdateServiceSafePoint register BackupTS to PD, to lock down BackupTS as safePoint with TTL seconds.
func UpdateServiceSafePoint(ctx context.Context, pdClient pd.Client, sp BRServiceSafePoint) error {
	log.Debug("update PD safePoint limit with TTL", zap.Object("safePoint", sp))

	lastSafePoint, err := pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, sp.TTL, sp.BackupTS-1)
	if lastSafePoint > sp.BackupTS-1 {
		log.Warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough",
			zap.Uint64("lastSafePoint", lastSafePoint),
			zap.Object("safePoint", sp),
		)
	}
	return errors.Trace(err)
}

// StartServiceSafePointKeeper will run UpdateServiceSafePoint periodicity
// hence keeping service safepoint won't lose.
func StartServiceSafePointKeeper(
	ctx context.Context,
	pdClient pd.Client,
	sp BRServiceSafePoint,
) error {
	if sp.ID == "" || sp.TTL <= 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid service safe point %v", sp)
	}
	if err := CheckGCSafePoint(ctx, pdClient, sp.BackupTS); err != nil {
		return errors.Trace(err)
	}
	// Update service safe point immediately to cover the gap between starting
	// update goroutine and updating service safe point.
	if err := UpdateServiceSafePoint(ctx, pdClient, sp); err != nil {
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
				if err := UpdateServiceSafePoint(ctx, pdClient, sp); err != nil {
					log.Warn("failed to update service safe point, backup may fail if gc triggered",
						zap.Error(err),
					)
				}
			case <-checkTick.C:
				if err := CheckGCSafePoint(ctx, pdClient, sp.BackupTS); err != nil {
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

type FakePDClient struct {
	pd.Client
	Stores []*metapb.Store
}

// GetAllStores return fake stores.
func (c FakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, c.Stores...), nil
}
