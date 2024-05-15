// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util"
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

	var lastSafePoint uint64
	var err error

	log.Debug("update PD safePoint limit with TTL", zap.Object("safePoint", sp))

	lastSafePoint, err = UpdateServiceSafePointWithGCManagementType(ctx, pdClient, sp.ID, sp.TTL, sp.BackupTS-1)
	if lastSafePoint > sp.BackupTS-1 {
		log.Warn("service GC safe point lost, we may fail to back up if GC lifetime isn't long enough",
			zap.Uint64("lastSafePoint", lastSafePoint),
			zap.Object("safePoint", sp),
		)
	}
	return errors.Trace(err)
}

func UpdateServiceSafePointWithGCManagementType(ctx context.Context, pdClient pd.Client, serviceID string, ttl int64, serviceSafePoint uint64) (uint64, error) {
	var lastServiceSafePoint uint64
	var err error
	var keyspaceID uint32
	var gcManagementType string
	keyspaceName := config.GetGlobalConfig().KeyspaceName

	if keyspaceName == "" {
		// If keyspace is not set.
		lastServiceSafePoint, err = pdClient.UpdateServiceGCSafePoint(ctx, serviceID, ttl, serviceSafePoint)
		log.Info("use service safe point.",
			zap.String("service-safe-point-id", serviceID),
			zap.Int64("service-safe-point-ttl", ttl),
			zap.Uint64("service-safe-point", serviceSafePoint),
			zap.Uint64("last-service-safe-point", lastServiceSafePoint))
	} else {
		keyspaceID, gcManagementType, err = getKeyspaceMeta(ctx, pdClient, keyspaceName)
		if err != nil {
			return 0, errors.Trace(err)
		}

		switch gcManagementType {
		case "keyspace_level_gc":
			{
				lastServiceSafePoint, err = pdClient.UpdateServiceSafePointV2(ctx, keyspaceID, serviceID, ttl, serviceSafePoint)
				log.Info("update keyspace level service safe point.",
					zap.String("keyspace-name", keyspaceName),
					zap.Uint32("keyspace-id", keyspaceID),
					zap.String("service-safe-point-id", serviceID),
					zap.Int64("service-safe-point-ttl", ttl),
					zap.Uint64("service-safe-point", serviceSafePoint),
					zap.Uint64("last-service-safe-point", lastServiceSafePoint))
			}
		case "global_gc":
			{
				lastServiceSafePoint, err = pdClient.UpdateServiceGCSafePoint(ctx, serviceID, ttl, serviceSafePoint)
				log.Info("use service safe point.",
					zap.String("keyspace-name", keyspaceName),
					zap.Uint32("keyspace-id", keyspaceID),
					zap.String("service-safe-point-id", serviceID),
					zap.Int64("service-safe-point-ttl", ttl),
					zap.Uint64("service-safe-point", serviceSafePoint),
					zap.Uint64("last-service-safe-point", lastServiceSafePoint))
			}
		case "":
			{
				// If `gc_management_type` is not set.
				lastServiceSafePoint, err = pdClient.UpdateServiceGCSafePoint(ctx, serviceID, ttl, serviceSafePoint)
				log.Info("use service safe point.",
					zap.String("keyspace-name", keyspaceName),
					zap.Uint32("keyspace-id", keyspaceID),
					zap.String("service-safe-point-id", serviceID),
					zap.Int64("service-safe-point-ttl", ttl),
					zap.Uint64("service-safe-point", serviceSafePoint),
					zap.Uint64("last-service-safe-point", lastServiceSafePoint))
			}
		default:
			{
				return 0, errors.New("keyspace has unknown GC management type")
			}
		}
	}
	return lastServiceSafePoint, err
}

func getKeyspaceMeta(ctx context.Context, pdClient pd.Client, keyspaceName string) (uint32, string, error) {
	// Load Keyspace meta with retry.
	var keyspaceMeta *keyspacepb.KeyspaceMeta
	err := util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (bool, error) {
		var errInner error
		keyspaceMeta, errInner = pdClient.LoadKeyspace(ctx, keyspaceName)
		// Retry when pd not bootstrapped or if keyspace not exists.
		if IsNotBootstrappedError(errInner) || IsKeyspaceNotExistError(errInner) {
			return true, errInner
		}
		// Do not retry when success or encountered unexpected error.
		return false, errInner
	})
	if err != nil {
		return 0, "", err
	}

	return keyspaceMeta.Id, keyspaceMeta.Config["gc_management_type"], nil

}

// IsNotBootstrappedError returns true if the error is pd not bootstrapped error.
func IsNotBootstrappedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
}

// IsKeyspaceNotExistError returns true the error is caused by keyspace not exists.
func IsKeyspaceNotExistError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
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
