// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/oracle"
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

// MakeSafePointID makes a unique safe point ID, for reduce name conflict.
func MakeSafePointID() string {
	return fmt.Sprintf(brServiceSafePointIDFormat, uuid.New())
}

// Manager abstracts GC operations, supporting both global and keyspace-level GC.
type Manager interface {
	// GetGCSafePoint returns the current GC safe point.
	GetGCSafePoint(ctx context.Context) (uint64, error)

	// SetServiceSafePoint sets the service safe point with TTL.
	// If TTL <= 0, it removes the service safe point.
	SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error

	// DeleteServiceSafePoint removes the service safe point.
	DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error
}
