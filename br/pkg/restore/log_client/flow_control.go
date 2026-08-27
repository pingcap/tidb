// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"context"
	"math"
	"strconv"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	tikvSoftPendingCompactionBytesLimit = "storage.flow-control.soft-pending-compaction-bytes-limit"
	tikvHardPendingCompactionBytesLimit = "storage.flow-control.hard-pending-compaction-bytes-limit"

	compactedSSTFlowControlSoftLimitFloor   uint64 = units.TiB
	compactedSSTFlowControlHardLimitFloor   uint64 = 2 * units.TiB
	compactedSSTFlowControlPendingThreshold uint64 = 100 * units.GiB
	compactedSSTMaxBytesForLevelMultiplier  uint64 = 10
)

type tikvConfigValue struct {
	instance string
	value    string
}

type compactedSSTFlowControlConfig struct {
	soft []tikvConfigValue
	hard []tikvConfigValue
}

type compactedSSTFlowControlEstimate struct {
	snapshotRestoreBytes uint64
	compactedSSTBytes    uint64
	l6BytesPerStore      uint64
	l5BytesPerStore      uint64
	pendingBytes         uint64
	storeCount           uint
	replicaCount         uint
}

func (rc *LogClient) adjustTiKVFlowControlForCompactedSSTRestore(
	ctx context.Context,
	backupFileSets restore.BatchBackupFileSet,
	snapshotRestoreBytes uint64,
	checkpointCompactedSSTBytes uint64,
) error {
	if rc.unsafeSession == nil {
		log.Warn("[Compacted SST Restore] skip adjusting TiKV flow-control configs because session is not initialized")
		return nil
	}
	if rc.sstRestoreManager == nil || rc.sstRestoreManager.storeCount == 0 {
		log.Warn("[Compacted SST Restore] skip adjusting TiKV flow-control configs because TiKV store count is not initialized")
		return nil
	}
	if rc.sstRestoreManager.replicaCount == 0 {
		log.Warn("[Compacted SST Restore] skip adjusting TiKV flow-control configs because TiKV replica count is not initialized")
		return nil
	}

	execCtx := rc.unsafeSession.GetSessionCtx().GetRestrictedSQLExecutor()
	originConfig, err := getCompactedSSTFlowControlConfig(ctx, execCtx)
	if err != nil {
		return errors.Trace(err)
	}
	if originConfig == nil {
		log.Warn("[Compacted SST Restore] skip adjusting TiKV flow-control configs because config items are unavailable")
		return nil
	}

	estimate := estimateCompactedSSTFlowControl(
		backupFileSets,
		snapshotRestoreBytes,
		checkpointCompactedSSTBytes,
		rc.sstRestoreManager.storeCount,
		rc.sstRestoreManager.replicaCount,
	)

	if estimate.pendingBytes <= compactedSSTFlowControlPendingThreshold {
		log.Info("[Compacted SST Restore] skip adjusting TiKV flow-control configs because estimated pending compaction bytes is small",
			zap.String("pending-compaction-bytes", formatBytes(estimate.pendingBytes)),
			zap.String("threshold", formatBytes(compactedSSTFlowControlPendingThreshold)))
		return nil
	}
	targetSoft, targetHard := compactedSSTFlowControlTarget(originConfig, estimate.pendingBytes)
	originSoftBytes := maxTiKVConfigBytes(originConfig.soft)
	originHardBytes := maxTiKVConfigBytes(originConfig.hard)
	logCompactedSSTFlowControlEstimate(estimate, targetSoft, targetHard)

	if allTiKVConfigsAtLeast(originConfig.soft, targetSoft) &&
		allTiKVConfigsAtLeast(originConfig.hard, targetHard) {
		log.Info("[Compacted SST Restore] TiKV flow-control configs are already large enough",
			zap.String("soft-current", formatBytes(originSoftBytes)),
			zap.String("hard-current", formatBytes(originHardBytes)))
		return nil
	}

	if err := setTiKVConfig(ctx, execCtx, tikvHardPendingCompactionBytesLimit, formatBytes(targetHard)); err != nil {
		return errors.Trace(err)
	}
	if err := setTiKVConfig(ctx, execCtx, tikvSoftPendingCompactionBytesLimit, formatBytes(targetSoft)); err != nil {
		return errors.Trace(err)
	}

	log.Warn("[Compacted SST Restore] adjusted TiKV flow-control configs",
		zap.String("soft", formatBytes(targetSoft)),
		zap.String("hard", formatBytes(targetHard)))
	return nil
}

func estimateCompactedSSTFlowControl(
	backupFileSets restore.BatchBackupFileSet,
	snapshotRestoreBytes uint64,
	checkpointCompactedSSTBytes uint64,
	storeCount uint,
	replicaCount uint,
) compactedSSTFlowControlEstimate {
	compactedSSTBytes := checkpointCompactedSSTBytes
	for _, set := range backupFileSets {
		for _, file := range set.SSTFiles {
			compactedSSTBytes = saturatingAddUint64(compactedSSTBytes, compactedSSTSizeForFlowControl(file))
		}
	}
	l6BytesPerStore := estimateLevelBytesPerStore(snapshotRestoreBytes, storeCount, replicaCount)
	l5BytesPerStore := estimateLevelBytesPerStore(compactedSSTBytes, storeCount, replicaCount)
	return compactedSSTFlowControlEstimate{
		snapshotRestoreBytes: snapshotRestoreBytes,
		compactedSSTBytes:    compactedSSTBytes,
		l6BytesPerStore:      l6BytesPerStore,
		l5BytesPerStore:      l5BytesPerStore,
		pendingBytes:         estimatePendingCompactionBytes(l6BytesPerStore, l5BytesPerStore),
		storeCount:           storeCount,
		replicaCount:         replicaCount,
	}
}

func compactedSSTSizeForFlowControl(file *backuppb.File) uint64 {
	if file.GetSize_() > 0 {
		return file.GetSize_()
	}
	return file.GetTotalBytes()
}

func estimateLevelBytesPerStore(totalBytes uint64, storeCount uint, replicaCount uint) uint64 {
	if storeCount == 0 || replicaCount == 0 || totalBytes == 0 {
		return 0
	}
	effectiveReplicaCount := replicaCount
	if effectiveReplicaCount > storeCount {
		effectiveReplicaCount = storeCount
	}
	return saturatingMulUint64(
		ceilDivUint64(totalBytes, uint64(storeCount)),
		uint64(effectiveReplicaCount),
	)
}

func estimatePendingCompactionBytes(l6BytesPerStore, l5BytesPerStore uint64) uint64 {
	if l5BytesPerStore == 0 {
		return 0
	}
	l6Bytes := float64(l6BytesPerStore)
	l5Bytes := float64(l5BytesPerStore)
	ratio := l6Bytes / l5Bytes
	if ratio > float64(compactedSSTMaxBytesForLevelMultiplier) {
		return 0
	}
	l5TargetBytes := l6Bytes / float64(compactedSSTMaxBytesForLevelMultiplier)
	if l5Bytes <= l5TargetBytes {
		return 0
	}
	pendingBytes := (l5Bytes - l5TargetBytes) * (ratio + 1)
	if pendingBytes <= 0 {
		return 0
	}
	if pendingBytes >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(pendingBytes)
}

func compactedSSTFlowControlTarget(
	originConfig *compactedSSTFlowControlConfig,
	pendingBytes uint64,
) (uint64, uint64) {
	adjustedPendingBytes := saturatingAddUint64(
		pendingBytes,
		ceilDivUint64(pendingBytes, 4),
	)
	soft := max(
		compactedSSTFlowControlSoftLimitFloor,
		adjustedPendingBytes,
		maxTiKVConfigBytes(originConfig.soft),
	)
	hard := saturatingMulUint64(soft, 2)
	hard = max(compactedSSTFlowControlHardLimitFloor, hard, maxTiKVConfigBytes(originConfig.hard))
	return soft, hard
}

func getCompactedSSTFlowControlConfig(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
) (*compactedSSTFlowControlConfig, error) {
	soft, err := getTiKVConfigValues(ctx, execCtx, tikvSoftPendingCompactionBytesLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hard, err := getTiKVConfigValues(ctx, execCtx, tikvHardPendingCompactionBytesLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(soft) == 0 || len(hard) == 0 {
		return nil, nil
	}
	return &compactedSSTFlowControlConfig{
		soft: soft,
		hard: hard,
	}, nil
}

func getTiKVConfigValues(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	name string,
) ([]tikvConfigValue, error) {
	rows, fields, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		"show config where name = %? and type = 'tikv'",
		name,
	)
	if errSQL != nil {
		return nil, errSQL
	}

	configs := make([]tikvConfigValue, 0, len(rows))
	for _, row := range rows {
		d := row.GetDatum(1, &fields[1].Column.FieldType)
		instance, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		d = row.GetDatum(3, &fields[3].Column.FieldType)
		value, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		configs = append(configs, tikvConfigValue{
			instance: instance,
			value:    value,
		})
	}
	return configs, nil
}

func setTiKVConfig(
	ctx context.Context,
	execCtx sqlexec.RestrictedSQLExecutor,
	name string,
	value string,
) error {
	_, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		"set config tikv `"+name+"`=%?",
		value,
	)
	if err != nil {
		return errors.Annotatef(err, "failed to set config `%s`=%s", name, value)
	}
	return nil
}

func maxTiKVConfigBytes(configs []tikvConfigValue) uint64 {
	maxBytes := uint64(0)
	for _, config := range configs {
		value, err := parseByteSizeConfig(config.value)
		if err != nil {
			log.Warn("[Compacted SST Restore] failed to parse TiKV flow-control config value",
				zap.String("instance", config.instance),
				zap.String("value", config.value),
				zap.Error(err))
			continue
		}
		maxBytes = max(maxBytes, value)
	}
	return maxBytes
}

func allTiKVConfigsAtLeast(configs []tikvConfigValue, target uint64) bool {
	if len(configs) == 0 {
		return false
	}
	for _, config := range configs {
		value, err := parseByteSizeConfig(config.value)
		if err != nil {
			log.Warn("[Compacted SST Restore] failed to parse TiKV flow-control config value",
				zap.String("instance", config.instance),
				zap.String("value", config.value),
				zap.Error(err))
			return false
		}
		if value < target {
			return false
		}
	}
	return true
}

func parseByteSizeConfig(s string) (uint64, error) {
	v, err := units.RAMInBytes(s)
	if err != nil {
		v, err = units.FromHumanSize(s)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	if v < 0 {
		return 0, errors.Errorf("invalid negative byte size: %s", s)
	}
	return uint64(v), nil
}

func formatBytes(bytes uint64) string {
	for _, unit := range []struct {
		bytes  uint64
		suffix string
	}{
		{uint64(units.TiB), "TiB"},
		{uint64(units.GiB), "GiB"},
		{uint64(units.MiB), "MiB"},
		{uint64(units.KiB), "KiB"},
	} {
		if bytes >= unit.bytes && bytes%unit.bytes == 0 {
			return strconv.FormatUint(bytes/unit.bytes, 10) + unit.suffix
		}
	}
	return strconv.FormatUint(bytes, 10) + "B"
}

func saturatingAddUint64(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func saturatingMulUint64(a uint64, b uint64) uint64 {
	if b != 0 && a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
}

func ceilDivUint64(a, b uint64) uint64 {
	if b == 0 {
		return 0
	}
	if a == 0 {
		return 0
	}
	return 1 + (a-1)/b
}

func logCompactedSSTFlowControlEstimate(
	estimate compactedSSTFlowControlEstimate,
	targetSoft uint64,
	targetHard uint64,
) {
	log.Info("[Compacted SST Restore] estimated added bytes by TiKV store",
		zap.Uint64("snapshot-restore-bytes", estimate.snapshotRestoreBytes),
		zap.String("snapshot-restore-size", formatBytes(estimate.snapshotRestoreBytes)),
		zap.Uint64("compacted-sst-bytes", estimate.compactedSSTBytes),
		zap.String("compacted-sst-size", formatBytes(estimate.compactedSSTBytes)),
		zap.Uint("store-count", estimate.storeCount),
		zap.Uint("replica-count", estimate.replicaCount),
		zap.Uint64("l6-bytes-per-store", estimate.l6BytesPerStore),
		zap.String("l6-size-per-store", formatBytes(estimate.l6BytesPerStore)),
		zap.Uint64("l5-bytes-per-store", estimate.l5BytesPerStore),
		zap.String("l5-size-per-store", formatBytes(estimate.l5BytesPerStore)),
		zap.Uint64("estimated-pending-compaction-bytes", estimate.pendingBytes),
		zap.String("estimated-pending-compaction-size", formatBytes(estimate.pendingBytes)))
	log.Info("[Compacted SST Restore] target TiKV flow-control configs",
		zap.String("soft-pending-compaction-bytes-limit", formatBytes(targetSoft)),
		zap.String("hard-pending-compaction-bytes-limit", formatBytes(targetHard)))
}
