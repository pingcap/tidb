// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"strconv"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	TidbNewCollationEnabled = "new_collation_enabled"
)

var (
	logBackupTaskCount = atomic.NewInt32(0)
)

func GetRegionSplitInfo(ctx sqlexec.RestrictedSQLExecutor) (uint64, int64) {
	return GetSplitSize(ctx), GetSplitKeys(ctx)
}

func GetSplitSize(ctx sqlexec.RestrictedSQLExecutor) uint64 {
	const defaultSplitSize = 96 * 1024 * 1024
	varStr := "show config where name = 'coprocessor.region-split-size' and type = 'tikv'"
	rows, fields, err := ctx.ExecRestrictedSQL(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR),
		nil,
		varStr,
	)
	if err != nil {
		log.Warn("failed to get split size, use default value", logutil.ShortError(err))
		return defaultSplitSize
	}
	if len(rows) == 0 {
		// use the default value
		return defaultSplitSize
	}

	d := rows[0].GetDatum(3, &fields[3].Column.FieldType)
	splitSizeStr, err := d.ToString()
	if err != nil {
		log.Warn("failed to get split size, use default value", logutil.ShortError(err))
		return defaultSplitSize
	}
	splitSize, err := units.FromHumanSize(splitSizeStr)
	if err != nil {
		log.Warn("failed to get split size, use default value", logutil.ShortError(err))
		return defaultSplitSize
	}
	return uint64(splitSize)
}

func GetSplitKeys(ctx sqlexec.RestrictedSQLExecutor) int64 {
	const defaultSplitKeys = 960000
	varStr := "show config where name = 'coprocessor.region-split-keys' and type = 'tikv'"
	rows, fields, err := ctx.ExecRestrictedSQL(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR),
		nil,
		varStr,
	)
	if err != nil {
		log.Warn("failed to get split keys, use default value", logutil.ShortError(err))
		return defaultSplitKeys
	}
	if len(rows) == 0 {
		// use the default value
		return defaultSplitKeys
	}

	d := rows[0].GetDatum(3, &fields[3].Column.FieldType)
	splitKeysStr, err := d.ToString()
	if err != nil {
		log.Warn("failed to get split keys, use default value", logutil.ShortError(err))
		return defaultSplitKeys
	}
	splitKeys, err := strconv.ParseInt(splitKeysStr, 10, 64)
	if err != nil {
		log.Warn("failed to get split keys, use default value", logutil.ShortError(err))
		return defaultSplitKeys
	}
	return splitKeys
}

func GetGcRatio(ctx sqlexec.RestrictedSQLExecutor) (string, error) {
	valStr := "show config where name = 'gc.ratio-threshold' and type = 'tikv'"
	rows, fields, errSQL := ctx.ExecRestrictedSQL(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR),
		nil,
		valStr,
	)
	if errSQL != nil {
		return "", errSQL
	}
	if len(rows) == 0 {
		// no rows mean not support log backup.
		return "", nil
	}

	d := rows[0].GetDatum(3, &fields[3].Column.FieldType)
	return d.ToString()
}

const DefaultGcRatioVal = "1.1"

func SetGcRatio(ctx sqlexec.RestrictedSQLExecutor, ratio string) error {
	_, _, err := ctx.ExecRestrictedSQL(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR),
		nil,
		"set config tikv `gc.ratio-threshold`=%?",
		ratio,
	)
	if err != nil {
		return errors.Annotatef(err, "failed to set config `gc.ratio-threshold`=%s", ratio)
	}
	log.Warn("set config tikv gc.ratio-threshold", zap.String("ratio", ratio))
	return nil
}

// LogBackupTaskCountInc increases the count of log backup task.
func LogBackupTaskCountInc() {
	logBackupTaskCount.Inc()
	log.Info("inc log backup task", zap.Int32("count", logBackupTaskCount.Load()))
}

// LogBackupTaskCountDec decreases the count of log backup task.
func LogBackupTaskCountDec() {
	logBackupTaskCount.Dec()
	log.Info("dec log backup task", zap.Int32("count", logBackupTaskCount.Load()))
}

// CheckLogBackupTaskExist checks that whether log-backup is existed.
func CheckLogBackupTaskExist() bool {
	return logBackupTaskCount.Load() > 0
}

// IsLogBackupInUse checks the log backup task existed.
func IsLogBackupInUse(ctx sessionctx.Context) bool {
	return CheckLogBackupTaskExist()
}

// GetTidbNewCollationEnabled returns the variable name of NewCollationEnabled.
func GetTidbNewCollationEnabled() string {
	return TidbNewCollationEnabled
}
