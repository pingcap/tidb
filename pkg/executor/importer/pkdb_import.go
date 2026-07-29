package importer

import (
	"context"
	"strconv"
	"time"

	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/sessionctx"

	"go.uber.org/zap"
)

func getSessionIntSysVar(ctx context.Context, seCtx sessionctx.Context, name string) (int, error) {
	v, err := seCtx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, name)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (ti *TableImporter) precheckConflictForImportFromSelect(
	ctx context.Context,
	closedDataEngine, closedIndexEngine *backend.ClosedEngine,
) error {
	precheckStart := time.Now()
	ti.logger.Info("start precheck conflict for IMPORT INTO FROM SELECT",
		zap.String("table", ti.tableInfo.Name),
		zap.Bool("hasClusteredIndex", ti.tableInfo.Core.HasClusteredIndex()),
		zap.Stringer("dataEngineUUID", closedDataEngine.GetUUID()),
		zap.Stringer("indexEngineUUID", closedIndexEngine.GetUUID()))
	ukIndexRanges, err := ti.backend.BuildIndexKeyRangesForUKPrecheck(closedIndexEngine.GetUUID(), ti.tableInfo.Core)
	if err != nil {
		return err
	}
	if len(ukIndexRanges) > 0 {
		if err = ti.backend.PrecheckConflictForImportEngineRanges(ctx, closedIndexEngine.GetUUID(), ukIndexRanges); err != nil {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				return local.ConvertToErrFoundConflictRecords(err, ti.encTable)
			}
			return err
		}
	}
	if ti.tableInfo.Core.HasClusteredIndex() {
		pkIndexRange, err := ti.backend.BuildClusteredPKRangesForPrecheck(closedDataEngine.GetUUID())
		if err != nil {
			return err
		}
		if pkIndexRange.StartKey != nil {
			if err = ti.backend.PrecheckConflictForImportEngineRanges(ctx, closedDataEngine.GetUUID(), []tidbkv.KeyRange{pkIndexRange}); err != nil {
				if common.ErrFoundDuplicateKeys.Equal(err) {
					return local.ConvertToErrFoundConflictRecords(err, ti.encTable)
				}
				return err
			}
		}
	}
	ti.logger.Info("finish precheck conflict for IMPORT INTO FROM SELECT",
		zap.String("table", ti.tableInfo.Name),
		zap.Bool("hasClusteredIndex", ti.tableInfo.Core.HasClusteredIndex()),
		zap.Stringer("dataEngineUUID", closedDataEngine.GetUUID()),
		zap.Stringer("indexEngineUUID", closedIndexEngine.GetUUID()),
		zap.Duration("takeTime", time.Since(precheckStart)))
	return nil
}

func initializeImportFromSelectChecksum(
	ctx context.Context,
	se sessionctx.Context,
	plan *Plan,
	logger *zap.Logger,
) (verify.KVChecksum, bool, error) {
	if plan.Checksum == config.OpLevelOff {
		return verify.KVChecksum{}, false, nil
	}
	checksumStart := time.Now()
	logger.Info("start initialize remote checksum for IMPORT INTO FROM SELECT",
		zap.String("db", plan.DBName),
		zap.String("table", plan.TableInfo.Name.L))
	remoteChecksum, err := RemoteChecksumTableByTiKV(ctx, se, plan, logger)
	logger.Info("finish initialize remote checksum for IMPORT INTO FROM SELECT",
		zap.String("db", plan.DBName),
		zap.String("table", plan.TableInfo.Name.L),
		zap.Duration("takeTime", time.Since(checksumStart)),
		zap.Error(err))
	if err != nil {
		if plan.Checksum != config.OpLevelOptional {
			return verify.KVChecksum{}, false, err
		}
		logger.Warn("get initial remote checksum failed, skip base checksum initialization", zap.Error(err))
		return verify.KVChecksum{}, false, nil
	}
	return verify.MakeKVChecksum(remoteChecksum.TotalBytes, remoteChecksum.TotalKVs, remoteChecksum.Checksum), true, nil
}

type autoIDAssignment struct {
	incrementID int64
	randomShard int64
	randomID    int64
}
