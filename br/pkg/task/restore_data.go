// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
)

func ReadBackupMetaData(ctx context.Context, s storage.ExternalStorage) (uint64, int, error) {
	metaInfo, err := config.NewMetaFromStorage(ctx, s)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if FullBackupType(metaInfo.GetFullBackupType()) != FullBackupTypeEBS {
		log.Error("invalid meta file", zap.Reflect("meta", metaInfo))
		return 0, 0, errors.New("invalid meta file, only support aws-ebs now")
	}
	return metaInfo.GetResolvedTS(), metaInfo.TiKVComponent.Replicas, nil
}

// RunResolveKvData starts a restore task inside the current goroutine.
func RunResolveKvData(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.Adjust()
	startAll := time.Now()
	defer summary.Summary(cmdName)

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// genenic work included opentrace, and restore client etc.
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.runResolveKvData", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// read the backup meta resolved ts and total tikvs from backup storage
	var resolveTs uint64
	_, externStorage, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	resolveTs, numBackupStore, err := ReadBackupMetaData(ctx, externStorage)
	if err != nil {
		return errors.Trace(err)
	}
	summary.CollectUint("resolve-ts", resolveTs)

	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, false, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, false)

	restoreTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// stop gc before restore tikv data
	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}

	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	// stop scheduler before recover data
	log.Info("starting to remove some PD schedulers")
	restoreFunc, e := mgr.RemoveAllPDSchedulers(ctx)
	if e != nil {
		return errors.Trace(err)
	}
	defer func() {
		if ctx.Err() != nil {
			log.Warn("context canceled, doing clean work with background context")
			ctx = context.Background()
		}
		if restoreE := restoreFunc(ctx); restoreE != nil {
			log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
		}
	}()

	var allStores []*metapb.Store
	err = utils.WithRetry(
		ctx,
		func() error {
			allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), util.SkipTiFlash)
			if err != nil {
				return errors.Trace(err)
			}
			numOnlineStore := len(allStores)
			// in this version, it suppose to have the same number of tikvs between backup cluster and restore cluster
			if numOnlineStore != numBackupStore {
				log.Warn("the restore meta contains the number of tikvs inconsist with the resore cluster, retry ...", zap.Int("current stores", len(allStores)), zap.Int("backup stores", numBackupStore))
				return errors.Annotatef(berrors.ErrRestoreTotalKVMismatch,
					"number of tikvs mismatch")
			}
			return nil
		},
		utils.NewPDReqBackofferExt(),
	)

	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("total tikv", zap.Int("total", numBackupStore), zap.String("progress file", cfg.ProgressFile))
	// progress = read meta + send recovery + iterate tikv + resolve kv data.
	progress := g.StartProgress(ctx, cmdName, int64(numBackupStore*4), !cfg.LogProgress)
	go progressFileWriterRoutine(ctx, progress, int64(numBackupStore*4), cfg.ProgressFile)

	// restore tikv data from a snapshot volume
	var totalRegions int

	totalRegions, err = restore.RecoverData(ctx, resolveTs, allStores, mgr, progress)
	if err != nil {
		return errors.Trace(err)
	}

	summary.CollectInt("total regions", totalRegions)
	log.Info("unmark recovering to pd")
	if err := mgr.UnmarkRecovering(ctx); err != nil {
		return errors.Trace(err)
	}

	//TODO: restore volume type into origin type
	//ModifyVolume(*ec2.ModifyVolumeInput) (*ec2.ModifyVolumeOutput, error) by backupmeta

	progress.Close()
	summary.CollectDuration("restore duration", time.Since(startAll))
	summary.SetSuccessStatus(true)
	return nil
}
