// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// RunBackupEBS starts a backup task to backup volume vai EBS snapshot.
func RunFileCopyBackup(c context.Context, g glue.Glue, cfg *BackupConfig) error {
	cfg.Adjust()

	var finished bool
	var totalSize int64
	var resolvedTs, backupStartTs uint64
	defer func() {
		if finished {
			summary.Log("File backup success", zap.Int64("size", totalSize), zap.Uint64("resolved_ts", resolvedTs), zap.Uint64("backup_start_ts", backupStartTs))
		} else {
			summary.Log("File backup failed, please check the log for details.")
		}
	}()

	cfg.adjust()

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackupEBS", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, false, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()
	client := backup.NewBackupClient(ctx, mgr)

	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
	if err = client.SetStorageAndCheckNotInUse(ctx, backend, &opts); err != nil {
		return errors.Trace(err)
	}
	err = client.SetLockFile(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	backupStartTs, err = client.GetCurrentTS(c)
	if err != nil {
		return errors.Trace(err)
	}

	// Step.1.1 stop scheduler as much as possible.
	log.Info("starting to remove some PD schedulers and pausing GC", zap.Bool("already-paused-by-operator", cfg.SkipPauseGCAndScheduler))
	var restoreFunc pdutil.UndoFunc

	if !cfg.SkipPauseGCAndScheduler {
		var e error
		restoreFunc, e = mgr.RemoveAllPDSchedulers(ctx)
		if e != nil {
			return errors.Trace(err)
		}
		defer func() {
			if ctx.Err() != nil {
				log.Warn("context canceled, doing clean work with background context")
				ctx = context.Background()
			}
			if restoreFunc == nil {
				return
			}
			if restoreE := restoreFunc(ctx); restoreE != nil {
				log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
			}
		}()
	}

	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	newBackupClientFn := func(ctx context.Context, storeAddr string) (
		brpb.BackupClient, *grpc.ClientConn, error) {
		bfConf := backoff.DefaultConfig
		bfConf.MaxDelay = 3 * time.Second

		connection, err := utils.GRPCConn(ctx, storeAddr, mgr.GetTLSConfig(),
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    cfg.GRPCKeepaliveTime,
				Timeout: cfg.GRPCKeepaliveTimeout,
			}),
		)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return brpb.NewBackupClient(connection), connection, nil
	}

	if err := backup.WaitAllScheduleStoppedAndNoRegionHole(ctx, allStores, newBackupClientFn); err != nil {
		return errors.Trace(err)
	}

	// Step.1.2 get global resolved ts and stop gc until all key range SST Files map generated.
	resolvedTs, err = mgr.GetMinResolvedTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !cfg.SkipPauseGCAndScheduler {
		sp := utils.BRServiceSafePoint{
			BackupTS: resolvedTs,
			TTL:      utils.DefaultBRGCSafePointTTL,
			ID:       utils.MakeSafePointID(),
		}
		log.Info("safe point will be stuck during ebs backup", zap.Object("safePoint", sp))
		err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Step.2 starts call prepare to generate map of key range and SST Files in all TiKVs.
	prepareReq := backuppb.PrepareRequest{
		SaveToStorage: false,
	}
	workers := utils.NewWorkerPool(uint(len(allStores)), "prepare")
	eg, ectx := errgroup.WithContext(ctx)
	for _, store := range allStores {
		storeId := store.Id
		workers.ApplyOnErrorGroup(eg, func() error {
			cli, err := mgr.GetBackupClient(ctx, storeId)
			if err != nil {
				log.Error("failed to create backup client for store", zap.Any("store", store))
				return err
			}
			resp, err := cli.Prepare(ectx, &prepareReq)
			if err != nil {
				log.Error("failed to prepare for store", zap.Any("store", store))
				return err
			}
			// TODO handle unique id in resp.
			log.Info("prepare for store", zap.Any("store", store), zap.String("unique_id", resp.UniqueId))
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return err
	}

	backupReq := backuppb.BackupRequest{
		ClusterId:    client.GetClusterID(),
		StartVersion: cfg.LastBackupTS,
		// use resolved ts as backup ts.
		EndVersion:     resolvedTs,
		RateLimit:      cfg.RateLimit,
		StorageBackend: client.GetStorageBackend(),
		Concurrency:    defaultBackupConcurrency,
		CipherInfo:     &cfg.CipherInfo,
		// use file copy backup
		Mode: backuppb.BackupMode_FILE,
	}
	brVersion := g.GetVersion()
	clusterVersion, err := mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ranges, _, _, err := client.BuildBackupRangeAndSchema(mgr.GetStorage(), cfg.TableFilter, resolvedTs, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Metafile size should be less than 64MB.
	metawriter := metautil.NewMetaWriter(client.GetStorage(),
		metautil.MetaFileSize, cfg.UseBackupMetaV2, "", &cfg.CipherInfo)
	// Hack way to update backupmeta.
	metawriter.Update(func(m *backuppb.BackupMeta) {
		m.StartVersion = backupReq.StartVersion
		m.EndVersion = backupReq.EndVersion
		m.ClusterId = backupReq.ClusterId
		m.ClusterVersion = clusterVersion
		m.BrVersion = brVersion
	})

	// nothing to backup
	if len(ranges) == 0 {
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to backup, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))

		err = metawriter.FlushBackupMeta(ctx)
		if err == nil {
			summary.SetSuccessStatus(true)
		}
		return err
	}

	summary.CollectInt("backup total ranges", len(ranges))

	var updateCh glue.Progress
	var unit backup.ProgressUnit
	if len(ranges) < 100 {
		unit = backup.RegionUnit
		// The number of regions need to backup
		approximateRegions := 0
		for _, r := range ranges {
			var regionCount int
			regionCount, err = mgr.GetRegionCount(ctx, r.StartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			approximateRegions += regionCount
		}
		// Redirect to log if there is no log file to avoid unreadable output.
		updateCh = g.StartProgress(
			ctx, "File Backup", int64(approximateRegions), !cfg.LogProgress)
		summary.CollectInt("backup total regions", approximateRegions)
	} else {
		unit = backup.RangeUnit
		// To reduce the costs, we can use the range as unit of progress.
		updateCh = g.StartProgress(
			ctx, "File Backup", int64(len(ranges)), !cfg.LogProgress)
	}

	progressCount := uint64(0)
	progressCallBack := func(callBackUnit backup.ProgressUnit) {
		if unit == callBackUnit {
			updateCh.Inc()
			atomic.AddUint64(&progressCount, 1)
		}
	}

	metawriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	err = client.BackupRanges(ctx, ranges, backupReq, uint(cfg.Concurrency), cfg.ReplicaReadLabel, metawriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}

	err = metawriter.FinishWriteMetas(ctx, metautil.AppendDataFile)
	if err != nil {
		return errors.Trace(err)
	}

	// Step.3 save backup meta file to s3.
	// NOTE: maybe define the meta file in kvproto in the future.
	err = metawriter.FlushBackupMeta(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	finished = true
	return nil
}
