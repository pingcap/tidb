// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/aws"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/common"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	flagBackupVolumeFile           = "volume-file"
	flagProgressFile               = "progress-file"
	waitAllScheduleStoppedInterval = 15 * time.Second
)

// todo: need a better name
var errHasPendingAdmin = errors.New("has pending admin")

// DefineBackupEBSFlags defines common flags for the backup command.
func DefineBackupEBSFlags(flags *pflag.FlagSet) {
	flags.String(flagFullBackupType, string(FullBackupTypeKV), "full backup type")
	flags.String(flagBackupVolumeFile, "./backup.json", "the file path of volume infos of TiKV node")
	flags.Bool(flagSkipAWS, false, "don't access to aws environment if set to true")
	flags.Uint(flagCloudAPIConcurrency, defaultCloudAPIConcurrency, "concurrency of calling cloud api")
	flags.String(flagProgressFile, "progress.txt", "the file name of progress file")

	_ = flags.MarkHidden(flagFullBackupType)
	_ = flags.MarkHidden(flagBackupVolumeFile)
	_ = flags.MarkHidden(flagSkipAWS)
	_ = flags.MarkHidden(flagCloudAPIConcurrency)
	_ = flags.MarkHidden(flagProgressFile)
}

// RunBackupEBS starts a backup task to backup volume vai EBS snapshot.
func RunBackupEBS(c context.Context, g glue.Glue, cfg *BackupConfig) error {
	cfg.Adjust()

	var finished bool
	var totalSize int64
	var resolvedTs, backupStartTs uint64
	defer func() {
		if finished {
			summary.Log("EBS backup success", zap.Int64("size", totalSize), zap.Uint64("resolved_ts", resolvedTs), zap.Uint64("backup_start_ts", backupStartTs))
		} else {
			summary.Log("EBS backup failed, please check the log for details.")
		}
	}()

	cfg.adjust()

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// receive the volume info from TiDB deployment tools.
	backupInfo := &config.EBSBasedBRMeta{}
	err := backupInfo.ConfigFromFile(cfg.VolumeFile)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("get backup info from file", zap.Any("info", backupInfo))
	storeCount := backupInfo.GetStoreCount()
	if storeCount == 0 {
		log.Info("nothing to backup")
		return nil
	}

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

	backupStartTs, err = client.GetCurerntTS(c)
	if err != nil {
		return errors.Trace(err)
	}

	// Step.1.1 stop scheduler as much as possible.
	log.Info("starting to remove some PD schedulers")
	restoreFunc, e := mgr.RemoveAllPDSchedulers(ctx)
	if e != nil {
		return errors.Trace(err)
	}
	var scheduleRestored bool
	defer func() {
		if ctx.Err() != nil {
			log.Warn("context canceled, doing clean work with background context")
			ctx = context.Background()
		}
		if scheduleRestored {
			return
		}
		if restoreE := restoreFunc(ctx); restoreE != nil {
			log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
		}
	}()

	if err := waitAllScheduleStoppedAndNoRegionHole(ctx, cfg.Config, mgr); err != nil {
		return errors.Trace(err)
	}

	// Step.1.2 get global resolved ts and stop gc until all volumes ebs snapshot starts.
	resolvedTs, err = mgr.GetMinResolvedTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
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

	// Step.1.3 backup the key info to recover cluster. e.g. PD alloc_id/cluster_id
	clusterVersion, err := mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	normalizedVer := version.NormalizeBackupVersion(clusterVersion)
	if normalizedVer == nil {
		return errors.New("invalid cluster version")
	}

	// Step.2 starts call ebs snapshot api to back up volume data.
	// NOTE: we should start snapshot in specify order.

	progress := g.StartProgress(ctx, "backup", int64(storeCount)*100, !cfg.LogProgress)
	go progressFileWriterRoutine(ctx, progress, int64(storeCount)*100, cfg.ProgressFile)

	ec2Session, err := aws.NewEC2Session(cfg.CloudAPIConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	snapIDMap := make(map[string]string)
	var volAZs aws.VolumeAZs
	if !cfg.SkipAWS {
		defer func() {
			if err != nil {
				log.Error("failed to backup ebs, cleaning up created volumes", zap.Error(err))
				ec2Session.DeleteSnapshots(snapIDMap)
			}
		}()
		log.Info("start async snapshots")
		snapIDMap, volAZs, err = ec2Session.CreateSnapshots(backupInfo)
		if err != nil {
			// TODO maybe we should consider remove snapshots already exists in a failure
			return errors.Trace(err)
		}

		log.Info("snapshot started, restore schedule")
		if restoreE := restoreFunc(ctx); restoreE != nil {
			log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
		} else {
			scheduleRestored = true
		}

		log.Info("wait async snapshots finish")
		totalSize, err = ec2Session.WaitSnapshotsCreated(snapIDMap, progress)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("async snapshots finished.")
	} else {
		for i := 0; i < int(storeCount); i++ {
			progress.IncBy(100)
			totalSize = 1024
			log.Info("mock snapshot finished.", zap.Int("index", i))
			time.Sleep(800 * time.Millisecond)
		}
	}
	progress.Close()

	// Step.3 save backup meta file to s3.
	// NOTE: maybe define the meta file in kvproto in the future.
	// but for now json is enough.
	backupInfo.SetClusterVersion(normalizedVer.String())
	backupInfo.SetFullBackupType(string(cfg.FullBackupType))
	backupInfo.SetResolvedTS(resolvedTs)
	backupInfo.SetSnapshotIDs(snapIDMap)
	backupInfo.SetVolumeAZs(volAZs)
	err = saveMetaFile(c, backupInfo, client.GetStorage())
	if err != nil {
		return err
	}
	finished = true
	return nil
}

func waitAllScheduleStoppedAndNoRegionHole(ctx context.Context, cfg Config, mgr *conn.Mgr) error {
	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	// we wait for nearly 15*40 = 600s = 10m
	backoffer := utils.InitialRetryState(40, 5*time.Second, waitAllScheduleStoppedInterval)
	for backoffer.Attempt() > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		allRegions, err2 := waitUntilAllScheduleStopped(ctx, cfg, allStores, mgr)
		if err2 != nil {
			if causeErr := errors.Cause(err2); causeErr == errHasPendingAdmin {
				log.Info("schedule ongoing on tikv, will retry later", zap.Error(err2))
			} else {
				log.Warn("failed to wait schedule, will retry later", zap.Error(err2))
			}
			continue
		}

		log.Info("all leader regions got, start checking hole", zap.Int("len", len(allRegions)))

		if !isRegionsHasHole(allRegions) {
			return nil
		}
		time.Sleep(backoffer.ExponentialBackoff())
	}
	return errors.New("failed to wait all schedule stopped")
}

func isRegionsHasHole(allRegions []*metapb.Region) bool {
	// sort by start key
	sort.Slice(allRegions, func(i, j int) bool {
		left, right := allRegions[i], allRegions[j]
		return bytes.Compare(left.StartKey, right.StartKey) < 0
	})

	for j := 0; j < len(allRegions)-1; j++ {
		left, right := allRegions[j], allRegions[j+1]
		// we don't need to handle the empty end key specially, since
		// we sort by start key of region, and the end key of the last region is not checked
		if !bytes.Equal(left.EndKey, right.StartKey) {
			log.Info("region hole found", zap.Reflect("left-region", left), zap.Reflect("right-region", right))
			return true
		}
	}
	return false
}

func waitUntilAllScheduleStopped(ctx context.Context, cfg Config, allStores []*metapb.Store, mgr *conn.Mgr) ([]*metapb.Region, error) {
	concurrency := mathutil.Min(len(allStores), common.MaxStoreConcurrency)
	workerPool := utils.NewWorkerPool(uint(concurrency), "collect schedule info")
	eg, ectx := errgroup.WithContext(ctx)

	// init this slice with guess that there are 100 leaders on each store
	var mutex sync.Mutex
	allRegions := make([]*metapb.Region, 0, len(allStores)*100)
	addRegionsFunc := func(regions []*metapb.Region) {
		mutex.Lock()
		defer mutex.Unlock()
		allRegions = append(allRegions, regions...)
	}
	for i := range allStores {
		store := allStores[i]
		if ectx.Err() != nil {
			break
		}
		workerPool.ApplyOnErrorGroup(eg, func() error {
			backupClient, connection, err := newBackupClient(ctx, store.Address, cfg, mgr.GetTLSConfig())
			if err != nil {
				return errors.Trace(err)
			}
			defer func() {
				_ = connection.Close()
			}()
			checkAdminClient, err := backupClient.CheckPendingAdminOp(ectx, &brpb.CheckAdminRequest{})
			if err != nil {
				return errors.Trace(err)
			}

			storeLeaderRegions := make([]*metapb.Region, 0, 100)
			for {
				response, err2 := checkAdminClient.Recv()
				if err2 != nil {
					causeErr := errors.Cause(err2)
					// other check routines may have HasPendingAdmin=true, so Recv() may receive canceled.
					// skip it to avoid error overriding
					if causeErr == io.EOF || causeErr == context.Canceled {
						break
					}
					return errors.Trace(err2)
				}
				if response.Error != nil {
					return errors.New(response.Error.String())
				}
				if response.Region == nil {
					return errors.New("region is nil")
				}
				if response.HasPendingAdmin {
					return errors.WithMessage(errHasPendingAdmin, fmt.Sprintf("store-id=%d", store.Id))
				}
				storeLeaderRegions = append(storeLeaderRegions, response.Region)
			}
			addRegionsFunc(storeLeaderRegions)
			return nil
		})
	}
	return allRegions, eg.Wait()
}

func newBackupClient(ctx context.Context, storeAddr string, cfg Config, tlsConfig *tls.Config) (
	brpb.BackupClient, *grpc.ClientConn, error) {
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = 3 * time.Second

	connection, err := utils.GRPCConn(ctx, storeAddr, tlsConfig,
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

func saveMetaFile(c context.Context, backupInfo *config.EBSBasedBRMeta, externalStorage storage.ExternalStorage) error {
	data, err := json.Marshal(backupInfo)
	if err != nil {
		return errors.Trace(err)
	}
	err = externalStorage.WriteFile(c, metautil.MetaFile, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
