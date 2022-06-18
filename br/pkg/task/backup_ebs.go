// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/pflag"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

const (
	flagBackupVolumeFile = "volume-file"
)

// BackupEBSConfig is the configuration specific for backup tasks.
type BackupEBSConfig struct {
	Config

	VolumeFile string `json:"volume-file"`
}

// DefineBackupEBSFlags defines common flags for the backup command.
func DefineBackupEBSFlags(flags *pflag.FlagSet) {
	flags.String(flagBackupVolumeFile, "./backup.toml", "the file path of volume infos of TiKV node")
}

// RunBackupEBS starts a backup task to backup volume vai EBS snapshot.
func RunBackupEBS(c context.Context, g glue.Glue, cmdName string, cfg *BackupEBSConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackupEBS", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()
	client, err := backup.NewBackupClient(ctx, mgr)
	if err != nil {
		return errors.Trace(err)
	}

	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	err = client.SetLockFile(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Step.1.1 get global resolved ts and stop gc until all volumes ebs snapshot starts.
	// TODO: get resolved ts
	resolvedTs, err := client.GetTS(ctx, 10*time.Minute, 0)
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

	// Step.1.2 stop scheduler as much as possible.
	log.Info("starting to remove some PD schedulers")
	restore, e := mgr.RemoveSchedulers(ctx)
	defer func() {
		if ctx.Err() != nil {
			log.Warn("context canceled, doing clean work with background context")
			ctx = context.Background()
		}
		if restoreE := restore(ctx); restoreE != nil {
			log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
		}
	}()
	if e != nil {
		return errors.Trace(err)
	}

	// Step.1.3 backup the key info to recover cluster. e.g. PD alloc_id/cluster_id
	pdCli := mgr.GetPDClient()
	clusterID := pdCli.GetClusterID(ctx)
	allocID, err := pdCli.GetAllocID(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("get pd cluster info", zap.Uint64("cluster-id", clusterID), zap.Uint64("alloc-id", allocID))

	// Step.2 starts call ebs snapshot api to back up volume data.
	// NOTE: we should start snapshot in specify order.

	// receive the volume info from TiDB deployment tools.
	backupInfo := &backup.EBSBackupInfo{}
	err = backupInfo.ConfigFromFile(cfg.VolumeFile)
	if err != nil {
		return errors.Trace(err)
	}
	backupInfo.SetClusterID(clusterID)
	backupInfo.SetAllocID(allocID)
	log.Info("get backup info from file", zap.Any("info", backupInfo))

	sess, err := backup.NewEC2Session(backupInfo.Region)
	if err != nil {
		return errors.Trace(err)
	}
	allVolumes, err := sess.StartsEBSSnapshot(backupInfo)
	if err != nil {
		// TODO maybe we should consider remove snapshots already exists in a failure
		return errors.Trace(err)
	}
	log.Info("all ebs snapshots are starts.", zap.Int("count", len(allVolumes)))
	err = sess.WaitEBSSnapshotFinished(allVolumes)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("all ebs snapshots are finished.", zap.Int("count", len(allVolumes)))

	// Step.3 save backup meta file to s3.
	externalStorage := client.GetStorage()
	// NOTE: maybe define the meta file in kvproto in the future.
	// but for now json is enough.
	data, err := json.Marshal(backupInfo)
	if err != nil {
		return errors.Trace(err)
	}
	err = externalStorage.WriteFile(c, metautil.MetaFile, data)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("finished ebs backup.")
	return nil
}
