// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	flagBackupDryRun     = "dry-run"
)

// BackupEBSConfig is the configuration specific for backup tasks.
type BackupEBSConfig struct {
	Config

	VolumeFile string `json:"volume-file"`
	DryRun     bool   `json:"dry-run"`
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupEBSConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.DryRun, err = flags.GetBool(flagDryRun)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.VolumeFile, err = flags.GetString(flagBackupVolumeFile)
	if err != nil {
		return errors.Trace(err)
	}

	return cfg.Config.ParseFromFlags(flags)
}

// DefineBackupEBSFlags defines common flags for the backup command.
func DefineBackupEBSFlags(flags *pflag.FlagSet) {
	flags.String(flagBackupVolumeFile, "./backup.json", "the file path of volume infos of TiKV node")
	flags.Bool(flagBackupDryRun, false, "don't access to aws environment if set to true")
}

// RunBackupEBS starts a backup task to backup volume vai EBS snapshot.
func RunBackupEBS(c context.Context, g glue.Glue, cmdName string, cfg *BackupEBSConfig) error {
	var finished bool
	var totalSize int64
	var resolvedTs uint64
	defer func() {
		if finished {
			summary.Log("EBS backup success", zap.Int64("size", totalSize), zap.Uint64("resolved_ts", resolvedTs))
		} else {
			summary.Log("EBS backup failed, please check the log for details.")
		}
	}()
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// receive the volume info from TiDB deployment tools.
	backupInfo := &backup.EBSBackupInfo{}
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
	if err = client.SetStorage(ctx, backend, &opts); err != nil {
		return errors.Trace(err)
	}
	err = client.SetLockFile(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Step.1.1 get global resolved ts and stop gc until all volumes ebs snapshot starts.
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

	// Step.1.2 stop scheduler as much as possible.
	log.Info("starting to remove some PD schedulers")
	restoreFunc, e := mgr.RemoveSchedulers(ctx)
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

	// Step.1.3 backup the key info to recover cluster. e.g. PD alloc_id/cluster_id
	allocID, err := mgr.GetBaseAllocID(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("get pd cluster info", zap.Uint64("alloc-id", allocID))

	// Step.2 starts call ebs snapshot api to back up volume data.
	// NOTE: we should start snapshot in specify order.

	// write progress in tmp file for tidb-operator, so tidb-operator can retrieve the
	// progress of ebs backup. and user can get the progress through `kubectl get job`
	progress := g.StartProgress(ctx, cmdName, int64(storeCount), !cfg.LogProgress)
	go func() {
		fileName := "progress.txt"
		// remove tmp file
		defer os.Remove(fileName)

		for progress.GetCurrent() < int64(storeCount) {
			time.Sleep(500 * time.Millisecond)
			cur := progress.GetCurrent()
			p := float64(cur) / float64(storeCount)
			p *= 100
			err = os.WriteFile(fileName, []byte(fmt.Sprintf("%.2f", p)), 0600)
			if err != nil {
				log.Warn("failed to update tmp progress file", zap.Error(err))
			}
		}
	}()

	ec2Session, err := backup.NewEC2Session()
	if err != nil {
		return errors.Trace(err)
	}
	snapIDMap := make(map[uint64]map[string]string)
	if !cfg.DryRun {
		log.Info("start async snapshots")
		snapIDMap, err = ec2Session.StartsEBSSnapshot(backupInfo)
		if err != nil {
			// TODO maybe we should consider remove snapshots already exists in a failure
			return errors.Trace(err)
		}
		log.Info("wait async snapshots finish")
		totalSize, err = ec2Session.WaitSnapshotFinished(snapIDMap, progress)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("async snapshots finished.")
	} else {
		for i := 0; i < int(storeCount); i++ {
			progress.Inc()
			totalSize = 1024
			log.Info("mock snapshot finished.", zap.Int("index", i))
			time.Sleep(800 * time.Millisecond)
		}
	}
	progress.Close()

	// Step.3 save backup meta file to s3.
	// NOTE: maybe define the meta file in kvproto in the future.
	// but for now json is enough.
	backupInfo.SetAllocID(allocID)
	backupInfo.SetResolvedTS(resolvedTs)
	backupInfo.SetSnapshotIDs(snapIDMap)
	if err2 := saveMetaFile(c, backupInfo, client.GetStorage()); err2 != nil {
		return err2
	}
	finished = true
	return nil
}

func saveMetaFile(c context.Context, backupInfo *backup.EBSBackupInfo, externalStorage storage.ExternalStorage) error {
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
