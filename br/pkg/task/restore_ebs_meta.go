// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/aws"
	"github.com/pingcap/tidb/br/pkg/config"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	flagPrepare          = "prepare"
	flagOutputMetaFile   = "output-file"
	flagVolumeType       = "volume-type"
	flagVolumeIOPS       = "volume-iops"
	flagVolumeThroughput = "volume-throughput"
	flagVolumeEncrypted  = "volume-encrypted"
	flagTargetAZ         = "target-az"
)

// DefineRestoreSnapshotFlags defines common flags for the backup command.
func DefineRestoreSnapshotFlags(command *cobra.Command) {
	command.Flags().String(flagFullBackupType, string(FullBackupTypeKV), "full backup type")
	command.Flags().Bool(flagPrepare, false, "prepare for snapshot based restore")
	command.Flags().String(flagOutputMetaFile, "output.json", "the file path of output meta file")
	command.Flags().Bool(flagSkipAWS, false, "don't access to aws environment if set to true")
	command.Flags().Uint(flagCloudAPIConcurrency, defaultCloudAPIConcurrency, "concurrency of calling cloud api")
	command.Flags().String(flagVolumeType, string(config.GP3Volume), "volume type: gp3, io1, io2")
	command.Flags().Int64(flagVolumeIOPS, 0, "volume iops(0 means default for that volume type)")
	command.Flags().Int64(flagVolumeThroughput, 0, "volume throughout in MiB/s(0 means default for that volume type)")
	command.Flags().Bool(flagVolumeEncrypted, false, "whether encryption is enabled for the volume")
	command.Flags().String(flagProgressFile, "progress.txt", "the file name of progress file")
	command.Flags().String(flagTargetAZ, "", "the target AZ for restored volumes")

	_ = command.Flags().MarkHidden(flagFullBackupType)
	_ = command.Flags().MarkHidden(flagPrepare)
	_ = command.Flags().MarkHidden(flagOutputMetaFile)
	_ = command.Flags().MarkHidden(flagSkipAWS)
	_ = command.Flags().MarkHidden(flagCloudAPIConcurrency)
	_ = command.Flags().MarkHidden(flagVolumeType)
	_ = command.Flags().MarkHidden(flagVolumeIOPS)
	_ = command.Flags().MarkHidden(flagVolumeThroughput)
	_ = command.Flags().MarkHidden(flagVolumeEncrypted)
	_ = command.Flags().MarkHidden(flagProgressFile)
	_ = command.Flags().MarkHidden(flagTargetAZ)
}

// RunRestoreEBSMeta phase 1 of EBS based restore
func RunRestoreEBSMeta(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.Adjust()
	helper := restoreEBSMetaHelper{
		rootCtx: c,
		g:       g,
		cmdName: cmdName,
		cfg:     cfg,
	}
	defer helper.close()
	return helper.restore()
}

type restoreEBSMetaHelper struct {
	rootCtx context.Context
	g       glue.Glue
	cmdName string
	cfg     *RestoreConfig

	metaInfo *config.EBSBasedBRMeta
	pdc      *pdutil.PdController
}

// we don't call close of fields on failure, outer logic should call helper.close.
func (h *restoreEBSMetaHelper) preRestore(ctx context.Context) error {
	_, externStorage, err := GetStorage(ctx, h.cfg.Config.Storage, &h.cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	// read meta from s3
	metaInfo, err := config.NewMetaFromStorage(ctx, externStorage)
	if err != nil {
		return errors.Trace(err)
	}
	if FullBackupType(metaInfo.GetFullBackupType()) != FullBackupTypeEBS {
		log.Error("invalid meta file", zap.Reflect("meta", metaInfo))
		return errors.New("invalid meta file, only support aws-ebs now")
	}
	h.metaInfo = metaInfo

	var (
		tlsConf *tls.Config
	)
	if len(h.cfg.PD) == 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "pd address can not be empty")
	}

	securityOption := pd.SecurityOption{}
	if h.cfg.TLS.IsEnabled() {
		securityOption.CAPath = h.cfg.TLS.CA
		securityOption.CertPath = h.cfg.TLS.Cert
		securityOption.KeyPath = h.cfg.TLS.Key
		tlsConf, err = h.cfg.TLS.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
	}

	controller, err := pdutil.NewPdController(ctx, h.cfg.PD, tlsConf, securityOption)
	if err != nil {
		log.Error("fail to create pd controller", zap.Error(err))
		return errors.Trace(err)
	}

	h.pdc = controller

	// todo: check whether target cluster is compatible with the backup
	// but cluster hasn't bootstrapped, we cannot get cluster version from pd now.
	return nil
}

func (h *restoreEBSMetaHelper) close() {
	if h.pdc != nil {
		h.pdc.Close()
	}
}

func (h *restoreEBSMetaHelper) restore() error {
	var (
		finished   bool
		totalSize  int64
		resolvedTs uint64
		err        error
	)
	defer func() {
		if finished {
			summary.Log("EBS restore success", zap.Int64("size", totalSize), zap.Uint64("resolved_ts", resolvedTs))
		} else {
			summary.Log("EBS restore failed, please check the log for details.")
		}
	}()
	ctx, cancel := context.WithCancel(h.rootCtx)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestoreEBSMeta", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	if err = h.preRestore(ctx); err != nil {
		return errors.Trace(err)
	}

	volumeCount := h.metaInfo.GetStoreCount() * h.metaInfo.GetTiKVVolumeCount()
	progress := h.g.StartProgress(ctx, h.cmdName, int64(volumeCount), !h.cfg.LogProgress)
	defer progress.Close()
	go progressFileWriterRoutine(ctx, progress, int64(volumeCount), h.cfg.ProgressFile)

	resolvedTs = h.metaInfo.ClusterInfo.ResolvedTS
	if totalSize, err = h.doRestore(ctx, progress); err != nil {
		return errors.Trace(err)
	}

	if err = h.writeOutputFile(); err != nil {
		return errors.Trace(err)
	}
	finished = true
	return nil
}
func (h *restoreEBSMetaHelper) doRestore(ctx context.Context, progress glue.Progress) (int64, error) {
	log.Info("mark recovering")
	if err := h.pdc.MarkRecovering(ctx); err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("set pd ts = max(resolved_ts, current pd ts)", zap.Uint64("resolved ts", h.metaInfo.ClusterInfo.ResolvedTS))
	if err := h.pdc.ResetTS(ctx, h.metaInfo.ClusterInfo.ResolvedTS); err != nil {
		return 0, errors.Trace(err)
	}

	if h.cfg.SkipAWS {
		for i := 0; i < int(h.metaInfo.GetStoreCount()); i++ {
			progress.Inc()
			log.Info("mock: create volume from snapshot finished.", zap.Int("index", i))
			time.Sleep(800 * time.Millisecond)
		}
		return 1234, nil
	}

	volumeIDMap, totalSize, err := h.restoreVolumes(progress)
	if err != nil {
		return 0, errors.Trace(err)
	}

	h.metaInfo.SetRestoreVolumeIDs(volumeIDMap)
	return totalSize, nil
}

func (h *restoreEBSMetaHelper) restoreVolumes(progress glue.Progress) (map[string]string, int64, error) {
	log.Info("create volume from snapshots")
	var (
		ec2Session  *aws.EC2Session
		volumeIDMap = make(map[string]string)
		err         error
		totalSize   int64
		// a map whose key is available zone, and value is the snapshot id array
		snapshotsIDsMap = make(map[string][]*string)
	)
	ec2Session, err = aws.NewEC2Session(h.cfg.CloudAPIConcurrency, h.cfg.S3.Region)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			log.Error("failed to create all volumes, cleaning up created volume")
			ec2Session.DeleteVolumes(volumeIDMap)
		}

		if h.cfg.UseFSR {
			err = ec2Session.DisableDataFSR(snapshotsIDsMap)
			if err != nil {
				log.Error("disable fsr failed", zap.Error(err))
			}
		}
	}()

	// Turn on FSR for TiKV data snapshots
	if h.cfg.UseFSR {
		snapshotsIDsMap, err = ec2Session.EnableDataFSR(h.metaInfo, h.cfg.TargetAZ)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	}

	volumeIDMap, err = ec2Session.CreateVolumes(h.metaInfo,
		string(h.cfg.VolumeType), h.cfg.VolumeIOPS, h.cfg.VolumeThroughput, h.cfg.VolumeEncrypted, h.cfg.TargetAZ)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	totalSize, err = ec2Session.WaitVolumesCreated(volumeIDMap, progress, h.cfg.UseFSR)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return volumeIDMap, totalSize, nil
}

func (h *restoreEBSMetaHelper) writeOutputFile() error {
	data, err := json.Marshal(h.metaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	err = os.WriteFile(h.cfg.OutputFile, data, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
