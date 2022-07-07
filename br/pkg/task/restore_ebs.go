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
	"encoding/json"
	"os"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/aws"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	flagOutputMetaFile   = "output-file"
	flagVolumeType       = "volume-type"
	flagVolumeIOPS       = "volume-iops"
	flagVolumeThroughput = "volume-throughput"
)

// DefineRestoreEBSMetaFlags defines common flags for the backup command.
func DefineRestoreEBSMetaFlags(command *cobra.Command) {
	command.Flags().String(flagOutputMetaFile, "output.json", "the file path of output meta file")
	command.Flags().Bool(flagDryRun, false, "don't access to aws environment if set to true")
	command.Flags().String(flagVolumeType, string(config.GP3Volume), "volume type: gp3, io1, io2")
	command.Flags().Uint(flagVolumeIOPS, 0, "volume iops(0 means default for that volume type)")
	command.Flags().Uint(flagVolumeThroughput, 0, "volume throughout in MiB/s(0 means default for that volume type)")
}

type RestoreEBSConfig struct {
	Config
	OutputFile       string               `json:"output-file"`
	DryRun           bool                 `json:"dry-run"`
	VolumeType       config.EBSVolumeType `json:"volume-type"`
	VolumeIOPS       uint32               `json:"volume-iops"`
	VolumeThroughput uint32               `json:"volume-throughput"`
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreEBSConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.DryRun, err = flags.GetBool(flagDryRun)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.OutputFile, err = flags.GetString(flagOutputMetaFile)
	if err != nil {
		return errors.Trace(err)
	}
	volumeType, err := flags.GetString(flagVolumeType)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.VolumeType = config.EBSVolumeType(volumeType)
	if !cfg.VolumeType.Valid() {
		return errors.New("invalid volume type: " + volumeType)
	}
	if cfg.VolumeIOPS, err = flags.GetUint32(flagVolumeIOPS); err != nil {
		return errors.Trace(err)
	}
	if cfg.VolumeThroughput, err = flags.GetUint32(flagVolumeThroughput); err != nil {
		return errors.Trace(err)
	}
	// iops: gp3 [3,000-16,000]; io1/io2 [100-32,000]
	// throughput: gp3 [125, 1000]; io1/io2 cannot set throughput
	// io1 and io2 volumes support up to 64,000 IOPS only on Instances built on the Nitro System.
	// Other instance families support performance up to 32,000 IOPS.
	// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CreateVolume.html
	// todo: check lower/upper bound

	return cfg.Config.ParseFromFlags(flags)
}

// RunRestoreEBSMeta phase 1 of EBS based restore
func RunRestoreEBSMeta(c context.Context, g glue.Glue, cmdName string, cfg *RestoreEBSConfig) error {
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
	cfg     *RestoreEBSConfig

	externStorage storage.ExternalStorage
	metaInfo      *config.EBSBasedBRMeta
	mgr           *conn.Mgr
}

// we don't call close of fields on failure, outer logic should call helper.close.
func (h *restoreEBSMetaHelper) preRestore(ctx context.Context) error {
	_, externStorage, err := GetStorage(ctx, h.cfg.Config.Storage, &h.cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	h.externStorage = externStorage

	mgr, err := NewMgr(ctx, h.g, h.cfg.PD, h.cfg.TLS, GetKeepalive(&h.cfg.Config), h.cfg.CheckRequirements, false)
	if err != nil {
		return errors.Trace(err)
	}
	h.mgr = mgr

	// read meta from s3
	metaInfo, err := h.getBackupMetaInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	h.metaInfo = metaInfo

	// todo: check whether target cluster is compatible with the backup
	// but cluster hasn't bootstrapped, we cannot get cluster version from pd now.

	return nil
}

func (h *restoreEBSMetaHelper) close() {
	if h.mgr != nil {
		h.mgr.Close()
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

	storeCount := h.metaInfo.GetStoreCount()
	progress := h.g.StartProgress(ctx, h.cmdName, int64(storeCount), !h.cfg.LogProgress)
	defer progress.Close()
	go progressFileWriterRoutine(ctx, progress, int64(storeCount))

	resolvedTs = h.metaInfo.ClusterInfo.ResolvedTS
	if h.cfg.DryRun {
		totalSize = 1234
		for i := 0; i < int(storeCount); i++ {
			progress.Inc()
			log.Info("mock: create volume from snapshot finished.", zap.Int("index", i))
			time.Sleep(800 * time.Millisecond)
		}
	} else {
		if totalSize, err = h.doRestore(ctx, progress); err != nil {
			return errors.Trace(err)
		}
	}

	if err = h.writeOutputFile(); err != nil {
		return errors.Trace(err)
	}
	finished = true
	return nil
}

func (h *restoreEBSMetaHelper) doRestore(ctx context.Context, progress glue.Progress) (int64, error) {
	log.Info("mark recovering")
	if err := h.mgr.MarkRecovering(ctx); err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("recover base alloc id")
	if err := h.mgr.RecoverBaseAllocID(ctx, h.metaInfo.ClusterInfo.MaxAllocID); err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("set pd ts = max(resolved_ts, current pd ts)")
	if err := h.mgr.ResetTS(ctx, h.metaInfo.ClusterInfo.ResolvedTS); err != nil {
		return 0, errors.Trace(err)
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
	)
	ec2Session, err = aws.NewEC2Session()
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			log.Error("failed to create all volumes, cleaning up created volume")
			ec2Session.DeleteVolumes(volumeIDMap)
		}
	}()
	volumeIDMap, err = ec2Session.CreateVolumes(h.metaInfo,
		string(h.cfg.VolumeType), int64(h.cfg.VolumeIOPS), int64(h.cfg.VolumeThroughput))
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	totalSize, err = ec2Session.WaitVolumesCreated(volumeIDMap, progress)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return volumeIDMap, totalSize, nil
}

func (h *restoreEBSMetaHelper) getBackupMetaInfo(ctx context.Context) (*config.EBSBasedBRMeta, error) {
	metaBytes, err := h.externStorage.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaInfo := &config.EBSBasedBRMeta{}
	err = json.Unmarshal(metaBytes, metaInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = h.checkEBSBRMeta(metaInfo); err != nil {
		return nil, errors.Trace(err)
	}
	return metaInfo, nil
}

func (h *restoreEBSMetaHelper) checkEBSBRMeta(meta *config.EBSBasedBRMeta) error {
	if meta.ClusterInfo == nil {
		return errors.New("no cluster info")
	}
	if _, err := semver.NewVersion(meta.ClusterInfo.Version); err != nil {
		return errors.Annotatef(err, "invalid cluster version")
	}
	if meta.ClusterInfo.ResolvedTS == 0 {
		return errors.New("invalid resolved ts")
	}
	if meta.GetStoreCount() == 0 {
		return errors.New("tikv info is empty")
	}
	return nil
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
