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
	flagOutputMetaFile = "output-file"
)

// DefineRestoreEBSMetaFlags defines common flags for the backup command.
func DefineRestoreEBSMetaFlags(command *cobra.Command) {
	command.Flags().String(flagOutputMetaFile, "output.json", "the file path of output meta file")
	command.Flags().Bool(flagDryRun, false, "don't access to aws environment if set to true")
}

type RestoreEBSConfig struct {
	Config
	OutputFile string `json:"output-file"`
	DryRun     bool   `json:"dry-run"`
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
// that's why we call it initFields to differ with init which call close of fields normally.
func (h *restoreEBSMetaHelper) initFields(ctx context.Context) error {
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
			summary.Log("restore EBS success", zap.Int64("size", totalSize), zap.Uint64("resolved_ts", resolvedTs))
		} else {
			summary.Log("restore EBS failed, please check the log for details.")
		}
	}()
	ctx, cancel := context.WithCancel(h.rootCtx)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestoreEBSMeta", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	if err = h.initFields(ctx); err != nil {
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
	// todo: create volume from snapshots
	// todo: save restored volumes back to output file
	return 0, nil
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
