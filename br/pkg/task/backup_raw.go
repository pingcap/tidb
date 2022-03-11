// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"bytes"
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	flagKeyFormat        = "format"
	flagTiKVColumnFamily = "cf"
	flagStartKey         = "start"
	flagEndKey           = "end"
)

// RawKvConfig is the common config for rawkv backup and restore.
type RawKvConfig struct {
	Config

	StartKey []byte `json:"start-key" toml:"start-key"`
	EndKey   []byte `json:"end-key" toml:"end-key"`
	CF       string `json:"cf" toml:"cf"`
	CompressionConfig
	RemoveSchedulers bool `json:"remove-schedulers" toml:"remove-schedulers"`
}

// DefineRawBackupFlags defines common flags for the backup command.
func DefineRawBackupFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagTiKVColumnFamily, "", "default", "backup specify cf, correspond to tikv cf")
	command.Flags().StringP(flagStartKey, "", "", "backup raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "backup raw kv end key, key is exclusive")
	command.Flags().String(flagCompressionType, "zstd",
		"backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
	command.Flags().Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = command.Flags().MarkHidden(flagRemoveSchedulers)
}

// ParseFromFlags parses the raw kv backup&restore common flags from the flag set.
func (cfg *RawKvConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	format, err := flags.GetString(flagKeyFormat)
	if err != nil {
		return errors.Trace(err)
	}
	start, err := flags.GetString(flagStartKey)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.StartKey, err = utils.ParseKey(format, start)
	if err != nil {
		return errors.Trace(err)
	}
	end, err := flags.GetString(flagEndKey)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.EndKey, err = utils.ParseKey(format, end)
	if err != nil {
		return errors.Trace(err)
	}

	if len(cfg.StartKey) > 0 && len(cfg.EndKey) > 0 && bytes.Compare(cfg.StartKey, cfg.EndKey) >= 0 {
		return errors.Annotate(berrors.ErrBackupInvalidRange, "endKey must be greater than startKey")
	}
	cfg.CF, err = flags.GetString(flagTiKVColumnFamily)
	if err != nil {
		return errors.Trace(err)
	}
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ParseBackupConfigFromFlags parses the backup-related flags from the flag set.
func (cfg *RawKvConfig) ParseBackupConfigFromFlags(flags *pflag.FlagSet) error {
	err := cfg.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	compressionCfg, err := parseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg

	cfg.RemoveSchedulers, err = flags.GetBool(flagRemoveSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionLevel = level

	return nil
}

// RunBackupRaw starts a backup task inside the current goroutine.
func RunBackupRaw(c context.Context, g glue.Glue, cmdName string, cfg *RawKvConfig) error {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackupRaw", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup raw does not need domain.
	needDomain := false
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain)
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

	backupRange := rtree.Range{StartKey: cfg.StartKey, EndKey: cfg.EndKey}

	if cfg.RemoveSchedulers {
		restore, e := mgr.RemoveSchedulers(ctx)
		defer func() {
			if ctx.Err() != nil {
				log.Warn("context canceled, try shutdown")
				ctx = context.Background()
			}
			if restoreE := restore(ctx); restoreE != nil {
				log.Warn("failed to restore removed schedulers, you may need to restore them manually", zap.Error(restoreE))
			}
		}()
		if e != nil {
			return errors.Trace(err)
		}
	}

	brVersion := g.GetVersion()
	clusterVersion, err := mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// The number of regions need to backup
	approximateRegions, err := mgr.GetRegionCount(ctx, backupRange.StartKey, backupRange.EndKey)
	if err != nil {
		return errors.Trace(err)
	}

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)

	progressCallBack := func(unit backup.ProgressUnit) {
		if unit == backup.RangeUnit {
			return
		}
		updateCh.Inc()
	}

	req := backuppb.BackupRequest{
		ClusterId:        client.GetClusterID(),
		StartKey:         backupRange.StartKey,
		EndKey:           backupRange.StartKey,
		StartVersion:     0,
		EndVersion:       0,
		RateLimit:        cfg.RateLimit,
		Concurrency:      cfg.Concurrency,
		StorageBackend:   client.GetStorageBackend(),
		IsRawKv:          true,
		Cf:               cfg.CF,
		CompressionType:  cfg.CompressionType,
		CompressionLevel: cfg.CompressionLevel,
		CipherInfo:       &cfg.CipherInfo,
	}
	metaWriter := metautil.NewMetaWriter(client.GetStorage(), metautil.MetaFileSize, false, &cfg.CipherInfo)
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	err = client.BackupRange(ctx, req, metaWriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup has finished
	updateCh.Close()
	rawRanges := []*backuppb.RawRange{{StartKey: backupRange.StartKey, EndKey: backupRange.EndKey, Cf: cfg.CF}}
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.StartVersion = req.StartVersion
		m.EndVersion = req.EndVersion
		m.IsRawKv = req.IsRawKv
		m.RawRanges = rawRanges
		m.ClusterId = req.ClusterId
		m.ClusterVersion = clusterVersion
		m.BrVersion = brVersion
		m.ApiVersion = client.GetApiVersion()
	})
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDataFile)
	if err != nil {
		return errors.Trace(err)
	}

	err = metaWriter.FlushBackupMeta(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	g.Record(summary.BackupDataSize, metaWriter.ArchiveSize())

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
