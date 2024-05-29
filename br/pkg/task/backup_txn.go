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
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	flagStartVersion = "start-version"
)

// TxnKvConfig is the common config for txn kv backup and restore.
type TxnKvConfig struct {
	Config

	StartKey     []byte `json:"start-key" toml:"start-key"`
	EndKey       []byte `json:"end-key" toml:"end-key"`
	StartVersion int64  `json:"start-version" toml:"start-version"`

	CompressionConfig
	RemoveSchedulers bool `json:"remove-schedulers" toml:"remove-schedulers"`
}

// DefineTxnBackupFlags defines common flags for the backup command.
func DefineTxnBackupFlags(command *cobra.Command) {
	command.Flags().StringP(flagStartKey, "", "", "backup txn kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "backup txn kv end key, key is exclusive")
	command.Flags().Int64P(flagStartVersion, "", 0, "backup timestamp for txn kv")
	command.Flags().String(flagCompressionType, "zstd",
		"backup sst file compression algorithm, value can be one of 'lz4|zstd|snappy'")
	command.Flags().Bool(flagRemoveSchedulers, false,
		"disable the balance, shuffle and region-merge schedulers in PD to speed up backup")
	// This flag can impact the online cluster, so hide it in case of abuse.
	_ = command.Flags().MarkHidden(flagRemoveSchedulers)
}

// ParseFromFlags parses the txn kv backup&restore common flags from the flag set.
func (cfg *TxnKvConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	if len(cfg.StartKey) > 0 && len(cfg.EndKey) > 0 && bytes.Compare(cfg.StartKey, cfg.EndKey) >= 0 {
		return errors.Annotate(berrors.ErrBackupInvalidRange, "endKey must be greater than startKey")
	}
	if err := cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ParseBackupConfigFromFlags parses the backup-related flags from the flag set.
func (cfg *TxnKvConfig) ParseBackupConfigFromFlags(flags *pflag.FlagSet) error {
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

// Adjust Txn kv config for backup
func (cfg *TxnKvConfig) Adjust() {
	cfg.adjust()
	if cfg.Config.Concurrency == 0 {
		cfg.Config.Concurrency = defaultBackupConcurrency
	}
}

// RunBackupTxn starts a backup task inside the current goroutine.
func RunBackupTxn(c context.Context, g glue.Glue, cmdName string, cfg *TxnKvConfig) error {
	cfg.Adjust()
	defer summary.Summary(cmdName)

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunBackupTxn", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup txn does not need domain.
	needDomain := false
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	client := backup.NewBackupClient(ctx, mgr)
	opts := storage.ExternalStorageOptions{
		NoCredentials:            cfg.NoCreds,
		SendCredentials:          cfg.SendCreds,
		CheckS3ObjectLockOptions: true,
	}
	if err = client.SetStorageAndCheckNotInUse(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}

	backupRanges := make([]rtree.Range, 0, 1)
	// current just build full txn range to support full txn backup
	minStartKey := []byte{}
	maxEndKey := []byte{}
	backupRanges = append(backupRanges, rtree.Range{
		StartKey: minStartKey,
		EndKey:   maxEndKey,
	})

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
	approximateRegions, err := mgr.GetRegionCount(ctx, minStartKey, maxEndKey)
	if err != nil {
		return errors.Trace(err)
	}

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := g.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)

	progressCallBack := func() {
		updateCh.Inc()
	}
	backupTS, err := client.GetCurrentTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	g.Record("BackupTS", backupTS)

	req := backuppb.BackupRequest{
		ClusterId:        client.GetClusterID(),
		StartVersion:     0,
		EndVersion:       backupTS,
		RateLimit:        cfg.RateLimit,
		Concurrency:      cfg.Concurrency,
		StorageBackend:   client.GetStorageBackend(),
		IsRawKv:          false,
		CompressionType:  cfg.CompressionType,
		CompressionLevel: cfg.CompressionLevel,
		CipherInfo:       &cfg.CipherInfo,
	}

	metaWriter := metautil.NewMetaWriter(client.GetStorage(), metautil.MetaFileSize, false, metautil.MetaFile, &cfg.CipherInfo)
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	err = client.BackupRanges(ctx, backupRanges, req, 1, nil, metaWriter, progressCallBack)
	if err != nil {
		return errors.Trace(err)
	}
	// Backup has finished
	updateCh.Close()
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.StartVersion = req.StartVersion
		m.EndVersion = req.EndVersion
		m.IsRawKv = false
		m.IsTxnKv = true
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
