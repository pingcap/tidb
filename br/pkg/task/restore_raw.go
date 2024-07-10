// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	kvconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// RestoreRawConfig is the configuration specific for raw kv restore tasks.
type RestoreRawConfig struct {
	RawKvConfig
	RestoreCommonConfig
}

// DefineRawRestoreFlags defines common flags for the backup command.
func DefineRawRestoreFlags(command *cobra.Command) {
	command.Flags().StringP(flagKeyFormat, "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP(flagTiKVColumnFamily, "", "default", "restore specify cf, correspond to tikv cf")
	command.Flags().StringP(flagStartKey, "", "", "restore raw kv start key, key is inclusive")
	command.Flags().StringP(flagEndKey, "", "", "restore raw kv end key, key is exclusive")

	DefineRestoreCommonFlags(command.PersistentFlags())
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *RestoreRawConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	err = cfg.RestoreCommonConfig.ParseFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	return cfg.RawKvConfig.ParseFromFlags(flags)
}

func (cfg *RestoreRawConfig) adjust() {
	cfg.Config.adjust()
	cfg.RestoreCommonConfig.adjust()

	if cfg.Concurrency == 0 {
		cfg.Concurrency = defaultRestoreConcurrency
	}
}

// RunRestoreRaw starts a raw kv restore task inside the current goroutine.
func RunRestoreRaw(c context.Context, g glue.Glue, cmdName string, cfg *RestoreRawConfig) (err error) {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// Restore raw does not need domain.
	needDomain := false
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config), cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	// need retrieve these configs from tikv if not set in command.
	kvConfigs := &kvconfig.KVConfig{
		MergeRegionSize:     cfg.MergeSmallRegionSizeBytes,
		MergeRegionKeyCount: cfg.MergeSmallRegionKeyCount,
	}

	if !kvConfigs.MergeRegionSize.Modified || !kvConfigs.MergeRegionKeyCount.Modified {
		// according to https://github.com/pingcap/tidb/issues/34167.
		// we should get the real config from tikv to adapt the dynamic region.
		httpCli := httputil.NewClient(mgr.GetTLSConfig())
		mgr.ProcessTiKVConfigs(ctx, kvConfigs, httpCli)
	}

	keepaliveCfg := GetKeepalive(&cfg.Config)
	// sometimes we have pooled the connections.
	// sending heartbeats in idle times is useful.
	keepaliveCfg.PermitWithoutStream = true
	client := snapclient.NewRestoreClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrencyPerStore(cfg.ConcurrencyPerStore.Value)
	err = client.Init(g, mgr.GetStorage())
	defer client.Close()
	if err != nil {
		return errors.Trace(err)
	}

	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	reader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.InitBackupMeta(c, backupMeta, u, reader, true); err != nil {
		return errors.Trace(err)
	}

	if !client.IsRawKvMode() {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do raw restore from transactional data")
	}

	files, err := client.GetFilesInRawRange(cfg.StartKey, cfg.EndKey, cfg.CF)
	if err != nil {
		return errors.Trace(err)
	}
	archiveSize := reader.ArchiveSize(ctx, files)
	g.Record(summary.RestoreDataSize, archiveSize)

	if len(files) == 0 {
		log.Info("all files are filtered out from the backup archive, nothing to restore")
		return nil
	}
	summary.CollectInt("restore files", len(files))

	ranges, _, err := restoreutils.MergeAndRewriteFileRanges(
		files, nil, kvConfigs.MergeRegionSize.Value, kvConfigs.MergeRegionKeyCount.Value)
	if err != nil {
		return errors.Trace(err)
	}

	// Redirect to log if there is no log file to avoid unreadable output.
	// TODO: How to show progress?
	updateCh := g.StartProgress(
		ctx,
		"Raw Restore",
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	// RawKV restore does not need to rewrite keys.
	err = client.SplitRanges(ctx, ranges, updateCh, true)
	if err != nil {
		return errors.Trace(err)
	}

	importModeSwitcher := restore.NewImportModeSwitcher(mgr.GetPDClient(), cfg.SwitchModeInterval, mgr.GetTLSConfig())
	restoreSchedulers, _, err := restore.RestorePreWork(ctx, mgr, importModeSwitcher, cfg.Online, true)
	if err != nil {
		return errors.Trace(err)
	}
	defer restore.RestorePostWork(ctx, importModeSwitcher, restoreSchedulers, cfg.Online)

	err = client.RestoreRaw(ctx, cfg.StartKey, cfg.EndKey, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	// Restore has finished.
	updateCh.Close()

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
