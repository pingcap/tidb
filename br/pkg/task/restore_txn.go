// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
)

// RunRestoreTxn starts a txn kv restore task inside the current goroutine.
func RunRestoreTxn(c context.Context, g glue.Glue, cmdName string, cfg *Config) (err error) {
	cfg.adjust()
	if cfg.Concurrency == 0 {
		cfg.Concurrency = defaultRestoreConcurrency
	}

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// Restore raw does not need domain.
	needDomain := false
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(cfg), cfg.CheckRequirements, needDomain, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg := GetKeepalive(cfg)
	// sometimes we have pooled the connections.
	// sending heartbeats in idle times is useful.
	keepaliveCfg.PermitWithoutStream = true
	client := snapclient.NewRestoreClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrencyPerStore(uint(cfg.Concurrency))
	err = client.Init(g, mgr.GetStorage())
	defer client.Close()
	if err != nil {
		return errors.Trace(err)
	}

	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	reader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.InitBackupMeta(c, backupMeta, u, reader, true); err != nil {
		return errors.Trace(err)
	}

	if client.IsRawKvMode() {
		return errors.Annotate(berrors.ErrRestoreModeMismatch, "cannot do transactional restore from raw data")
	}

	files := backupMeta.Files
	archiveSize := reader.ArchiveSize(ctx, files)
	g.Record(summary.RestoreDataSize, archiveSize)

	if len(files) == 0 {
		log.Info("all files are filtered out from the backup archive, nothing to restore")
		return nil
	}
	summary.CollectInt("restore files", len(files))

	ranges, _, err := restoreutils.MergeAndRewriteFileRanges(
		files, nil, conn.DefaultMergeRegionSizeBytes, conn.DefaultMergeRegionKeyCount)
	if err != nil {
		return errors.Trace(err)
	}
	// Redirect to log if there is no log file to avoid unreadable output.
	// TODO: How to show progress?
	updateCh := g.StartProgress(
		ctx,
		"Txn Restore",
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	// RawKV restore does not need to rewrite keys.
	err = client.SplitRanges(ctx, ranges, updateCh, false)
	if err != nil {
		return errors.Trace(err)
	}

	importModeSwitcher := restore.NewImportModeSwitcher(mgr.GetPDClient(), cfg.SwitchModeInterval, mgr.GetTLSConfig())
	restoreSchedulers, _, err := restore.RestorePreWork(ctx, mgr, importModeSwitcher, false, true)
	if err != nil {
		return errors.Trace(err)
	}
	defer restore.RestorePostWork(ctx, importModeSwitcher, restoreSchedulers, false)

	err = client.WaitForFilesRestored(ctx, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	// Restore has finished.
	updateCh.Close()

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}
