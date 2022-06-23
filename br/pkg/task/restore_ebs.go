// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
)

const (
	EBSRestoreCmd = "EBS Restore"
)

//TODO P1: get resolved_ts and num of tikv from S3 backup meta
func ReadBackupMetaData() (uint64, int) {
	return 476457456, 3
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestoreEBS(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestoreEBS", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, false)
	err = configureRestoreClient(ctx, client, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	u, s, backupMeta, err := ReadBackupMeta(ctx, metautil.MetaFile, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	backupVersion := version.NormalizeBackupVersion(backupMeta.ClusterVersion)
	if cfg.CheckRequirements && backupVersion != nil {
		if versionErr := version.CheckClusterVersion(ctx, mgr.GetPDClient(), version.CheckVersionForBackup(backupVersion)); versionErr != nil {
			return errors.Trace(versionErr)
		}
	}

	reader := metautil.NewMetaReader(backupMeta, s, &cfg.CipherInfo)
	if err = client.InitBackupMeta(c, backupMeta, u, reader); err != nil {
		return errors.Trace(err)
	}

	resolveTs, numOfStores := ReadBackupMetaData()

	restoreTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}

	// restore checksum will check safe point with its start ts, see details at
	// https://github.com/pingcap/tidb/blob/180c02127105bed73712050594da6ead4d70a85f/store/tikv/kv.go#L186-L190
	// so, we should keep the safe point unchangeable. to avoid GC life time is shorter than transaction duration.
	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	//TODO: switch PD into recovery mode, which lead to the TiKV will prepare the region meta and wait BR to pull it
	restore.SwitchToRecoveryMode(mgr.GetPDClient())

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	err = restore.RecoverCluster(ctx, resolveTs, numOfStores, allStores, mgr.GetTLSConfig())
	if err != nil {
		return errors.Trace(err)
	}

	defer restore.SwitchToNormalMode(mgr.GetPDClient())

	return nil

}
