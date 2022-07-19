// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	EBSRestoreCmd = "EBS Restore"
)

const (
	flagDumpRegionInfo = "dump-region-info"
)

// DefineRestoreDataFlags defines common flags for the restore command.
func DefineRestoreDataFlags(command *cobra.Command) {
	command.Flags().Bool(flagDryRun, false, "don't access to aws environment if set to true")
	command.Flags().Bool(flagDumpRegionInfo, false, "dump all regions info")
}

type RestoreDataConfig struct {
	Config
	DumpRegionInfo bool `json:"region-info"`
	DryRun         bool `json:"dry-run"`
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreDataConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.DryRun, err = flags.GetBool(flagDryRun)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.DumpRegionInfo, err = flags.GetBool(flagDumpRegionInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return cfg.Config.ParseFromFlags(flags)
}

func ReadBackupMetaData(ctx context.Context, s storage.ExternalStorage) (uint64, int, error) {
	metaInfo, err := config.NewMetaFromStorage(ctx, s)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	return metaInfo.GetResolvedTS(), int(metaInfo.GetStoreCount()), nil
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestoreData(c context.Context, g glue.Glue, cmdName string, cfg *RestoreDataConfig) error {
	startAll := time.Now()
	defer summary.Summary(cmdName)

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// genenic work included opentrace, and restore client etc.
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestoreData", opentracing.ChildOf(span.Context()))
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

	// read the backup meta resolved ts and total tikvs from backup storage
	var resolveTs uint64
	_, externStorage, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	resolveTs, totalTiKVs, err := ReadBackupMetaData(ctx, externStorage)
	if err != nil {
		return errors.Trace(err)
	}
	summary.CollectUint("resolve-ts", resolveTs)

	restoreTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// stop gc before restore tikv data
	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}

	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	if len(allStores) != totalTiKVs {
		log.Error("the restore meta contains the number of tikvs inconsist with the resore cluster")
		return errors.Annotatef(berrors.ErrRestoreTotalKVMismatch,
			"number of tikvs mismatch")
	}

	// restore tikv data from a snapshot volume
	var totalRegions int
	if cfg.DryRun {
		totalRegions = 1024
	} else {
		totalRegions, err = restore.RecoverData(ctx, resolveTs, allStores, mgr.GetTLSConfig(), cfg.DumpRegionInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	summary.CollectInt("total regions", totalRegions)
	log.Info("unmark recovering to pd")
	if err := mgr.UnmarkRecovering(ctx); err != nil {
		return errors.Trace(err)
	}
	summary.CollectDuration("restore duration", time.Since(startAll))
	return nil

}
