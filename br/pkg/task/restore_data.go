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
	connutil "github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	RestoreDataCmd = "Restore Data"
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
	return metaInfo.GetResolvedTS(), metaInfo.TiKVComponent.Replicas, nil
}

func removeSchedulersForDataPhase(ctx context.Context, p *pdutil.PdController) (undo pdutil.UndoFunc, err error) {
	undo = pdutil.Nop

	// during phase-2, pd is fresh and in recovering-mode(recovering-mark=true), there's no leader
	// so there's no leader or region schedule initially. when phase-2 start force setting leaders, schedule may begin.
	// we don't want pd do any leader or region schedule during this time, so we set those params to 0
	// before we force setting leaders
	scheduleLimitParams := []string{
		"hot-region-schedule-limit",
		"leader-schedule-limit",
		"merge-schedule-limit",
		"region-schedule-limit",
		"replica-schedule-limit",
	}
	pdConfigGenerators := pdutil.DefaultExpectPDCfgGenerators()
	for _, param := range scheduleLimitParams {
		pdConfigGenerators[param] = func(int, interface{}) interface{} { return 0 }
	}

	oldPDConfig, _, err1 := p.RemoveSchedulersWithConfigGenerator(ctx, pdConfigGenerators)
	if err1 != nil {
		err = err1
		return
	}

	undo = p.GenRestoreSchedulerFunc(oldPDConfig, pdConfigGenerators)
	return undo, errors.Trace(err)
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
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, false, conn.NormalVersionChecker)
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

	// stop scheduler before recover data
	log.Info("starting to remove some PD schedulers")
	restoreFunc, e := removeSchedulersForDataPhase(ctx, mgr.PdController)
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

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), connutil.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	if len(allStores) != totalTiKVs {
		log.Error("the restore meta contains the number of tikvs inconsist with the resore cluster", zap.Int("current stores", len(allStores)), zap.Int("backup stores", totalTiKVs))
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
	summary.SetSuccessStatus(true)
	return nil

}
