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
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/data"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func ReadBackupMetaData(ctx context.Context, s storage.ExternalStorage) (uint64, int, error) {
	metaInfo, err := config.NewMetaFromStorage(ctx, s)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if FullBackupType(metaInfo.GetFullBackupType()) != FullBackupTypeEBS {
		log.Error("invalid meta file", zap.Reflect("meta", metaInfo))
		return 0, 0, errors.New("invalid meta file, only support aws-ebs now")
	}
	return metaInfo.GetResolvedTS(), metaInfo.TiKVComponent.Replicas, nil
}

// RunResolveKvData starts a restore task inside the current goroutine.
func RunResolveKvData(c context.Context, g glue.Glue, cmdName string, cfg *RestoreConfig) error {
	cfg.Adjust()
	startAll := time.Now()
	defer summary.Summary(cmdName)

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// genenic work included opentrace, and restore client etc.
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.runResolveKvData", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// read the backup meta resolved ts and total tikvs from backup storage
	var resolveTS uint64
	_, externStorage, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	resolveTS, numStores, err := ReadBackupMetaData(ctx, externStorage)
	if err != nil {
		return errors.Trace(err)
	}
	summary.CollectUint("resolve-ts", resolveTS)

	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, false, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg.PermitWithoutStream = true
	tc := tidbconfig.GetGlobalConfig()
	tc.SkipRegisterToDashboard = true
	tc.EnableGlobalKill = false
	tidbconfig.StoreGlobalConfig(tc)

	restoreTS, err := restore.GetTSWithRetry(ctx, mgr.GetPDClient())
	if err != nil {
		return errors.Trace(err)
	}

	// stop gc before restore tikv data
	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}

	// TODO: since data restore does not have tidb up, it looks we can remove this keeper
	// it requires to do more test, then remove this part of code.
	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	// stop scheduler before recover data
	log.Info("starting to remove some PD schedulers")
	restoreFunc, e := mgr.RemoveAllPDSchedulers(ctx)
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
	err = utils.WithRetry(
		ctx,
		func() error {
			allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), util.SkipTiFlash)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		},
		utils.NewPDReqBackofferExt(),
	)
	restoreNumStores := len(allStores)
	if restoreNumStores != numStores {
		log.Warn("the number of stores in the cluster has changed", zap.Int("origin", numStores), zap.Int("current", restoreNumStores))
	}

	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("total tikv", zap.Int("total", restoreNumStores), zap.String("progress file", cfg.ProgressFile))
	// progress = read meta + send recovery + iterate tikv + (1 * prepareflashback + 1 * flashback)
	progress := g.StartProgress(ctx, cmdName, int64(restoreNumStores*3+2), !cfg.LogProgress)
	go progressFileWriterRoutine(ctx, progress, int64(restoreNumStores*3+2), cfg.ProgressFile)

	// restore tikv data from a snapshot volume
	var totalRegions int

	totalRegions, err = data.RecoverData(ctx, resolveTS, allStores, mgr, progress, restoreTS, cfg.Concurrency)
	if err != nil {
		return errors.Trace(err)
	}

	summary.CollectInt("total regions", totalRegions)
	log.Info("unmark recovering to pd")
	if err := mgr.UnmarkRecovering(ctx); err != nil {
		return errors.Trace(err)
	}

	//TODO: restore volume type into origin type
	//ModifyVolume(*ec2.ModifyVolumeInput) (*ec2.ModifyVolumeOutput, error) by backupmeta

	// since we cannot reset tiflash automaticlly. so we should start it manually
	if err = resetTiFlashReplicas(ctx, g, mgr.GetStorage(), mgr.GetPDClient()); err != nil {
		return errors.Trace(err)
	}

	progress.Close()
	summary.CollectDuration("restore duration", time.Since(startAll))
	summary.SetSuccessStatus(true)
	return nil
}

func resetTiFlashReplicas(ctx context.Context, g glue.Glue, storage kv.Storage, pdClient pd.Client) error {
	dom, err := g.GetDomain(storage)
	if err != nil {
		return errors.Trace(err)
	}
	info := dom.InfoSchema()
	allSchemaName := info.AllSchemaNames()
	recorder := tiflashrec.New()

	expectTiFlashStoreCount := uint64(0)
	needTiFlash := false
	for _, s := range allSchemaName {
		for _, t := range info.SchemaTables(s) {
			t := t.Meta()
			if t.TiFlashReplica != nil {
				expectTiFlashStoreCount = max(expectTiFlashStoreCount, t.TiFlashReplica.Count)
				recorder.AddTable(t.ID, *t.TiFlashReplica)
				needTiFlash = true
			}
		}
	}
	if !needTiFlash {
		log.Info("no need to set tiflash replica, since there is no tables enable tiflash replica")
		return nil
	}
	// we wait for ten minutes to wait tiflash starts.
	// since tiflash only starts when set unmark recovery mode finished.
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	err = utils.WithRetry(timeoutCtx, func() error {
		tiFlashStoreCount, err := getTiFlashNodeCount(ctx, pdClient)
		log.Info("get tiflash store count for resetting TiFlash Replica",
			zap.Uint64("count", tiFlashStoreCount))
		if err != nil {
			return errors.Trace(err)
		}
		if tiFlashStoreCount < expectTiFlashStoreCount {
			log.Info("still waiting for enough tiflash store start",
				zap.Uint64("expect", expectTiFlashStoreCount),
				zap.Uint64("actual", tiFlashStoreCount),
			)
			return errors.New("tiflash store count is less than expected")
		}
		return nil
	}, &waitTiFlashBackoffer{
		Attempts:    30,
		BaseBackoff: 4 * time.Second,
	})
	if err != nil {
		return err
	}

	sqls := recorder.GenerateResetAlterTableDDLs(info)
	log.Info("Generating SQLs for resetting tiflash replica",
		zap.Strings("sqls", sqls))

	return g.UseOneShotSession(storage, false, func(se glue.Session) error {
		for _, sql := range sqls {
			if errExec := se.ExecuteInternal(ctx, sql); errExec != nil {
				logutil.WarnTerm("Failed to restore tiflash replica config, you may execute the sql restore it manually.",
					logutil.ShortError(errExec),
					zap.String("sql", sql),
				)
			}
		}
		return nil
	})
}

type waitTiFlashBackoffer struct {
	Attempts    int
	BaseBackoff time.Duration
}

// NextBackoff returns a duration to wait before retrying again
func (b *waitTiFlashBackoffer) NextBackoff(error) time.Duration {
	bo := b.BaseBackoff
	b.Attempts--
	if b.Attempts == 0 {
		return 0
	}
	b.BaseBackoff *= 2
	if b.BaseBackoff > 32*time.Second {
		b.BaseBackoff = 32 * time.Second
	}
	return bo
}

// Attempt returns the remain attempt times
func (b *waitTiFlashBackoffer) Attempt() int {
	return b.Attempts
}
