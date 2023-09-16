// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/keepalive"
)

func dialPD(ctx context.Context, cfg *task.Config) (*pdutil.PdController, error) {
	pdAddrs := strings.Join(cfg.PD, ",")
	var tc *tls.Config
	if cfg.TLS.IsEnabled() {
		var err error
		tc, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return nil, err
		}
	}
	mgr, err := pdutil.NewPdController(ctx, pdAddrs, tc, cfg.TLS.ToPDSecurityOption())
	if err != nil {
		return nil, err
	}
	return mgr, nil
}

func cleanUpWith(f func(ctx context.Context)) {
	cleanUpWithErr(func(ctx context.Context) error { f(ctx); return nil })
}

func cleanUpWithErr(f func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return f(ctx)
}

type AdaptEnvForSnapshotBackupContext struct {
	context.Context

	pdMgr *pdutil.PdController
	kvMgr *utils.StoreManager
	cfg   PauseGcConfig

	rdGrp  sync.WaitGroup
	runGrp *errgroup.Group
}

func (ctx *AdaptEnvForSnapshotBackupContext) ReadyL(name string, notes ...zap.Field) {
	logutil.CL(ctx).Info("Stage ready.", append(notes, zap.String("component", name))...)
	ctx.rdGrp.Done()
}

func hintAllReady() {
	// Hacking: some version of operators using the follow two logs to check whether we are ready...
	log.Info("Schedulers are paused.")
	log.Info("GC is paused.")
	log.Info("All ready.")
}

// AdaptEnvForSnapshotBackup blocks the current goroutine and pause the GC safepoint and remove the scheduler by the config.
// This function will block until the context being canceled.
func AdaptEnvForSnapshotBackup(ctx context.Context, cfg *PauseGcConfig) error {
	mgr, err := dialPD(ctx, &cfg.Config)
	if err != nil {
		return errors.Annotate(err, "failed to dial PD")
	}
	var tconf *tls.Config
	if cfg.TLS.IsEnabled() {
		tconf, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return errors.Annotate(err, "invalid tls config")
		}
	}
	kvMgr := utils.NewStoreManager(mgr.GetPDClient(), keepalive.ClientParameters{
		Time:    time.Duration(cfg.Config.GRPCKeepaliveTime) * time.Second,
		Timeout: time.Duration(cfg.Config.GRPCKeepaliveTimeout) * time.Second,
	}, tconf)
	eg, ectx := errgroup.WithContext(ctx)
	cx := &AdaptEnvForSnapshotBackupContext{
		Context: ectx,
		pdMgr:   mgr,
		kvMgr:   kvMgr,
		cfg:     *cfg,
		rdGrp:   sync.WaitGroup{},
		runGrp:  eg,
	}
	cx.rdGrp.Add(3)

	eg.Go(func() error { return pauseGCKeeper(cx) })
	eg.Go(func() error { return pauseSchedulerKeeper(cx) })
	eg.Go(func() error { return goPauseImporting(cx) })
	go func() {
		cx.rdGrp.Wait()
		hintAllReady()
	}()

	return eg.Wait()
}

func goPauseImporting(ctx *AdaptEnvForSnapshotBackupContext) error {
	denyLightning := utils.NewDenyImporting("prepare_for_snapshot_backup", ctx.kvMgr)
	if _, err := denyLightning.DenyAllStores(ctx, ctx.cfg.TTL); err != nil {
		return errors.Trace(err)
	}
	ctx.ReadyL("pause_lightning")
	ctx.runGrp.Go(func() error {
		err := denyLightning.Keeper(ctx, ctx.cfg.TTL)
		if errors.Cause(err) != context.Canceled {
			log.Warn("keeper encounters error.", logutil.ShortError(err))
		}
		return cleanUpWithErr(func(ctx context.Context) error {
			res, err := denyLightning.AllowAllStores(ctx)
			if err != nil {
				return err
			}
			return denyLightning.ConsistentWithPrev(res)
		})
	})
	return nil
}

func pauseGCKeeper(ctx *AdaptEnvForSnapshotBackupContext) error {
	// Note: should we remove the service safepoint as soon as this exits?
	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      int64(ctx.cfg.TTL.Seconds()),
		BackupTS: ctx.cfg.SafePoint,
	}
	if sp.BackupTS == 0 {
		rts, err := ctx.pdMgr.GetMinResolvedTS(ctx)
		if err != nil {
			return err
		}
		log.Info("No service safepoint provided, using the minimal resolved TS.", zap.Uint64("min-resolved-ts", rts))
		sp.BackupTS = rts
	}
	err := utils.StartServiceSafePointKeeper(ctx, ctx.pdMgr.GetPDClient(), sp)
	if err != nil {
		return err
	}
	ctx.ReadyL("pause_gc", zap.Object("safepoint", sp))
	// Note: in fact we can directly return here.
	// But the name `keeper` implies once the function exits,
	// the GC should be resume, so let's block here.
	<-ctx.Done()
	return nil
}

func pauseSchedulerKeeper(ctx *AdaptEnvForSnapshotBackupContext) error {
	undo, err := ctx.pdMgr.RemoveAllPDSchedulers(ctx)
	if undo != nil {
		defer cleanUpWith(func(ctx context.Context) {
			if err := undo(ctx); err != nil {
				log.Warn("failed to restore pd scheduler.", logutil.ShortError(err))
			}
		})
	}
	if err != nil {
		return err
	}
	ctx.ReadyL("pause_scheduler")
	// Wait until the context canceled.
	// So we can properly do the clean up work.
	<-ctx.Done()
	return nil
}
