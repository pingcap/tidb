package operator

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f(ctx)
}

// PauseGCAndScheduler blocks the current goroutine and pause the GC safepoint and remove the scheduler by the config.
// This function will block until the context being canceled.
func PauseGCAndScheduler(ctx context.Context, cfg *PauseGcConfig) error {
	mgr, err := dialPD(ctx, &cfg.Config)
	if err != nil {
		return err
	}

	eg, ectx := errgroup.WithContext(ctx)

	eg.Go(func() error { return pauseGCKeeper(ectx, cfg, mgr) })
	eg.Go(func() error { return pauseSchedulerKeeper(ectx, mgr) })

	return eg.Wait()
}

func pauseGCKeeper(ctx context.Context, cfg *PauseGcConfig, ctl *pdutil.PdController) error {
	// Note: should we remove the service safepoint as soon as this exits?
	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      int64(cfg.TTL.Seconds()),
		BackupTS: cfg.SafePoint,
	}
	if sp.BackupTS == 0 {
		rts, err := ctl.GetMinResolvedTS(ctx)
		if err != nil {
			return err
		}
		log.Info("No service safepoint provided, using the minimal resolved TS.", zap.Uint64("min-resolved-ts", rts))
		sp.BackupTS = rts
	}
	err := utils.StartServiceSafePointKeeper(ctx, ctl.GetPDClient(), sp)
	if err != nil {
		return err
	}
	log.Info("GC is paused.", zap.String("ID", sp.ID), zap.Uint64("at", sp.BackupTS))
	// Note: in fact we can directly return here.
	// But the name `keeper` implies once the function exits,
	// the GC should be resume, so let's block here.
	<-ctx.Done()
	return nil
}

func pauseSchedulerKeeper(ctx context.Context, ctl *pdutil.PdController) error {
	undo, err := ctl.RemoveAllPDSchedulers(ctx)
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
	log.Info("Schedulers are paused.")
	// Wait until the context canceled.
	// So we can properly do the clean up work.
	<-ctx.Done()
	return nil
}
