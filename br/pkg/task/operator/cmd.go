package operator

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
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

// PauseGC blocks the current goroutine and pause the GC safepoint by the config.
func PauseGC(ctx context.Context, cfg *PauseGcConfig) error {
	mgr, err := dialPD(ctx, &cfg.Config)
	if err != nil {
		return err
	}
	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      int64(cfg.TTL.Seconds()),
		BackupTS: cfg.SafePoint,
	}
	if sp.BackupTS == 0 {
		rts, err := mgr.GetMinResolvedTS(ctx)
		if err != nil {
			return err
		}
		log.Info("No service safepoint provided, using the minimal resolved TS.", zap.Uint64("min-resolved-ts", rts))
		sp.BackupTS = rts
	}
	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
