package operator

import (
	"context"
	"slices"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func getAllTiKVs(ctx context.Context, p pd.Client) ([]*metapb.Store, error) {
	stores, err := p.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	withoutTiFlash := slices.DeleteFunc(stores, engine.IsTiFlash)
	return withoutTiFlash, err
}

func RunForceFlush(ctx context.Context, cfg *ForceFlushConfig) error {
	pdMgr, err := dialPD(ctx, &cfg.Config)
	if err != nil {
		return err
	}
	defer pdMgr.Close()

	stores, err := createStoreManager(pdMgr.GetPDClient(), &cfg.Config)
	if err != nil {
		return err
	}
	defer stores.Close()

	tikvs, err := getAllTiKVs(ctx, pdMgr.GetPDClient())
	if err != nil {
		return err
	}
	eg, ectx := errgroup.WithContext(ctx)
	log.Info("About to start force flushing.", zap.Stringer("stores-pattern", cfg.StoresPattern))
	for _, s := range tikvs {
		if !cfg.StoresPattern.MatchString(s.Address) || engine.IsTiFlash(s) {
			log.Info("Skipping TiFlash or not matched TiKV.",
				zap.Uint64("store", s.GetId()), zap.String("addr", s.Address), zap.Bool("tiflash?", engine.IsTiFlash(s)))
			continue
		}

		log.Info("Starting force flush TiKV.", zap.Uint64("store", s.GetId()), zap.String("addr", s.Address))
		eg.Go(func() error {
			var logBackupCli logbackup.LogBackupClient
			err := stores.WithConn(ectx, s.GetId(), func(cc *grpc.ClientConn) {
				logBackupCli = logbackup.NewLogBackupClient(cc)
			})
			if err != nil {
				return err
			}

			resp, err := logBackupCli.FlushNow(ectx, &logbackup.FlushNowRequest{})
			if err != nil {
				return errors.Annotatef(err, "failed to flush store %d", s.GetId())
			}
			for _, res := range resp.Results {
				if !res.Success {
					return errors.Errorf("failed to flush task %s at store %d: %s", res.TaskName, s.GetId(), res.ErrorMessage)
				}
				log.Info("Force flushed task of TiKV store.", zap.Uint64("store", s.Id), zap.String("task", res.TaskName))
			}
			return nil
		})
	}
	return eg.Wait()
}
