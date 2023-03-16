package restore

import (
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/pipeline"
	"github.com/pingcap/tidb/kv"
	"golang.org/x/sync/errgroup"
)

func NewValidateChecksum(rc *Client, kvc kv.Client, updateCh glue.Progress, concurrency uint) pipeline.Worker[CreatedTable, struct{}] {
	return validateChecksum{
		rc:          rc,
		kvClient:    kvc,
		updateCh:    updateCh,
		concurrency: concurrency,
	}
}

type validateChecksum struct {
	rc          *Client
	kvClient    kv.Client
	updateCh    glue.Progress
	concurrency uint
}

func (v validateChecksum) MainLoop(ctx pipeline.Context[struct{}], input <-chan CreatedTable) {
	log.Info("Start to validate checksum")
	defer ctx.Finish()

	wg := new(sync.WaitGroup)
	wg.Add(2)
	loadStatCh := make(chan *CreatedTable, 1024)
	// run the stat loader
	go func() {
		defer wg.Done()
		v.rc.updateMetaAndLoadStats(ctx, loadStatCh)
	}()
	workers := utils.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go func() {
		eg, ectx := errgroup.WithContext(ctx)
		defer func() {
			if err := eg.Wait(); err != nil {
				ctx.EmitErr(err)
			}
			close(loadStatCh)
			wg.Done()
		}()

		for {
			select {
			// if we use ectx here, maybe canceled will mask real error.
			case <-ctx.Done():
				ctx.EmitErr(ctx.Err())
				return
			case tbl, ok := <-input:
				if !ok {
					return
				}

				workers.ApplyOnErrorGroup(eg, func() error {
					start := time.Now()
					defer func() {
						elapsed := time.Since(start)
						summary.CollectSuccessUnit("table checksum", 1, elapsed)
					}()
					err := v.rc.execChecksum(ectx, tbl, v.kvClient, v.concurrency, loadStatCh)
					if err != nil {
						return errors.Trace(err)
					}
					v.updateCh.Inc()
					return nil
				})
			}
		}
	}()
	wg.Wait()
	log.Info("all checksum ended")
}
