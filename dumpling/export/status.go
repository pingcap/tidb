// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

const logProgressTick = 2 * time.Minute

func (d *Dumper) runLogProgress(tctx *tcontext.Context) {
	logProgressTicker := time.NewTicker(logProgressTick)
	lastCheckpoint := time.Now()
	lastBytes := float64(0)
	defer logProgressTicker.Stop()
	for {
		select {
		case <-tctx.Done():
			tctx.L().Debug("stopping log progress")
			return
		case <-logProgressTicker.C:
			nanoseconds := float64(time.Since(lastCheckpoint).Nanoseconds())
			midd := d.GetParameters()
			tctx.L().Info("progress",
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", midd.CompletedTables, float64(d.totalTables), midd.CompletedTables/float64(d.totalTables)*100)),
				zap.String("finished rows", fmt.Sprintf("%.0f", midd.FinishedRows)),
				zap.String("estimate total rows", fmt.Sprintf("%.0f", midd.EstimateTotalRows)),
				zap.String("finished size", units.HumanSize(midd.FinishedBytes)),
				zap.Float64("average speed(MiB/s)", (midd.FinishedBytes-lastBytes)/(1048576e-9*nanoseconds)),
			)

			lastCheckpoint = time.Now()
			lastBytes = midd.FinishedBytes
		}
	}
}

type Midparams struct {
	CompletedTables   float64
	FinishedBytes     float64
	FinishedRows      float64
	EstimateTotalRows float64
	TotalTables       int64
}

func (d *Dumper) GetParameters() (midparams *Midparams) {
	conf := d.conf
	mid := &Midparams{}
	mid.TotalTables = atomic.LoadInt64(&d.totalTables)
	mid.CompletedTables = ReadCounter(finishedTablesCounter, conf.Labels)
	mid.FinishedBytes = ReadGauge(finishedSizeGauge, conf.Labels)
	mid.FinishedRows = ReadGauge(finishedRowsGauge, conf.Labels)
	mid.EstimateTotalRows = ReadCounter(estimateTotalRowsCounter, conf.Labels)
	return mid
}

func calculateTableCount(m DatabaseTables) int {
	cnt := 0
	for _, tables := range m {
		for _, table := range tables {
			if table.Type == TableTypeBase {
				cnt++
			}
		}
	}
	return cnt
}
