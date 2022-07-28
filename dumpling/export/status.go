// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
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
			s := d.GetStatus()
			tctx.L().Info("progress",
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", s.CompletedTables, float64(d.totalTables), s.CompletedTables/float64(d.totalTables)*100)),
				zap.String("finished rows", fmt.Sprintf("%.0f", s.FinishedRows)),
				zap.String("estimate total rows", fmt.Sprintf("%.0f", s.EstimateTotalRows)),
				zap.String("finished size", units.HumanSize(s.FinishedBytes)),
				zap.Float64("average speed(MiB/s)", (s.FinishedBytes-lastBytes)/(1048576e-9*nanoseconds)),
			)

			lastCheckpoint = time.Now()
			lastBytes = s.FinishedBytes
		}
	}
}

// DumpStatus is the status of dumping.
type DumpStatus struct {
	CompletedTables   float64
	FinishedBytes     float64
	FinishedRows      float64
	EstimateTotalRows float64
	TotalTables       int64
}

// GetStatus returns the status of dumping by reading metrics.
func (d *Dumper) GetStatus() *DumpStatus {
	ret := &DumpStatus{}
	ret.TotalTables = atomic.LoadInt64(&d.totalTables)
	ret.CompletedTables = ReadCounter(d.metrics.finishedTablesCounter)
	ret.FinishedBytes = ReadGauge(d.metrics.finishedSizeGauge)
	ret.FinishedRows = ReadGauge(d.metrics.finishedRowsGauge)
	ret.EstimateTotalRows = ReadCounter(d.metrics.estimateTotalRowsCounter)
	return ret
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
