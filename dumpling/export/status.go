// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

const logProgressTick = 2 * time.Minute

func (d *Dumper) runLogProgress(tctx *tcontext.Context) {
	logProgressTicker := time.NewTicker(logProgressTick)
	failpoint.Inject("EnableLogProgress", func() {
		logProgressTicker.Stop()
		logProgressTicker = time.NewTicker(time.Duration(1) * time.Second)
		tctx.L().Debug("EnableLogProgress")
	})
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
				zap.Float64("recent speed bps", s.CurrentSpeedBPS),
				zap.String("chunks progress", s.Progress),
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
	CurrentSpeedBPS   float64
	Progress          string
}

// GetStatus returns the status of dumping by reading metrics.
func (d *Dumper) GetStatus() *DumpStatus {
	ret := &DumpStatus{}
	ret.TotalTables = atomic.LoadInt64(&d.totalTables)
	ret.CompletedTables = ReadCounter(d.metrics.finishedTablesCounter)
	ret.FinishedBytes = ReadGauge(d.metrics.finishedSizeGauge)
	ret.FinishedRows = ReadGauge(d.metrics.finishedRowsGauge)
	ret.EstimateTotalRows = ReadCounter(d.metrics.estimateTotalRowsCounter)
	ret.CurrentSpeedBPS = d.speedRecorder.GetSpeed(ret.FinishedBytes)
	if d.metrics.progressReady.Load() {
		// chunks will be zero when upstream has no data
		if d.metrics.totalChunks.Load() == 0 {
			ret.Progress = "100 %"
			return ret
		}
		progress := float64(d.metrics.completedChunks.Load()) / float64(d.metrics.totalChunks.Load())
		if progress > 1 {
			ret.Progress = "100 %"
			d.L().Warn("completedChunks is greater than totalChunks", zap.Int64("completedChunks", d.metrics.completedChunks.Load()), zap.Int64("totalChunks", d.metrics.totalChunks.Load()))
		} else {
			ret.Progress = fmt.Sprintf("%5.2f %%", progress*100)
		}
	}
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

// SpeedRecorder record the finished bytes and calculate its speed.
type SpeedRecorder struct {
	mu             sync.Mutex
	lastFinished   float64
	lastUpdateTime time.Time
	speedBPS       float64
}

// NewSpeedRecorder new a SpeedRecorder.
func NewSpeedRecorder() *SpeedRecorder {
	return &SpeedRecorder{
		lastUpdateTime: time.Now(),
	}
}

// GetSpeed calculate status speed.
func (s *SpeedRecorder) GetSpeed(finished float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if finished <= s.lastFinished {
		// for finished bytes does not get forwarded, use old speed to avoid
		// display zero. We may find better strategy in future.
		return s.speedBPS
	}

	now := time.Now()
	elapsed := now.Sub(s.lastUpdateTime).Seconds()
	if elapsed == 0 {
		// if time is short, return last speed
		return s.speedBPS
	}
	currentSpeed := (finished - s.lastFinished) / elapsed
	if currentSpeed == 0 {
		currentSpeed = 1
	}

	s.lastFinished = finished
	s.lastUpdateTime = now
	s.speedBPS = currentSpeed

	return currentSpeed
}
