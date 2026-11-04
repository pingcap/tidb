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
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	logProgressTick   = 2 * time.Minute
	statusRefreshTick = 5 * time.Second
)

func (d *Dumper) startLogProgress(tctx *tcontext.Context) func() {
	ctx, cancel := tctx.WithCancel()
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		d.runLogProgress(ctx)
	})
	return func() {
		cancel()
		// Publish the final snapshot before Dump returns or releases resources.
		wg.Wait()
	}
}

func (d *Dumper) runLogProgress(tctx *tcontext.Context) {
	d.RefreshStatus()
	defer d.RefreshStatus()
	statusTicker := time.NewTicker(statusRefreshTick)
	defer statusTicker.Stop()
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
		case <-statusTicker.C:
			d.RefreshStatus()
		case <-logProgressTicker.C:
			// Integration tests shorten the log interval below the snapshot refresh interval.
			failpoint.Inject("EnableLogProgress", func() {
				d.RefreshStatus()
			})
			nanoseconds := float64(time.Since(lastCheckpoint).Nanoseconds())
			// Keep the average over the log interval independent of snapshot age.
			finishedBytes := ReadGauge(d.metrics.finishedSizeGauge)
			s := d.GetStatus()
			tctx.L().Info("progress",
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", s.CompletedTables, float64(s.TotalTables), s.CompletedTables/float64(s.TotalTables)*100)),
				zap.String("finished rows", fmt.Sprintf("%.0f", s.FinishedRows)),
				zap.String("estimate total rows", fmt.Sprintf("%.0f", s.EstimateTotalRows)),
				zap.String("finished size", units.HumanSize(s.FinishedBytes)),
				zap.Float64("average speed(MiB/s)", (finishedBytes-lastBytes)/(1048576e-9*nanoseconds)),
				zap.Float64("recent speed bps", s.CurrentSpeedBPS),
				zap.String("chunks progress", s.Progress),
			)

			lastCheckpoint = time.Now()
			lastBytes = finishedBytes
		}
	}
}

// DumpStatus is the status of dumping.
type DumpStatus struct {
	CompletedTables   float64 `json:"completedTables"`
	FinishedBytes     float64 `json:"finishedBytes"`
	FinishedRows      float64 `json:"finishedRows"`
	EstimateTotalRows float64 `json:"estimateTotalRows"`
	TotalTables       int64   `json:"totalTables"`
	CurrentSpeedBPS   float64 `json:"currentSpeedBPS"`
	// Progress is rendered for a person reading a log line. A caller that
	// needs to compute with it - to drive a progress bar, or to estimate a
	// finish time - should read ProgressPercent instead of parsing this.
	Progress string `json:"progress,omitempty"`
	// ProgressPercent is Progress as a number between 0 and 100. It is nil
	// until the chunk count is known, which is the same moment Progress stops
	// being empty: before that there is no denominator to divide by, and
	// reporting zero would claim no work had been done rather than that the
	// answer is not available yet.
	ProgressPercent *float64 `json:"progressPercent,omitempty"`
}

// GetStatus returns an independent copy of the latest status snapshot without
// updating the speed recorder. Before the first refresh it is empty.
func (d *Dumper) GetStatus() *DumpStatus {
	if status := d.status.Load(); status != nil {
		result := *status
		if status.ProgressPercent != nil {
			percent := *status.ProgressPercent
			result.ProgressPercent = &percent
		}
		return &result
	}
	return &DumpStatus{}
}

// RefreshStatus samples metrics and publishes a new status snapshot.
// The progress loop is the sole refresher, so readers cannot change the speed's sampling window.
func (d *Dumper) RefreshStatus() {
	ret := &DumpStatus{}
	defer d.status.Store(ret)
	ret.TotalTables = atomic.LoadInt64(&d.totalTables)
	ret.CompletedTables = ReadCounter(d.metrics.finishedTablesCounter)
	ret.FinishedBytes = ReadGauge(d.metrics.finishedSizeGauge)
	ret.FinishedRows = ReadGauge(d.metrics.finishedRowsGauge)
	ret.EstimateTotalRows = ReadCounter(d.metrics.estimateTotalRowsCounter)
	ret.CurrentSpeedBPS = d.speedRecorder.GetSpeed(ret.FinishedBytes)
	if d.metrics.progressReady.Load() {
		// chunks will be zero when upstream has no data
		if d.metrics.totalChunks.Load() == 0 {
			ret.setProgress(1)
			return
		}
		progress := float64(d.metrics.completedChunks.Load()) / float64(d.metrics.totalChunks.Load())
		if progress > 1 {
			progress = 1
			d.L().Warn("completedChunks is greater than totalChunks", zap.Int64("completedChunks", d.metrics.completedChunks.Load()), zap.Int64("totalChunks", d.metrics.totalChunks.Load()))
		}
		ret.setProgress(progress)
	}
}

// setProgress records one progress value in both the shapes callers need:
// the string a log line prints, and the number an API returns. They are set
// together so the two can never disagree.
func (s *DumpStatus) setProgress(fraction float64) {
	percent := fraction * 100
	s.ProgressPercent = &percent
	if fraction >= 1 {
		s.Progress = "100 %"
		return
	}
	s.Progress = fmt.Sprintf("%5.2f %%", percent)
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
