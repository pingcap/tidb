package scheduler

import (
	"time"

	"github.com/pingcap/tidb/resourcemanage"
	"github.com/pingcap/tidb/util/cpu"
	"github.com/pingcap/tidb/util/mathutil"
)

const (
	defaultSmoothing      float64 = 0.2
	defaultMinConcurrency int     = 2

	defaultOverloadConcurrencyDelta int = 2 // minimum concurrency = user setting concurrency - delta
)

type Gradient2Scheduler struct {
	smoothing      float64
	estimatedLimit int16
}

func NewGradient2Scheduler() *Gradient2Scheduler {
	return &Gradient2Scheduler{
		smoothing:      defaultSmoothing,
		estimatedLimit: 1,
	}
}

func (b *Gradient2Scheduler) Tune(component resourcemanage.Component, p resourcemanage.GorotinuePool) SchedulerCommand {
	if time.Since(p.LastTunerTs()) < minCPUSchedulerInterval {
		return NoIdea
	}
	usage := cpu.GetCPUUsage()
	if usage > 0.8 && component == resourcemanage.DDL {
		return Downclock
	}

	if p.InFlight() < int64(p.Cap())/2 {
		return Hold
	}

	// Rtt could be higher than rtt_noload because of smoothing rtt noload updates
	// so set to 1.0 to indicate no queuing.  Otherwise calculate the slope and don't
	// allow it to be reduced by more than half to avoid aggressive load-shedding due to
	// outliers.
	gradient := mathutil.Max(0.5, mathutil.Min(1.0, p.LongRTT()/float64(p.ShortRTT())))
	newLimit := float64(p.Running())*gradient + float64(p.GetQueueSize())
	newLimit = float64(p.Running())*(1-b.smoothing) + newLimit*b.smoothing
	newLimit = mathutil.Max(float64(defaultMinConcurrency), mathutil.Min(float64(defaultOverloadConcurrencyDelta+p.Cap()), newLimit))
	if newLimit > float64(p.Running()) {
		return Overclock
	} else if newLimit < float64(p.Running()) {
		return Downclock
	}
	return Hold
}
