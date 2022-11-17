package scheduler

import (
	"time"

	"github.com/pingcap/tidb/resourcemanage"
	"github.com/pingcap/tidb/util/cpu"
)

const defaultSmoothing float64 = 0.2

type Gradient2Scheduler struct {
	smoothing float64
}

func NewGradient2Scheduler() *Gradient2Scheduler {
	return &Gradient2Scheduler{
		smoothing: defaultSmoothing,
	}
}

func (b *Gradient2Scheduler) Tune(component resourcemanage.Component, p resourcemanage.GorotinuePool) SchedulerCommand {
	if time.Since(p.LastTunerTs()) < minCPUSchedulerInterval {
		return NoIdea
	}
	usage := cpu.GetCPUUsage()
	if usage > 0.8 {
		return Downclock
	}
	if usage < 0.7 {
		return Overclock
	}
	return Hold
}
