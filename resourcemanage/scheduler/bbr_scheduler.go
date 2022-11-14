package scheduler

import (
	"time"

	"github.com/pingcap/tidb/resourcemanage"
	"github.com/pingcap/tidb/util/cpu"
)

type BBRScheduler struct {
}

func (b *BBRScheduler) Tune(component resourcemanage.Component, p resourcemanage.GorotinuePool) SchedulerCommand {
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
