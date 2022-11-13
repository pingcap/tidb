package gpool

import (
	"github.com/pingcap/tidb/util/window"
	"go.uber.org/atomic"
)

type Statistic struct {
	passStat window.RollingCounter[float64]
	rtStat   window.RollingCounter[float64]

	prevScheduleTime atomic.Time
}
