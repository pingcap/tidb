package gpool

import (
	"time"

	"github.com/pingcap/tidb/util/window"
	"go.uber.org/atomic"
)

type Statistic struct {
	passStat window.RollingCounter[float64]
	rtStat   window.RollingCounter[float64]

	prevScheduleTime atomic.Time
}

func NewStatistic() Statistic {
	const win = time.Second * 10
	const size = 100
	opts := window.RollingCounterOpts{
		Size:           size,
		BucketDuration: win / size,
	}
	return Statistic{
		passStat: window.NewRollingCounter[float64](opts),
		rtStat:   window.NewRollingCounter[float64](opts),
	}
}
